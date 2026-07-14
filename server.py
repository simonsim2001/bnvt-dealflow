#!/usr/bin/env python3
import http.server
import socketserver
import urllib.request
import urllib.error
import json
import os
import sys
import time
import uuid
import re
import traceback
from urllib.parse import urlparse, parse_qs, unquote
import threading

db_lock = threading.Lock()

def get_adapter_type():
    try:
        with open('manifest.json', 'r', encoding='utf-8') as f:
            manifest = json.load(f)
            return manifest.get('adapterType', 'dealflow-pipeline')
    except Exception:
        return 'dealflow-pipeline'

ADAPTER_TYPE = get_adapter_type()


PORT = int(os.environ.get("PORT", 8000))

# ---- PostgreSQL Storage Fallback Helpers ------------------------------------------

def get_db_connection():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        return None
    try:
        import psycopg2
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"[PostgreSQL Connection Error] {e}")
        return None

def init_postgres():
    conn = get_db_connection()
    if not conn:
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS key_value_store (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)
        print("[PostgreSQL] Table initialized successfully.")
    except Exception as e:
        print(f"[PostgreSQL Init Error] {e}")
    finally:
        conn.close()

def get_sql_value(key):
    conn = get_db_connection()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM key_value_store WHERE key = %s", (key,))
            row = cur.fetchone()
            if row:
                return row[0]
    except Exception as e:
        print(f"[PostgreSQL Get Error for {key}] {e}")
    finally:
        conn.close()
    return None

def set_sql_value(key, value):
    conn = get_db_connection()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO key_value_store (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
            """, (key, value))
        return True
    except Exception as e:
        print(f"[PostgreSQL Set Error for {key}] {e}")
        return False
    finally:
        conn.close()

def get_stored_value(key):
    if os.environ.get("DATABASE_URL"):
        sql_val = get_sql_value(key)
        if sql_val is not None:
            return sql_val
            
    path = os.path.join('db_storage', f"{key}.json")
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('value')
        except Exception:
            pass
    if key == 'anthropic_api_key':
        return os.environ.get("ANTHROPIC_API_KEY")
    elif key == 'granola_api_key':
        return os.environ.get("GRANOLA_API_KEY")
    return None

def get_llm_config():
    config = {"provider": "groq", "model": "llama-3.3-70b-versatile", "search_model": "llama-3.3-70b-versatile"}
    
    # Check if config file exists
    config_path = os.path.join('db_storage', 'llm_config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                config.update(data)
                return config
        except Exception:
            pass
            
    # Auto-detect provider based on available keys/env vars if config doesn't exist
    if get_stored_value("groq_api_key") or os.environ.get("GROQ_API_KEY"):
        config["provider"] = "groq"
        config["model"] = "llama-3.3-70b-versatile"
        config["search_model"] = "llama-3.3-70b-versatile"
    elif get_stored_value("openrouter_api_key") or os.environ.get("OPENROUTER_API_KEY"):
        config["provider"] = "openrouter"
        config["model"] = "google/gemini-2.5-flash"
        config["search_model"] = "google/gemini-2.5-flash"
    elif get_stored_value("deepseek_api_key") or os.environ.get("DEEPSEEK_API_KEY"):
        config["provider"] = "deepseek"
        config["model"] = "deepseek-chat"
        config["search_model"] = "deepseek-chat"
    elif get_stored_value("openai_api_key") or os.environ.get("OPENAI_API_KEY"):
        config["provider"] = "openai"
        config["model"] = "gpt-4o-mini"
        config["search_model"] = "gpt-4o-mini"
    elif get_stored_value("anthropic_api_key") or os.environ.get("ANTHROPIC_API_KEY"):
        config["provider"] = "anthropic"
        config["model"] = "claude-sonnet-4-6"
        config["search_model"] = "claude-sonnet-4-6"
        
    return config

def call_claude_api(api_key, prompt, use_search=False):
    config = get_llm_config()
    provider = config.get("provider", "anthropic")
    
    if provider == "anthropic":
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        }
        
        messages = [
            {"role": "user", "content": prompt}
        ]
        
        payload = {
            "model": config.get("model", "claude-sonnet-4-6"),
            "max_tokens": 4000,
            "messages": messages
        }
        if use_search:
            headers["anthropic-beta"] = "web-search-2025-03-05"
            payload["tools"] = [{"type": "web_search_20250305", "name": "web_search"}]

        max_loops = 5
        for loop_idx in range(max_loops):
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            try:
                with urllib.request.urlopen(req) as response:
                    res_data = response.read()
                    res_json = json.loads(res_data.decode('utf-8'))
                    
                    stop_reason = res_json.get('stop_reason')
                    content = res_json.get('content', [])
                    
                    print(f"[DEBUG CLAUDE] Loop {loop_idx}: stop_reason={stop_reason}")
                    print(f"[DEBUG CLAUDE] Loop {loop_idx} content={content}")
                    
                    has_tool_use = any(block.get('type') in ('server_tool_use', 'tool_use') for block in content)
                    
                    if stop_reason == 'end_turn' and not has_tool_use:
                        text_blocks = [b.get('text', '') for b in content if b.get('type') == 'text']
                        if text_blocks:
                            text_val = "".join(text_blocks).strip()
                            print(f"[DEBUG CLAUDE] Loop {loop_idx} returning combined: {text_val}")
                            return text_val
                        raise Exception(f"Claude resolved request, but no text output was returned: {res_json}")
                    
                    elif stop_reason == 'pause_turn' or (stop_reason == 'end_turn' and has_tool_use):
                        # Append assistant message content and loop again
                        print(f"[DEBUG CLAUDE] Loop {loop_idx} pausing/tool-using and appending to messages.")
                        assistant_content = []
                        user_content = []
                        for block in content:
                            b_type = block.get('type')
                            if b_type == 'text':
                                assistant_content.append(block)
                            elif b_type == 'server_tool_use':
                                assistant_content.append({
                                    "type": "tool_use",
                                    "id": block["id"],
                                    "name": block.get("name", "web_search"),
                                    "input": block["input"]
                                })
                            elif b_type == 'tool_use':
                                assistant_content.append(block)
                            elif b_type == 'web_search_tool_result':
                                user_content.append({
                                    "type": "tool_result",
                                    "tool_use_id": block["tool_use_id"],
                                    "content": json.dumps(block.get("content", ""))
                                })
                            elif b_type == 'tool_result':
                                user_content.append(block)
                        
                        if assistant_content:
                            messages.append({
                                "role": "assistant",
                                "content": assistant_content
                            })
                        if user_content:
                            messages.append({
                                "role": "user",
                                "content": user_content
                            })
                        else:
                            messages.append({
                                "role": "assistant",
                                "content": content
                            })
                        payload["messages"] = messages
                        continue
                    
                    else:
                        text_blocks = [b.get('text', '') for b in content if b.get('type') == 'text']
                        if text_blocks:
                            text_val = "\n".join(text_blocks).strip()
                            print(f"[DEBUG CLAUDE] Loop {loop_idx} returning else fallback: {text_val}")
                            return text_val
                        raise Exception(f"Unexpected response format from Claude (stop_reason: {stop_reason}): {res_json}")
                        
            except urllib.error.HTTPError as e:
                err_body = e.read().decode('utf-8', errors='ignore')
                print(f"Error calling Anthropic API: {e} - Details: {err_body}")
                try:
                    err_json = json.loads(err_body)
                    err_msg = err_json.get('error', {}).get('message')
                    if err_msg:
                        raise Exception(f"Claude API error: {err_msg}")
                except Exception as json_err:
                    if "Claude API error" in str(json_err):
                        raise json_err
                raise Exception(f"Claude API returned HTTP {e.code}: {err_body}")
            except Exception as e:
                print(f"Error calling Anthropic API: {e}")
                raise e

        raise Exception("Anthropic API failed to resolve after maximum search loops.")

    elif provider in ("openrouter", "deepseek", "groq", "openai"):
        prov_key = get_stored_value(f"{provider}_api_key") or os.environ.get(f"{provider.upper()}_API_KEY")
        if not prov_key:
            prov_key = api_key
            
        model = config.get("search_model") if use_search else config.get("model")
        
        user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        if provider == "openrouter":
            url = "https://openrouter.ai/api/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {prov_key}",
                "HTTP-Referer": "https://bnvt-dealflow.onrender.com",
                "X-Title": "BNVT Dealflow",
                "User-Agent": user_agent
            }
        elif provider == "deepseek":
            url = "https://api.deepseek.com/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {prov_key}",
                "User-Agent": user_agent
            }
        elif provider == "groq":
            url = "https://api.groq.com/openapi/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {prov_key}",
                "User-Agent": user_agent
            }
        elif provider == "openai":
            url = "https://api.openai.com/v1/chat/completions"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {prov_key}",
                "User-Agent": user_agent
            }
            
        payload = {
            "model": model,
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "max_tokens": 4000
        }
        
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        try:
            with urllib.request.urlopen(req) as response:
                res_data = response.read()
                res_json = json.loads(res_data.decode('utf-8'))
                choices = res_json.get('choices', [])
                if choices:
                    content = choices[0].get('message', {}).get('content', '').strip()
                    return content
                raise Exception(f"Unexpected response format from {provider}: {res_json}")
        except urllib.error.HTTPError as e:
            err_body = e.read().decode('utf-8', errors='ignore')
            print(f"Error calling {provider} API: {e} - Details: {err_body}")
            raise Exception(f"{provider} API error: {err_body}")
        except Exception as e:
            print(f"Error calling {provider} API: {e}")
            raise e
    else:
        raise Exception(f"Unsupported LLM provider: {provider}")




def extract_json(text):
    text = text.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    start = text.find('{')
    end = text.rfind('}')
    if start != -1 and end != -1:
        text = text[start:end+1]
    return json.loads(text)

def process_whatsapp_message(message_text, sender_phone, sender_name):
    api_key = get_stored_value('anthropic_api_key')
    if not api_key:
        return False, "Anthropic API key not configured on server"
        
    prompt = f"""You are an AI data ingestion parser for the BNVT Dealflow Atelier platform.
An investor/analyst has sent a WhatsApp message containing information about either:
1. A new company (startup / investment opportunity)
2. A new person / co-investor to add to our network

Here is the message content:
---
{message_text}
---

Your goal is to classify this message and extract structured details.

Classifications:
- "company": If the message describes a startup, website, company name, or investment opportunity.
- "investor": If the message describes a person, investor, co-investor, fund manager, partner, or contact we want to add to our co-investor network.
- "unknown": If the message is completely irrelevant or contains no useful name/URL.

For "company", extract:
- name (mandatory, fallback to derived name from website/url if name is not explicitly mentioned)
- url (a website or LinkedIn company page, e.g. "https://linear.app")
- oneLiner (a brief description of what they do, max 12 words)
- sector (industry, e.g. "SaaS", "Enterprise AI", "B2B SaaS")
- stage (one of: "Pre-seed", "Seed", "Series A", "Series B", "Growth". Fallback to "Seed" if not specified)
- geo (Country name, e.g. "United States", "United Kingdom", "Germany")
- priority (one of: "High", "Medium", "Low". Fallback to "Medium" if not specified)
- notes (additional context or description parsed from the message)

For "investor", extract:
- name (mandatory, person's name)
- fund (venture capital firm / fund name, e.g. "Lowercarbon Capital")
- email (email address if mentioned, else "")
- linkedin (personal LinkedIn URL if mentioned, else "")
- stage (preferred stage focus, one of: "Pre-seed", "Seed", "Series A", "Series B", "Growth". Fallback to "Seed" if not specified)
- priority (one of: "High", "Medium", "Low". Fallback to "Medium" if not specified)
- focus (list of interest sectors / focus areas, e.g. ["AI", "climate", "SaaS"])
- sweetSpot (typical check size or stage, e.g. "Seed, $1-3M")
- geo (Country, e.g. "United States", "United Kingdom")
- city (City name, e.g. "London", "San Francisco")
- notes (additional context or meeting notes from the message)

Return your response as a raw JSON object ONLY, with the following structure:
{{
  "type": "company" | "investor" | "unknown",
  "data": {{ ... }}
}}
Do not include any markdown styling, conversational filler, or code blocks in your response. Just return raw JSON."""

    response_text = call_claude_api(api_key, prompt)
    if not response_text:
        return False, "Failed to get response from Claude"
        
    try:
        parsed = extract_json(response_text)
    except Exception as e:
        return False, f"Failed to parse JSON response: {str(e)}\nRaw response: {response_text}"
        
    with db_lock:
        # Read database
        db = read_db()
                
        if entity_type == 'company':
            company_data = parsed.get('data', {})
            name = company_data.get('name')
            if not name:
                return False, "Parsed company name is missing"
                
            url = company_data.get('url', '')
            def clean_url(u):
                if not u: return ""
                u = u.lower().strip()
                if u.startswith("https://"): u = u[8:]
                if u.startswith("http://"): u = u[7:]
                if u.startswith("www."): u = u[4:]
                if u.endswith("/"): u = u[:-1]
                return u
                
            clean_new_url = clean_url(url)
            clean_new_name = name.lower().strip()
            
            existing_comp = None
            for c in db.get('companies', []):
                if clean_new_url and clean_url(c.get('url')) == clean_new_url:
                    existing_comp = c
                    break
                if c.get('name', '').lower().strip() == clean_new_name:
                    existing_comp = c
                    break
                    
            if existing_comp:
                notes = existing_comp.setdefault('notes', [])
                notes.append({
                    "ts": int(time.time() * 1000),
                    "text": f"[WhatsApp Sync] {company_data.get('notes', 'Updated via WhatsApp')}",
                    "via": "whatsapp"
                })
                response_msg = f"Updated existing company: {existing_comp.get('name')}"
            else:
                new_id = f"wa_{uuid.uuid4().hex[:8]}"
                new_comp = {
                    "id": new_id,
                    "name": name,
                    "url": url,
                    "oneLiner": company_data.get('oneLiner', ''),
                    "sector": company_data.get('sector', ''),
                    "stage": company_data.get('stage', 'Seed'),
                    "geo": company_data.get('geo', ''),
                    "priority": company_data.get('priority', 'Medium'),
                    "source": "WhatsApp",
                    "status": "Active opportunity",
                    "dtm": False,
                    "overview": {},
                    "scores": None,
                    "rationale": {},
                    "notes": [
                        {
                            "ts": int(time.time() * 1000),
                            "text": company_data.get('notes', ''),
                            "via": "whatsapp"
                        }
                    ] if company_data.get('notes') else [],
                    "createdAt": int(time.time() * 1000),
                    "founders": [],
                    "assets": [],
                    "granolaNotes": []
                }
                db.setdefault('companies', []).insert(0, new_comp)
                response_msg = f"Added new company: {name}"
                
        else: # investor
            investor_data = parsed.get('data', {})
            name = investor_data.get('name')
            if not name:
                return False, "Parsed investor name is missing"
                
            linkedin = investor_data.get('linkedin', '')
            email = investor_data.get('email', '')
            fund = investor_data.get('fund', '')
            
            existing_inv = None
            for i in db.get('investors', []):
                if linkedin and i.get('linkedin') == linkedin:
                    existing_inv = i
                    break
                if email and i.get('email') == email:
                    existing_inv = i
                    break
                if i.get('name', '').lower().strip() == name.lower().strip() and i.get('fund', '').lower().strip() == fund.lower().strip():
                    existing_inv = i
                    break
                    
            if existing_inv:
                new_notes = investor_data.get('notes', '')
                if new_notes:
                    old_notes = existing_inv.get('notes', '')
                    existing_inv['notes'] = f"{old_notes}\n[WhatsApp Sync] {new_notes}".strip()
                response_msg = f"Updated existing investor: {existing_inv.get('name')} ({existing_inv.get('fund')})"
            else:
                new_id = f"wa_{uuid.uuid4().hex[:8]}"
                new_inv = {
                    "id": new_id,
                    "name": name,
                    "fund": fund,
                    "email": email,
                    "linkedin": linkedin,
                    "stage": investor_data.get('stage', 'Seed'),
                    "priority": investor_data.get('priority', 'Medium'),
                    "focus": investor_data.get('focus', []),
                    "sweetSpot": investor_data.get('sweetSpot', ''),
                    "geo": investor_data.get('geo', ''),
                    "city": investor_data.get('city', ''),
                    "notes": investor_data.get('notes', '')
                }
                db.setdefault('investors', []).insert(0, new_inv)
                response_msg = f"Added new investor: {name} ({fund})"
                
        # Save back database
        write_db(db)
            
        return True, response_msg


def extract_docx_text(path):
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(path) as z:
            xml_content = z.read('word/document.xml')
            root = ET.fromstring(xml_content)
            texts = []
            for elem in root.iter():
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                if tag == 't' and elem.text:
                    texts.append(elem.text)
            return "\n".join(texts)
    except Exception as e:
        return f"[Error parsing DOCX: {str(e)}]"

def extract_pptx_text(path):
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(path) as z:
            texts = []
            slide_files = sorted([name for name in z.namelist() if name.startswith('ppt/slides/slide') and name.endswith('.xml')],
                                 key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            for slide_name in slide_files:
                texts.append(f"\n--- {os.path.basename(slide_name).replace('.xml', '').upper()} ---")
                xml_content = z.read(slide_name)
                root = ET.fromstring(xml_content)
                slide_texts = []
                for elem in root.iter():
                    tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                    if tag == 't' and elem.text:
                        slide_texts.append(elem.text)
                texts.append(" ".join(slide_texts))
            return "\n".join(texts)
    except Exception as e:
        return f"[Error parsing PPTX: {str(e)}]"

def extract_xlsx_text(path):
    import zipfile
    import xml.etree.ElementTree as ET
    try:
        with zipfile.ZipFile(path) as z:
            shared_strings = []
            if 'xl/sharedStrings.xml' in z.namelist():
                xml_content = z.read('xl/sharedStrings.xml')
                root = ET.fromstring(xml_content)
                for si_elem in root.iter():
                    si_tag = si_elem.tag.split('}')[-1] if '}' in si_elem.tag else si_elem.tag
                    if si_tag == 'si':
                        si_text_parts = []
                        for t_elem in si_elem.iter():
                            t_tag = t_elem.tag.split('}')[-1] if '}' in t_elem.tag else t_elem.tag
                            if t_tag == 't' and t_elem.text:
                                si_text_parts.append(t_elem.text)
                        shared_strings.append("".join(si_text_parts))
            
            sheet_files = sorted([name for name in z.namelist() if name.startswith('xl/worksheets/sheet') and name.endswith('.xml')],
                                 key=lambda x: int(''.join(filter(str.isdigit, x)) or 0))
            
            texts = []
            for sheet_name in sheet_files:
                texts.append(f"\n--- {os.path.basename(sheet_name).replace('.xml', '').upper()} ---")
                xml_content = z.read(sheet_name)
                root = ET.fromstring(xml_content)
                
                row_texts = []
                for row_elem in root.iter():
                    row_tag = row_elem.tag.split('}')[-1] if '}' in row_elem.tag else row_elem.tag
                    if row_tag == 'row':
                        cell_texts = []
                        for cell_elem in row_elem:
                            cell_tag = cell_elem.tag.split('}')[-1] if '}' in cell_elem.tag else cell_elem.tag
                            if cell_tag == 'c':
                                t_attr = cell_elem.attrib.get('t', '')
                                val = None
                                for sub in cell_elem:
                                    sub_tag = sub.tag.split('}')[-1] if '}' in sub.tag else sub.tag
                                    if sub_tag == 'v':
                                        val = sub.text
                                        break
                                
                                if val:
                                    if t_attr == 's':
                                        try:
                                            idx = int(val)
                                            if idx < len(shared_strings):
                                                cell_texts.append(shared_strings[idx])
                                            else:
                                                cell_texts.append(val)
                                        except ValueError:
                                            cell_texts.append(val)
                                    else:
                                        cell_texts.append(val)
                        if cell_texts:
                            row_texts.append(" | ".join(cell_texts))
                texts.append("\n".join(row_texts))
            return "\n".join(texts)
    except Exception as e:
        return f"[Error parsing XLSX: {str(e)}]"


# ---- Azava & Inbound Intake Helpers ----------------------------------------------

def get_azava_secret():
    env_secret = os.environ.get("AZAVA_SECRET") or os.environ.get("AZAVA_SECRET_TOKEN")
    if env_secret:
        return env_secret.strip()

    secret_path = os.path.join('db_storage', 'azava_secret.json')
    if os.path.exists(secret_path):
        try:
            with open(secret_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('secret', 'azava_super_secret_token_2026').strip()
        except Exception:
            pass
    # Create default secret if not exists
    os.makedirs('db_storage', exist_ok=True)
    default_secret = 'azava_super_secret_token_2026'
    try:
        with open(secret_path, 'w', encoding='utf-8') as f:
            json.dump({'secret': default_secret}, f, indent=2)
    except Exception:
        pass
    return default_secret

def check_auth(handler):
    # Check Authorization header first
    auth_header = handler.headers.get('Authorization')
    expected_secret = get_azava_secret()
    if auth_header and auth_header.startswith('Bearer '):
        token = auth_header.split('Bearer ')[1].strip()
        if token == expected_secret:
            return True
            
    # Check query params (especially for GET requests)
    parsed_url = urlparse(handler.path)
    query = parse_qs(parsed_url.query)
    token_param = query.get('secret', [None])[0] or query.get('token', [None])[0]
    if token_param and token_param.strip() == expected_secret:
        return True
        
    return False

def merge_list(client_list, server_list, key_field):
    # Maps by key_field (id)
    client_map = {item.get(key_field): item for item in client_list if item.get(key_field)}
    server_map = {item.get(key_field): item for item in server_list if item.get(key_field)}
    
    merged_list = []
    
    # Iterate client items to preserve UI-defined order/edits
    for item in client_list:
        item_id = item.get(key_field)
        if not item_id:
            merged_list.append(item)
            continue
            
        if item_id in server_map:
            server_item = server_map[item_id]
            merged_item = dict(server_item) # Start with server properties
            merged_item.update(item)        # Overwrite with client modifications
            
            # Combine notes list (union + deduplicate + sort by timestamp)
            if "notes" in item or "notes" in server_item:
                client_notes = item.get("notes", [])
                server_notes = server_item.get("notes", [])
                if isinstance(client_notes, list) and isinstance(server_notes, list):
                    notes_map = {}
                    for note in server_notes:
                        note_id = note.get("id") or note.get("ts") or note.get("date")
                        if note_id:
                            notes_map[note_id] = note
                    for note in client_notes:
                        note_id = note.get("id") or note.get("ts") or note.get("date")
                        if note_id:
                            notes_map[note_id] = note
                    
                    combined_notes = list(notes_map.values())
                    def get_note_time(n):
                        return n.get("date") or n.get("ts") or n.get("createdAt") or 0
                    try:
                        combined_notes.sort(key=get_note_time, reverse=True)
                    except Exception:
                        pass
                    merged_item["notes"] = combined_notes
            
            merged_list.append(merged_item)
        else:
            merged_list.append(item)
            
    # Add server-only items (e.g. newly ingested via WhatsApp while client tab was open)
    for item_id, item in server_map.items():
        if item_id not in client_map:
            # Prepend so new incoming deals are visible at top immediately
            merged_list.insert(0, item)
            
    return merged_list

def merge_databases(client_db, server_db):
    return {
        "companies": merge_list(client_db.get("companies", []), server_db.get("companies", []), "id"),
        "investors": merge_list(client_db.get("investors", []), server_db.get("investors", []), "id"),
        "deepDives": merge_list(client_db.get("deepDives", []), server_db.get("deepDives", []), "id"),
        "founderProfiles": merge_list(client_db.get("founderProfiles", []), server_db.get("founderProfiles", []), "id")
    }

def read_db():
    db = {"companies": [], "investors": [], "deepDives": [], "founderProfiles": []}
    if os.environ.get("DATABASE_URL"):
        sql_val = get_sql_value("bnvt-dealflow-v1")
        if sql_val:
            try:
                db = json.loads(sql_val)
                return db
            except Exception as e:
                print(f"[Error parsing SQL db JSON] {e}")
                
    db_path = os.path.join('db_storage', 'bnvt-dealflow-v1.json')
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                outer_data = json.load(f)
                value_str = outer_data.get('value', '')
                if value_str:
                    db = json.loads(value_str)
        except Exception as e:
            print(f"[Error reading db] {e}")
    return db

def write_db(db):
    db_str = json.dumps(db, ensure_ascii=False)
    if os.environ.get("DATABASE_URL"):
        if set_sql_value("bnvt-dealflow-v1", db_str):
            return True
            
    db_path = os.path.join('db_storage', 'bnvt-dealflow-v1.json')
    try:
        outer_data = {
            "key": "bnvt-dealflow-v1",
            "value": db_str
        }
        os.makedirs('db_storage', exist_ok=True)
        with open(db_path, 'w', encoding='utf-8') as f:
            json.dump(outer_data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        print(f"[Error writing db] {e}")
        return False

def get_openai_key():
    path = os.path.join('db_storage', 'openai_api_key.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return (data.get('value') or data.get('key') or "").strip()
        except Exception:
            pass
    return os.environ.get("OPENAI_API_KEY")

def transcribe_audio_openai(audio_bytes, filename, openai_key):
    url = "https://api.openai.com/v1/audio/transcriptions"
    boundary = '----AzavaVoiceIngestBoundary'
    
    body = []
    
    # field: model
    body.append(f'--{boundary}'.encode('utf-8'))
    body.append('Content-Disposition: form-data; name="model"'.encode('utf-8'))
    body.append(''.encode('utf-8'))
    body.append('whisper-1'.encode('utf-8'))
    
    # field: file
    body.append(f'--{boundary}'.encode('utf-8'))
    ext = os.path.splitext(filename)[1].lower()
    mime = "audio/mpeg"
    if ext == '.wav':
        mime = "audio/wav"
    elif ext == '.ogg':
        mime = "audio/ogg"
    elif ext == '.m4a':
        mime = "audio/m4a"
        
    body.append(f'Content-Disposition: form-data; name="file"; filename="{filename}"'.encode('utf-8'))
    body.append(f'Content-Type: {mime}'.encode('utf-8'))
    body.append(''.encode('utf-8'))
    body.append(audio_bytes)
    
    # end boundary
    body.append(f'--{boundary}--'.encode('utf-8'))
    body.append(''.encode('utf-8'))
    
    payload = b'\r\n'.join(body)
    
    headers = {
        "Authorization": f"Bearer {openai_key}",
        "Content-Type": f"multipart/form-data; boundary={boundary}",
        "Content-Length": str(len(payload))
    }
    
    req = urllib.request.Request(
        url,
        data=payload,
        headers=headers,
        method="POST"
    )
    
    with urllib.request.urlopen(req) as res:
        res_data = res.read()
        res_json = json.loads(res_data.decode('utf-8'))
        return res_json.get('text', '')

def clean_comp_name(name):
    if not name:
        return ""
    cleaned = name.lower()
    cleaned = re.sub(r'\([^)]*\)', '', cleaned) # remove parentheticals
    cleaned = re.sub(r'\b(inc|ltd|llc|co|corp|ab|gmbh|sa|s\.a\.)\b', '', cleaned)
    cleaned = re.sub(r'[^a-z0-9]', '', cleaned) # remove symbols and spaces
    return cleaned.strip()

def clean_phone(phone):
    if not phone:
        return ""
    cleaned = re.sub(r'\D', '', str(phone))
    return cleaned[-9:] if len(cleaned) >= 9 else cleaned

def resolve_entity_match(db, name, contactPhone):
    clean_in_name = clean_comp_name(name)
    
    # Match by clean name (fuzzy)
    if clean_in_name:
        for c in db.get('companies', []):
            clean_c_name = clean_comp_name(c.get('name'))
            if not clean_c_name:
                continue
            if clean_c_name == clean_in_name:
                return c
            if len(clean_in_name) >= 4 and len(clean_c_name) >= 4:
                if clean_in_name in clean_c_name or clean_c_name in clean_in_name:
                    return c
                    
    return None

def map_azava_fields_to_db(fields):
    db_fields = {}
    
    name_val = fields.get('name') or fields.get('Name') or fields.get('Company') or fields.get('companyName')
    if name_val is not None:
        db_fields['name'] = str(name_val)
        
    stage_val = fields.get('stage') or fields.get('Stage')
    if stage_val is not None:
        db_fields['stage'] = str(stage_val)
        
    source_val = fields.get('source') or fields.get('Source')
    if source_val is not None:
        db_fields['source'] = str(source_val)
        
    notes_val = fields.get('notes') or fields.get('Notes')
    if notes_val is not None:
        db_fields['notes'] = str(notes_val)
        
    contact_name_val = fields.get('contactName') or fields.get('ContactName')
    if contact_name_val is not None:
        db_fields['contactName'] = str(contact_name_val)
        
    contact_phone_val = fields.get('contactPhone') or fields.get('ContactPhone')
    if contact_phone_val is not None:
        db_fields['contactPhone'] = str(contact_phone_val)
        
    deck_url_val = fields.get('deckUrl') or fields.get('DeckUrl')
    if deck_url_val is not None:
        db_fields['deckUrl'] = str(deck_url_val)
        
    amount_val = fields.get('amount') or fields.get('Amount') or fields.get('Amount/Ask') or fields.get('ask')
    if amount_val is not None:
        db_fields['amount'] = str(amount_val)
        
    sector_val = fields.get('sector') or fields.get('Sector')
    if sector_val is not None:
        db_fields['sector'] = str(sector_val)
        
    created_at_val = fields.get('createdAt') or fields.get('ReceivedAt')
    if created_at_val is not None:
        try:
            db_fields['createdAt'] = int(created_at_val)
        except ValueError:
            pass
            
    return db_fields

def upsert_deal_record(db, extracted_data, extra_notes_header=""):
    name = extracted_data.get('name', '').strip()
    contactPhone = extracted_data.get('contactPhone', '').strip()
    
    if not name:
        name = "Inbound Deal"
        
    # Check match
    matched = resolve_entity_match(db, name, contactPhone)
    
    stage = extracted_data.get('stage', '').strip() or "Seed"
    amount = extracted_data.get('amount', '').strip()
    sector = extracted_data.get('sector', '').strip()
    contactName = extracted_data.get('contactName', '').strip()
    deckUrl = extracted_data.get('deckUrl', '').strip()
    notes = extracted_data.get('notes', '').strip()
    oneParagraphSummary = extracted_data.get('oneParagraphSummary', '').strip()
    
    # Note text formatting
    note_text_parts = []
    if extra_notes_header:
        note_text_parts.append(extra_notes_header)
    if notes:
        note_text_parts.append(f"Inbound Notes:\n{notes}")
    if oneParagraphSummary:
        note_text_parts.append(f"Pitch Deck Summary:\n{oneParagraphSummary}")
    if amount:
        note_text_parts.append(f"Ask/Amount: {amount}")
    
    combined_note_text = "\n\n".join(note_text_parts) if note_text_parts else ""
    
    if matched:
        print(f"[Upsert] Updating existing deal: {matched['name']} (ID: {matched['id']})")
        if extracted_data.get('stage'):
            matched['stage'] = stage
        if sector:
            matched['sector'] = sector
        if contactName:
            matched['contactName'] = contactName
        if contactPhone:
            matched['contactPhone'] = contactPhone
        if deckUrl:
            matched['deckUrl'] = deckUrl
        if amount:
            matched['amount'] = amount
            
        if combined_note_text:
            new_note = {
                "id": f"note_{uuid.uuid4().hex[:8]}",
                "text": combined_note_text,
                "createdAt": int(time.time() * 1000)
            }
            matched.setdefault('notes', []).append(new_note)
            
        # Update assets if deckUrl provided
        if deckUrl:
            assets_list = matched.setdefault('assets', [])
            if not any(asset.get('url') == deckUrl for asset in assets_list):
                assets_list.append({
                    "name": "inbound_pitch_deck.pdf",
                    "url": deckUrl,
                    "uploadedAt": int(time.time() * 1000)
                })
                
        # Sync founders
        if contactName:
            founders_list = matched.setdefault('founders', [])
            if not any(f.get('name') == contactName for f in founders_list):
                founders_list.append({"name": contactName, "linkedin": ""})
                
        return matched
    else:
        new_id = f"comp_{uuid.uuid4().hex[:8]}"
        print(f"[Upsert] Creating new deal: {name} (ID: {new_id})")
        new_deal = {
            "id": new_id,
            "name": name,
            "stage": stage,
            "source": "WhatsApp",
            "status": "Active opportunity",
            "sector": sector,
            "contactName": contactName,
            "contactPhone": contactPhone,
            "deckUrl": deckUrl,
            "amount": amount,
            "createdAt": int(time.time() * 1000),
            "priority": "Medium",
            "overview": {},
            "scores": None,
            "rationale": {},
            "notes": [],
            "founders": [],
            "assets": []
        }
        
        if combined_note_text:
            new_note = {
                "id": f"note_{uuid.uuid4().hex[:8]}",
                "text": combined_note_text,
                "createdAt": int(time.time() * 1000)
            }
            new_deal['notes'].append(new_note)
            
        if deckUrl:
            new_deal['assets'].append({
                "name": "inbound_pitch_deck.pdf",
                "url": deckUrl,
                "uploadedAt": int(time.time() * 1000)
            })
            
        if contactName:
            new_deal['founders'].append({"name": contactName, "linkedin": ""})
            
        db.setdefault('companies', []).insert(0, new_deal)
        return new_deal

def ingest_pitch_deck(url, email='simon@bnvtcapital.com', code=''):
    os.makedirs('uploads', exist_ok=True)
    unique_id = uuid.uuid4().hex[:8]
    pdf_path = os.path.join('uploads', f"deck_{unique_id}.pdf")
    
    # Check if direct PDF URL (either via extension, Content-Type header, or %PDF magic bytes)
    is_pdf = False
    if url.lower().endswith('.pdf') or '.pdf?' in url.lower() or '/files/blob/' in url.lower():
        is_pdf = True
    else:
        try:
            import requests
            # Try a quick GET with stream=True to read headers (much more reliable than HEAD for pre-signed/S3 URLs)
            headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
            res_get = requests.get(url, headers=headers, timeout=10, stream=True)
            content_type = res_get.headers.get('Content-Type', '').lower()
            if 'application/pdf' in content_type:
                is_pdf = True
            else:
                # Fallback: check magic bytes
                peek = next(res_get.iter_content(chunk_size=4), b'')
                if peek.startswith(b'%PDF'):
                    is_pdf = True
        except Exception as e:
            print(f"[Deck Ingest] PDF detection error: {e}")

    if is_pdf:
        print(f"[Deck Ingest] Plain PDF URL detected: {url}")
        try:
            import requests
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            res = requests.get(url, headers=headers, timeout=30)
            if res.status_code == 200:
                with open(pdf_path, 'wb') as f:
                    f.write(res.content)
                
                # Extract text
                import pypdf
                reader = pypdf.PdfReader(pdf_path)
                text = ""
                for idx, page in enumerate(reader.pages):
                    text += f"\n--- Slide {idx+1} ---\n"
                    text += page.extract_text() or ""
                
                return {
                    "success": True,
                    "pdf_path": pdf_path,
                    "text": text
                }
            else:
                raise Exception(f"HTTP {res.status_code}")
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed to download/parse plain PDF: {str(e)}"
            }
            
    # Otherwise assume DocSend link
    else:
        print(f"[Deck Ingest] DocSend URL detected: {url}")
        import subprocess
        try:
            proc = subprocess.run(
                [sys.executable, 'docsend_scraper.py', url, email, pdf_path, code],
                capture_output=True,
                text=True
            )
            
            # Parse scraper stdout JSON
            lines = proc.stdout.strip().split('\n')
            result_json = None
            for line in reversed(lines):
                if line.strip().startswith('{') and line.strip().endswith('}'):
                    try:
                        result_json = json.loads(line.strip())
                        break
                    except Exception:
                        continue
            
            if result_json and result_json.get('success'):
                return {
                    "success": True,
                    "pdf_path": pdf_path,
                    "text": result_json.get('text', '')
                }
            else:
                err_msg = result_json.get('error') if result_json else None
                if not err_msg:
                    err_msg = f"Scraper error (code {proc.returncode}): {proc.stderr}"
                return {
                    "success": False,
                    "error": err_msg
                }
        except Exception as e:
            return {
                "success": False,
                "error": f"Failed during DocSend scrape: {str(e)}"
            }

def extract_deck_data_via_claude(deck_text):
    api_key = get_stored_value('anthropic_api_key')
    if not api_key:
        raise Exception("Anthropic API key is not configured")
        
    prompt = f"""You are a principal investment analyst at BNVT Capital.
We have parsed the pitch deck of an inbound deal.
Analyze the pitch deck text and extract the following details in structured JSON:
- name: The company name.
- stage: Funding stage (Pre-seed, Seed, Series A, Series B, Growth, or empty if unknown).
- amount: Ask/Amount raising (e.g. '$1.5M', '$500k', or empty if unknown).
- sector: The sector or industry focus (e.g. 'Robotics', 'Generative AI', 'Dev Tools', etc.).
- oneParagraphSummary: A dense one-paragraph summary of the company's product, market, and business model.

Pitch Deck Text:
{deck_text}

Respond ONLY with valid JSON.
"""
    response_text = call_claude_api(api_key, prompt)
    if not response_text:
        raise Exception("Failed to get response from Claude for pitch deck extraction")
    return extract_json(response_text)

def get_pipeline_summary_text():
    db = read_db()
    companies = db.get('companies', [])
    
    total_deals = len(companies)
    # Open deals: status is not Passed or Killed
    open_deals = [c for c in companies if c.get('status') not in ('Killed', 'Passed')]
    total_open = len(open_deals)
    
    # Counts by stage for open deals
    stage_counts = {}
    for c in open_deals:
        stage = c.get('stage') or 'Unknown'
        stage_counts[stage] = stage_counts.get(stage, 0) + 1
        
    stage_lines = []
    defined_stages = ["Pre-seed", "Seed", "Series A", "Series B", "Growth"]
    for s in defined_stages:
        if s in stage_counts:
            stage_lines.append(f"• {s}: {stage_counts[s]}")
    for s, count in stage_counts.items():
        if s not in defined_stages:
            stage_lines.append(f"• {s}: {count}")
            
    # 5 most recently added/updated
    def get_created_at(c):
        val = c.get('createdAt', 0)
        try:
            return int(val)
        except Exception:
            return 0
            
    sorted_comps = sorted(companies, key=get_created_at, reverse=True)
    recent_deals = sorted_comps[:5]
    
    recent_lines = []
    for idx, c in enumerate(recent_deals, 1):
        name = c.get('name', 'Unknown')
        stage = c.get('stage', 'Unknown')
        created_val = get_created_at(c)
        date_str = ""
        if created_val > 0:
            date_str = time.strftime('%m/%d/%Y', time.localtime(created_val / 1000))
        recent_lines.append(f"{idx}. {name} ({stage}) - {date_str}")
        
    summary_parts = [
        "📊 *BNVT Pipeline Summary*",
        f"Total Deals: {total_deals}",
        f"Active Opportunities: {total_open}",
        "",
        "*By Stage:*",
        "\n".join(stage_lines) if stage_lines else "None",
        "",
        "*Recent Intake:*",
        "\n".join(recent_lines) if recent_lines else "None"
    ]
    
    return "\n".join(summary_parts)

def handle_get_field_value(db, type_id, record_id, field_id):
    companies = db.get('companies', [])
    matched = None
    for c in companies:
        if str(c.get('id')) == str(record_id):
            matched = c
            break
    if not matched:
        return {"ok": False, "error": {"code": "NOT_FOUND", "message": "Record not found", "retryable": False}}
        
    field_lower = field_id.lower()
    if field_lower in ('name', 'company'):
        val = matched.get('name', '')
    elif field_lower == 'stage':
        val = matched.get('stage', '')
    elif field_lower == 'source':
        val = matched.get('source', '')
    elif field_lower == 'notes':
        notes_list = matched.get('notes', [])
        val = "\n".join([n.get('text', '') for n in notes_list if n.get('text')])
    elif field_lower in ('contactname', 'contact_name'):
        val = matched.get('contactName', '')
    elif field_lower in ('contactphone', 'contact_phone'):
        val = matched.get('contactPhone', '')
    elif field_lower in ('deckurl', 'deck_url'):
        val = matched.get('deckUrl', '')
    elif field_lower in ('amount', 'ask', 'amount/ask'):
        val = matched.get('amount', '')
    elif field_lower == 'sector':
        val = matched.get('sector', '')
    elif field_lower in ('createdat', 'receivedat', 'received_at'):
        val = matched.get('createdAt', 0)
    else:
        val = matched.get(field_id, '')
        
    return {"ok": True, "result": val}


def handle_update_record(db, type_id, record_id, fields):
    companies = db.get('companies', [])
    matched = None
    for c in companies:
        if str(c.get('id')) == str(record_id):
            matched = c
            break
            
    if not matched:
        return {"ok": True, "result": {"notFound": True}}
        
    mapped = map_azava_fields_to_db(fields)
    for k, v in mapped.items():
        if k == 'notes':
            new_note = {
                "id": f"note_{uuid.uuid4().hex[:8]}",
                "text": str(v),
                "createdAt": int(time.time() * 1000)
            }
            matched.setdefault('notes', []).append(new_note)
        elif k == 'deckUrl':
            matched['deckUrl'] = v
            assets_list = matched.setdefault('assets', [])
            if not any(asset.get('url') == v for asset in assets_list):
                assets_list.append({
                    "name": "inbound_pitch_deck.pdf",
                    "url": v,
                    "uploadedAt": int(time.time() * 1000)
                })
        else:
            matched[k] = v
            
    if 'contactName' in mapped:
        c_name = mapped['contactName']
        if c_name:
            founders_list = matched.setdefault('founders', [])
            if not any(f.get('name') == c_name for f in founders_list):
                founders_list.append({"name": c_name, "linkedin": ""})
                
    return {
        "ok": True, 
        "result": {
            "id": str(record_id),
            "externalId": str(record_id),
            "adapterType": ADAPTER_TYPE,
            "recordType": type_id or "Deal",
            "data": {}
        }
    }


class DealflowHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query = parse_qs(parsed_url.query)
        
        if parsed_url.path == '/api/storage':
            key = query.get('key', [None])[0]
            if not key:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing key"}).encode('utf-8'))
                return
                
            if os.environ.get("DATABASE_URL"):
                sql_val = get_sql_value(key)
                if sql_val is not None:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"key": key, "value": sql_val}).encode('utf-8'))
                    return
                    
            os.makedirs('db_storage', exist_ok=True)
            path = os.path.join('db_storage', f"{key}.json")
            if os.path.exists(path):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode('utf-8'))
                    return
                except Exception:
                    pass
            
            # Key not found
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"value": None}).encode('utf-8'))
            return
            
        elif parsed_url.path == '/api/whatsapp':
            mode = query.get('hub.mode', [None])[0]
            token = query.get('hub.verify_token', [None])[0]
            challenge = query.get('hub.challenge', [None])[0]
            
            verify_token = "bnvt_dealflow_token"
            
            if mode == 'subscribe' and token == verify_token:
                print(f"[WhatsApp Webhook] Verification successful. Challenge: {challenge}")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                self.wfile.write(challenge.encode('utf-8'))
            else:
                print(f"[WhatsApp Webhook] Verification failed. Token: {token}")
                self.send_response(403)
                self.end_headers()
            return

        elif parsed_url.path == '/pipeline/summary':
            if not check_auth(self):
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Invalid or missing auth token",
                        "retryable": False
                    }
                }).encode('utf-8'))
                return
                
            try:
                summary_text = get_pipeline_summary_text()
                
                accept = self.headers.get('Accept', '')
                is_json = 'application/json' in accept or query.get('format', [''])[0] == 'json'
                
                self.send_response(200)
                if is_json:
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "ok": True,
                        "result": summary_text
                    }).encode('utf-8'))
                else:
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(summary_text.encode('utf-8'))
            except Exception as e:
                traceback.print_exc()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "SUMMARY_FAILED",
                        "message": str(e),
                        "retryable": True
                    }
                }).encode('utf-8'))
            return
            
        super().do_GET()

    def do_POST(self):
        import uuid
        import traceback
        parsed_url = urlparse(self.path)
        
        # Auth check for Azava & Ingest endpoints
        if parsed_url.path in ('/api/azava', '/ingest/voice', '/ingest/deck'):
            if not check_auth(self):
                self.send_response(401)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "UNAUTHORIZED",
                        "message": "Invalid or missing auth token",
                        "retryable": False
                    }
                }).encode('utf-8'))
                return

        if parsed_url.path == '/api/azava':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                payload = json.loads(post_data.decode('utf-8'))
            except Exception:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "BAD_REQUEST",
                        "message": "Malformed JSON payload",
                        "retryable": False
                    }
                }).encode('utf-8'))
                return
                
            method = payload.get('method')
            params = payload.get('params', {})
            print(f"[Azava RPC] Method: {method}, Params: {params}")
            
            try:
                if method == 'manifest':
                    host = self.headers.get('Host', 'localhost:8000')
                    proto = self.headers.get('X-Forwarded-Proto', 'http')
                    dynamic_base_url = f"{proto}://{host}/api/azava"
                    
                    try:
                        with open('manifest.json', 'r', encoding='utf-8') as mf:
                            res = json.load(mf)
                    except Exception:
                        res = {
                            "adapterType": ADAPTER_TYPE,
                            "displayName": "BNVT Dealflow Pipeline",
                            "description": "Adapter connecting Azava to the BNVT Dealflow Pipeline database.",
                            "baseUrl": dynamic_base_url,
                            "authStrategy": { "kind": "bearer" },
                            "methods": [
                                "manifest",
                                "listEntryPoints",
                                "describe",
                                "resolveEntity",
                                "getFieldValue",
                                "getRelated",
                                "createRecord",
                                "updateRecord",
                                "deleteRecord"
                            ]
                        }
                    res["baseUrl"] = dynamic_base_url
                    
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                    return
                    
                elif method == 'listEntryPoints':
                    res = [
                        {
                            "typeId": "Deal",
                            "displayName": "Deal",
                            "readable": True,
                            "writable": True
                        },
                        {
                            "typeId": "ResearchQuery",
                            "displayName": "ResearchQuery",
                            "readable": False,
                            "writable": True
                        }
                    ]
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                    return
                    
                elif method == 'describe':
                    type_id = params.get('typeId') or params.get('recordType')
                    if type_id == 'ResearchQuery':
                        res = {
                            "typeId": "ResearchQuery",
                            "displayName": "ResearchQuery",
                            "fields": [
                                { "fieldId": "CompanyName", "displayName": "CompanyName", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "Question", "displayName": "Question", "kind": "string", "writable": True, "required": True },
                                { "fieldId": "Answer", "displayName": "Answer", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "DeckUrl", "displayName": "DeckUrl", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "DeckEmail", "displayName": "DeckEmail", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "DeckPassword", "displayName": "DeckPassword", "kind": "string", "writable": True, "required": False }
                            ],
                            "references": []
                        }
                    else:
                        res = {
                            "typeId": "Deal",
                            "displayName": "Deal",
                            "fields": [
                                { "fieldId": "Name", "displayName": "Name", "kind": "string", "writable": True, "required": True },
                                { "fieldId": "Stage", "displayName": "Stage", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "Source", "displayName": "Source", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "Notes", "displayName": "Notes", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "ContactName", "displayName": "ContactName", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "ContactPhone", "displayName": "ContactPhone", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "DeckUrl", "displayName": "DeckUrl", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "Amount", "displayName": "Amount", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "Sector", "displayName": "Sector", "kind": "string", "writable": True, "required": False },
                                { "fieldId": "CreatedAt", "displayName": "CreatedAt", "kind": "number", "writable": True, "required": False }
                            ],
                            "references": []
                        }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                    return
                    
                elif method == 'resolveEntity':
                    type_id = params.get('typeId') or params.get('recordType')
                    fields = params.get('fields', {})
                    db = read_db()
                    
                    name = fields.get('name') or fields.get('Company') or fields.get('companyName')
                    contactPhone = fields.get('contactPhone') or fields.get('ContactPhone')
                    
                    matched = resolve_entity_match(db, name, contactPhone)
                    candidates = []
                    if matched:
                        candidates.append({
                            "id": str(matched["id"]),
                            "externalId": str(matched["id"]),
                            "adapterType": ADAPTER_TYPE,
                            "recordType": type_id or "Deal",
                            "data": {}
                        })
                    res = { "candidates": candidates }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                    return
                    
                elif method == 'getFieldValue':
                    position = params.get('position') or {}
                    type_id = position.get('recordType') or params.get('typeId')
                    record_id = position.get('externalId') or params.get('id')
                    field_id = params.get('fieldId')
                    db = read_db()
                    res = handle_get_field_value(db, type_id, record_id, field_id)
                    
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(res).encode('utf-8'))
                    return
                    
                elif method == 'getRelated':
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": []}).encode('utf-8'))
                    return
                    
                elif method == 'createRecord':
                    type_id = params.get('typeId') or params.get('recordType')
                    fields = params.get('fields', {})
                    db = read_db()
                    
                    if type_id == 'ResearchQuery':
                        company_name = fields.get('CompanyName') or ""
                        question = fields.get('Question') or ""
                        deck_url = fields.get('DeckUrl') or ""
                        deck_email = fields.get('DeckEmail') or "simon@bnvtcapital.com"
                        deck_password = fields.get('DeckPassword') or ""
                        
                        # Support parsing encoded parameters from Question (e.g. from WhatsApp automation):
                        # "DECK_INGEST: url={url} email={email} password={password}"
                        # Automatically detect if Question contains a DocSend or PDF URL
                        is_deck_query = False
                        urls = re.findall(r'https?://[^\s]+', question)
                        if urls:
                            first_url = urls[0].split('?')[0].lower()
                            if "docsend.com/view" in first_url or first_url.endswith(".pdf") or "/files/blob/" in first_url:
                                is_deck_query = True
                                
                        if question.startswith("DECK_INGEST:") or is_deck_query:
                            try:
                                if urls:
                                    deck_url = urls[0].rstrip('.,;()[]{}')
                                    
                                # Robust parsing of space-separated or keyword-based arguments
                                email_match = re.search(r'email[=:\s]+([^\s]+)', question, re.IGNORECASE)
                                if email_match:
                                    deck_email = email_match.group(1).strip().rstrip('.,;()[]{}')
                                else:
                                    deck_email = "simon@bnvtcapital.com"
                                    
                                pass_match = re.search(r'(?:password|code|pass)[=:\s]+([^\s]+)', question, re.IGNORECASE)
                                if pass_match:
                                    deck_password = pass_match.group(1).strip().rstrip('.,;()[]{}')
                                else:
                                    deck_password = ""
                            except Exception as parse_err:
                                print(f"Error parsing encoded deck question: {parse_err}")
                                
                        # Handle Pitch Deck Ingestion Flow
                        if deck_url:
                            print(f"[Research Query Deck Ingest] Ingesting deck from URL: {deck_url} (Email: {deck_email}, Password: {bool(deck_password)})")
                            try:
                                ingest_res = ingest_pitch_deck(deck_url, deck_email, deck_password)
                                if not ingest_res.get('success'):
                                    raise Exception(ingest_res.get('error', 'Failed to ingest deck'))
                                    
                                deck_text = ingest_res.get('text', '')
                                pdf_path = ingest_res.get('pdf_path', '')
                                
                                if not deck_text.strip():
                                    raise Exception("Pitch deck text extraction yielded empty content.")
                                    
                                print("[Research Query Deck Ingest] Extracting metadata via Claude...")
                                extracted_data = extract_deck_data_via_claude(deck_text)
                                
                                local_asset_url = ""
                                if pdf_path:
                                    local_asset_url = f"/{pdf_path.replace(os.sep, '/')}"
                                    extracted_data['deckUrl'] = local_asset_url
                                else:
                                    extracted_data['deckUrl'] = deck_url
                                    
                                # Upsert deal to database in a thread-safe way
                                with db_lock:
                                    db_latest = read_db()
                                    deal = upsert_deal_record(db_latest, extracted_data, extra_notes_header="[WhatsApp PDF/DocSend Ingest]")
                                    write_db(db_latest)
                                
                                # Perform concise VC research on the deck's content
                                api_key = get_stored_value('anthropic_api_key') or os.environ.get("ANTHROPIC_API_KEY")
                                if not api_key:
                                    answer = f"📥 *Import Completed!* Added *{deal.get('name')}* to your pipeline.\n\n⚠️ Anthropic API key is not configured on the server to run deck research."
                                else:
                                    research_prompt = f"""You are a senior investment analyst for a venture capital firm, BNVT Capital.
Provide a highly concise, crisp, and professional bulleted summary of this pitch deck.

Pitch Deck Text Content:
{deck_text[:4000]}

Instructions:
1. Analyze the pitch deck content.
2. Structure your answer using these EXACT headings (keep it extremely concise and under 120 words total):
   - **What it does**: 1-sentence crisp description of product/proposition.
   - **Market (TAM)**: Target market segment / size in 1 quick bullet.
   - **Founders**: Names of founders and their LinkedIn URLs (if found).
   - **Funding**: Total raised, latest round, and notable investors.
3. Keep details tight. No pleasantries or meta-commentary."""
                                    
                                    answer_text = call_claude_api(api_key, research_prompt, use_search=False)
                                    answer = f"📥 *Import Completed!* Added *{deal.get('name')}* to your pipeline.\n\n" + answer_text
                                    
                                    # Write research summary back to deal notes in a thread-safe way
                                    with db_lock:
                                        db_latest = read_db()
                                        for comp in db_latest.get('companies', []):
                                            if comp.get('id') == deal.get('id'):
                                                if 'notes' not in comp or not isinstance(comp['notes'], list):
                                                    comp['notes'] = []
                                                comp['notes'].insert(0, {
                                                    "id": str(uuid.uuid4()),
                                                    "text": f"[Automated Pitch Deck Research] {answer_text}",
                                                    "date": int(time.time() * 1000),
                                                    "author": "Azava AI"
                                                })
                                                break
                                        write_db(db_latest)
                            except Exception as e:
                                answer = f"⚠️ Failed to parse pitch deck: {str(e)}"
                        
                        else:
                            # Standard ResearchQuery processing logic
                            # Gather context from all deals in the database for pipeline-wide queries
                            db_deals = db.get('companies', [])
                            db_context = "Here is the list of all deals currently in our pipeline database:\n"
                            for idx, deal in enumerate(db_deals):
                                db_context += f"- {deal.get('name', 'Unknown')} | Stage: {deal.get('stage', 'Unknown')} | Sector: {deal.get('sector', 'Unknown')} | Amount: {deal.get('amount', 'Unknown')} | Notes: {deal.get('notes', 'None')}\n"
                            
                            # Gather context from specific matched deal if name is provided
                            specific_context = ""
                            if company_name:
                                matched = resolve_entity_match(db, company_name, "")
                                if matched:
                                    specific_context = f"\nHere is the detailed existing info about the matched company '{matched.get('name')}' in our database:\n"
                                    specific_context += f"Stage: {matched.get('stage')}\n"
                                    specific_context += f"Sector: {matched.get('sector')}\n"
                                    specific_context += f"Amount: {matched.get('amount')}\n"
                                    specific_context += f"Notes: {matched.get('notes')}\n"
                                    
                            # Call Claude with web search
                            api_key = get_stored_value('anthropic_api_key') or os.environ.get("ANTHROPIC_API_KEY")
                                
                            if not api_key:
                                answer = "⚠️ Anthropic API key is not configured on the server. Please set it to enable deal research."
                            else:
                                # Optimize prompt for automated research vs general questions
                                if "Perform comprehensive VC research" in question or (company_name and not question):
                                    research_prompt = f"""You are a senior investment analyst for a venture capital firm, BNVT Capital.
Provide a highly concise, crisp, and professional bulleted summary of the target company.

Target Company Name: {company_name}

Instructions:
1. Search the web to find the company's website, product, team, and funding.
2. Structure your answer using these EXACT headings (keep it extremely concise and under 120 words total):
   - **What it does**: 1-sentence crisp description of product/proposition.
   - **Market (TAM)**: Target market segment / size in 1 quick bullet.
   - **Founders**: Names (and LinkedIn URLs if available).
   - **Funding**: Total raised, latest round, and notable investors.
3. Keep details tight. No pleasantries or meta-commentary."""
                                else:
                                    research_prompt = f"""You are a senior investment analyst for a venture capital firm, BNVT Capital.
You are helping the team interact with their dealflow pipeline database.

{db_context}
{specific_context}

Question: {question}
Target Company Name: {company_name or 'None specified'}

Instructions:
1. If the question is about the pipeline database (e.g. listing deals, counting deals, summary statistics, grouping by stage/sector, finding specific deals in the list), answer it using the database context provided above.
2. If the question asks for external research about a specific company (e.g. team size, competitors, founders, latest news), use your web_search tool to conduct web research and answer it.
3. Be professional, direct, and concise."""

                                try:
                                    is_external = bool(company_name) or "Perform comprehensive" in question
                                    answer = call_claude_api(api_key, research_prompt, use_search=is_external)
                                    
                                    # Update database notes with the automated research overview in a thread-safe way
                                    if company_name and ("Perform comprehensive" in question or not question) and "⚠️" not in answer:
                                        with db_lock:
                                            db_latest = read_db()
                                            matched = resolve_entity_match(db_latest, company_name, "")
                                            if matched:
                                                for comp in db_latest.get('companies', []):
                                                    if comp.get('id') == matched.get('id'):
                                                        if 'notes' not in comp or not isinstance(comp['notes'], list):
                                                            comp['notes'] = []
                                                        # Remove previous automated research
                                                        comp['notes'] = [n for n in comp['notes'] if "[Automated Research]" not in n.get('text', '')]
                                                        # Insert new concise research note
                                                        comp['notes'].insert(0, {
                                                            "id": str(uuid.uuid4()),
                                                            "text": f"[Automated Research] {answer}",
                                                            "date": int(time.time() * 1000),
                                                            "author": "Azava AI"
                                                        })
                                                        break
                                                write_db(db_latest)
                                except Exception as e:
                                    answer = f"⚠️ Error performing research: {str(e)}"
                                
                        res = {
                            "id": str(uuid.uuid4()),
                            "externalId": str(uuid.uuid4()),
                            "adapterType": ADAPTER_TYPE,
                            "recordType": "ResearchQuery",
                            "data": {
                                "CompanyName": company_name,
                                "Question": question,
                                "Answer": answer
                            }
                        }
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                        return
                        
                    mapped = map_azava_fields_to_db(fields)
                    if not mapped.get('source'):
                        mapped['source'] = 'WhatsApp'
                        
                    with db_lock:
                        db_latest = read_db()
                        new_deal = upsert_deal_record(db_latest, mapped, extra_notes_header="[Azava Intake]")
                        write_db(db_latest)
                    
                    res = {
                        "id": str(new_deal["id"]),
                        "externalId": str(new_deal["id"]),
                        "adapterType": ADAPTER_TYPE,
                        "recordType": type_id or "Deal",
                        "data": {}
                    }
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"ok": True, "result": res}).encode('utf-8'))
                    return
                    
                elif method == 'updateRecord':
                    type_id = params.get('typeId') or params.get('recordType')
                    record_id = params.get('id') or params.get('externalId')
                    fields = params.get('fields', {})
                    with db_lock:
                        db_latest = read_db()
                        res = handle_update_record(db_latest, type_id, record_id, fields)
                        write_db(db_latest)
                    
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(res).encode('utf-8'))
                    return
                    
                elif method == 'deleteRecord':
                    type_id = params.get('typeId') or params.get('recordType')
                    record_id = params.get('id') or params.get('externalId')
                    with db_lock:
                        db_latest = read_db()
                        companies = db_latest.get('companies', [])
                        initial_len = len(companies)
                        db_latest['companies'] = [c for c in companies if str(c.get('id')) != str(record_id)]
                        
                        if len(db_latest['companies']) < initial_len:
                            write_db(db_latest)
                            res = {"ok": True, "result": {"success": True}}
                        else:
                            res = {"ok": False, "error": {"code": "NOT_FOUND", "message": "Record not found", "retryable": False}}
                        
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(res).encode('utf-8'))
                    return
                else:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "ok": False,
                        "error": {
                            "code": "METHOD_NOT_SUPPORTED",
                            "message": f"Method '{method}' not implemented by this adapter",
                            "retryable": False
                        }
                    }).encode('utf-8'))
                    return
            except Exception as method_err:
                traceback.print_exc()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "METHOD_EXECUTION_ERROR",
                        "message": str(method_err),
                        "retryable": True
                    }
                }).encode('utf-8'))
                return

        elif parsed_url.path == '/ingest/voice':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            
            try:
                try:
                    params = json.loads(post_data.decode('utf-8'))
                    audio_url = params.get('url') or params.get('audioUrl')
                    sender_phone = params.get('senderPhone') or params.get('contactPhone') or ""
                    sender_name = params.get('senderName') or params.get('contactName') or ""
                except Exception:
                    params = {}
                    audio_url = None
                    sender_phone = ""
                    sender_name = ""
                
                if audio_url:
                    print(f"[Voice Ingest] Fetching audio from URL: {audio_url}")
                    req = urllib.request.Request(audio_url, headers={"User-Agent": "Mozilla/5.0"})
                    with urllib.request.urlopen(req) as res:
                        audio_bytes = res.read()
                    filename = "voice_note.mp3"
                    parsed_fn = os.path.basename(urlparse(audio_url).path)
                    if any(parsed_fn.lower().endswith(ext) for ext in ['.mp3', '.wav', '.ogg', '.m4a', '.aac']):
                        filename = parsed_fn
                else:
                    print("[Voice Ingest] Reading raw binary audio payload")
                    audio_bytes = post_data
                    filename = "voice_note.mp3"
                    content_type = self.headers.get('Content-Type', '')
                    if 'audio/wav' in content_type:
                        filename = "voice_note.wav"
                    elif 'audio/ogg' in content_type:
                        filename = "voice_note.ogg"
                    elif 'audio/m4a' in content_type:
                        filename = "voice_note.m4a"
                
                if not audio_bytes or len(audio_bytes) < 100:
                    raise Exception("Audio payload is empty or too small")
                    
                openai_key = get_openai_key()
                if not openai_key:
                    raise Exception("OpenAI API key not configured (required for voice note Whisper transcription)")
                    
                print(f"[Voice Ingest] Transcribing audio file '{filename}' ({len(audio_bytes)} bytes) via Whisper...")
                transcript = transcribe_audio_openai(audio_bytes, filename, openai_key)
                print(f"[Voice Ingest] Transcript: {transcript}")
                
                if not transcript.strip():
                    raise Exception("Transcription returned empty text")
                    
                print("[Voice Ingest] Extracting deal details via Claude...")
                api_key = get_stored_value('anthropic_api_key')
                prompt = f"""You are a principal investment analyst at BNVT Capital.
We have received a voice note transcript of an inbound deal intake.
Extract the following information from the transcript in structured JSON:
- name: The company name (or individual name if stealth).
- stage: The company's funding stage (e.g., Pre-seed, Seed, Series A, Series B, Growth, or empty if unknown).
- amount: The ask/amount raising (e.g. '$1.5M', '$500k', or empty if unknown).
- sector: The sector or industry focus (e.g. 'SaaS', 'Robotics', 'Generative AI', etc.).
- contactName: The name of the founder or contact person.
- contactPhone: The phone number of the contact person if mentioned.
- notes: Summary of what they do, highlights, or general notes from the voice note.

Transcript:
{transcript}

Respond ONLY with valid JSON.
"""
                response_text = call_claude_api(api_key, prompt)
                extracted_data = extract_json(response_text)
                
                if sender_phone and not extracted_data.get('contactPhone'):
                    extracted_data['contactPhone'] = sender_phone
                if sender_name and not extracted_data.get('contactName'):
                    extracted_data['contactName'] = sender_name
                
                with db_lock:
                    db_latest = read_db()
                    deal = upsert_deal_record(db_latest, extracted_data, extra_notes_header="[Voice Note Intake]")
                    write_db(db_latest)
                
                summary = {
                    "company": deal.get('name'),
                    "stage": deal.get('stage'),
                    "amount": deal.get('amount') or "Unknown",
                    "sector": deal.get('sector') or "Unknown",
                    "contactName": deal.get('contactName') or "Unknown",
                    "notes": extracted_data.get('notes') or "Inbound deal ingested via voice note.",
                    "transcript": transcript
                }
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "result": summary}).encode('utf-8'))
                
            except Exception as voice_err:
                traceback.print_exc()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "VOICE_INGEST_FAILED",
                        "message": str(voice_err),
                        "retryable": True
                    }
                }).encode('utf-8'))
            return

        elif parsed_url.path == '/ingest/deck':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            
            try:
                try:
                    params = json.loads(post_data.decode('utf-8'))
                except Exception:
                    raise Exception("Invalid JSON request body. Expected properties: url (required), email, code.")
                    
                url = params.get('url')
                if not url:
                    raise Exception("Missing required 'url' parameter in JSON payload.")
                    
                email = params.get('email', 'simon@bnvtcapital.com')
                code = params.get('code') or params.get('password') or ""
                
                print(f"[Deck Ingest] Ingesting deck from URL: {url} (Email: {email}, Code: {bool(code)})")
                ingest_res = ingest_pitch_deck(url, email, code)
                
                if not ingest_res.get('success'):
                    raise Exception(ingest_res.get('error', 'Failed to ingest deck'))
                    
                deck_text = ingest_res.get('text', '')
                pdf_path = ingest_res.get('pdf_path', '')
                
                if not deck_text.strip():
                    raise Exception("Pitch deck text extraction yielded empty content.")
                    
                print("[Deck Ingest] Extracting deck metadata via Claude...")
                extracted_data = extract_deck_data_via_claude(deck_text)
                
                local_asset_url = ""
                if pdf_path:
                    local_asset_url = f"/{pdf_path.replace(os.sep, '/')}"
                    extracted_data['deckUrl'] = local_asset_url
                else:
                    extracted_data['deckUrl'] = url
                
                with db_lock:
                    db_latest = read_db()
                    deal = upsert_deal_record(db_latest, extracted_data, extra_notes_header="[Pitch Deck Intake]")
                    write_db(db_latest)
                
                summary = {
                    "company": deal.get('name'),
                    "stage": deal.get('stage'),
                    "amount": deal.get('amount') or "Unknown",
                    "sector": deal.get('sector') or "Unknown",
                    "summary": extracted_data.get('oneParagraphSummary') or "Pitch deck ingested successfully.",
                    "assetUrl": local_asset_url or url
                }
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"ok": True, "result": summary}).encode('utf-8'))
                
            except Exception as deck_err:
                traceback.print_exc()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "ok": False,
                    "error": {
                        "code": "DECK_INGEST_FAILED",
                        "message": str(deck_err),
                        "retryable": True
                    }
                }).encode('utf-8'))
            return

        if self.path == '/api/upload':
            content_length = int(self.headers.get('Content-Length', 0))
            filename_header = self.headers.get('X-Filename', 'upload.pdf')
            filename = unquote(filename_header)
            safe_filename = "".join(c for c in filename if c.isalnum() or c in ('.', '_', '-')).strip()
            if not safe_filename:
                safe_filename = "upload.pdf"
            
            os.makedirs('uploads', exist_ok=True)
            base, ext = os.path.splitext(safe_filename)
            import uuid
            unique_name = f"{base}_{uuid.uuid4().hex[:8]}{ext}"
            file_path = os.path.join('uploads', unique_name)
            
            try:
                file_data = self.rfile.read(content_length) if content_length > 0 else b''
                with open(file_path, 'wb') as f:
                    f.write(file_data)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "url": f"/uploads/{unique_name}",
                    "name": filename
                }).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "error": {
                        "message": f"Upload failed: {str(e)}"
                    }
                }).encode('utf-8'))
            return

        elif self.path == '/api/anthropic':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            
            # Load LLM config
            llm_config = get_llm_config()
            provider = llm_config.get("provider", "anthropic")
            
            if provider != "anthropic":
                try:
                    import json as pyjson
                    incoming_payload = pyjson.loads(post_data.decode('utf-8'))
                    
                    prov_key = get_stored_value(f"{provider}_api_key") or os.environ.get(f"{provider.upper()}_API_KEY")
                    if not prov_key:
                        # Fallback to incoming auth headers if no local key exists
                        auth_header = self.headers.get('Authorization', '')
                        if auth_header.startswith('Bearer '):
                            prov_key = auth_header[7:].strip()
                            
                    if not prov_key:
                        raise Exception(f"API key for provider '{provider}' is not configured.")
                        
                    model = llm_config.get("search_model") if incoming_payload.get("tools") else llm_config.get("model")
                    
                    openai_messages = []
                    for msg in incoming_payload.get("messages", []):
                        role = msg.get("role")
                        content = msg.get("content")
                        text_content = ""
                        if isinstance(content, str):
                            text_content = content
                        elif isinstance(content, list):
                            for block in content:
                                if isinstance(block, dict):
                                    if block.get("type") == "text":
                                        text_content += block.get("text", "")
                                elif isinstance(block, str):
                                    text_content += block
                        openai_messages.append({"role": role, "content": text_content})
                        
                    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                    
                    # Build request to alternative provider
                    if provider == "openrouter":
                        url = "https://openrouter.ai/api/v1/chat/completions"
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {prov_key}",
                            "HTTP-Referer": "https://bnvt-dealflow.onrender.com",
                            "X-Title": "BNVT Dealflow",
                            "User-Agent": user_agent
                        }
                    elif provider == "deepseek":
                        url = "https://api.deepseek.com/chat/completions"
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {prov_key}",
                            "User-Agent": user_agent
                        }
                    elif provider == "groq":
                        url = "https://api.groq.com/openapi/v1/chat/completions"
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {prov_key}",
                            "User-Agent": user_agent
                        }
                    elif provider == "openai":
                        url = "https://api.openai.com/v1/chat/completions"
                        headers = {
                            "Content-Type": "application/json",
                            "Authorization": f"Bearer {prov_key}",
                            "User-Agent": user_agent
                        }
                        
                    outgoing_payload = {
                        "model": model,
                        "messages": openai_messages,
                        "max_tokens": incoming_payload.get("max_tokens", 4000)
                    }
                    
                    outgoing_req = urllib.request.Request(
                        url,
                        data=pyjson.dumps(outgoing_payload).encode('utf-8'),
                        headers=headers,
                        method="POST"
                    )
                    
                    with urllib.request.urlopen(outgoing_req) as response:
                        res_data = response.read()
                        res_json = pyjson.loads(res_data.decode('utf-8'))
                        choices = res_json.get('choices', [])
                        if choices:
                            assistant_text = choices[0].get('message', {}).get('content', '')
                            # Translate back to Anthropic response format
                            anthropic_response = {
                                "id": f"msg_mock_{uuid.uuid4().hex[:12]}",
                                "type": "message",
                                "role": "assistant",
                                "model": model,
                                "content": [
                                    {
                                        "type": "text",
                                        "text": assistant_text
                                    }
                                ],
                                "stop_reason": "end_turn",
                                "stop_sequence": None,
                                "usage": {
                                    "input_tokens": 0,
                                    "output_tokens": 0
                                }
                            }
                            res_bytes = pyjson.dumps(anthropic_response).encode('utf-8')
                            self.send_response(200)
                            self.send_header("Content-Type", "application/json")
                            self.send_header("Content-Length", str(len(res_bytes)))
                            self.end_headers()
                            self.wfile.write(res_bytes)
                            return
                        else:
                            raise Exception(f"No completions returned from {provider}")
                            
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(pyjson.dumps({
                        "error": {
                            "message": f"Adapter translation failed: {str(e)}"
                        }
                    }).encode('utf-8'))
                    return

            else:
                # Retrieve API key: client header has priority, environment variable is fallback
                api_key = self.headers.get('X-Anthropic-API-Key')
                if not api_key:
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                    
                if not api_key:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "error": {
                            "message": "No Anthropic API Key provided. Please enter it in the 'CLAUDE API KEY' field in the top header of the page, or set the ANTHROPIC_API_KEY environment variable in your terminal before launching."
                        }
                    }).encode('utf-8'))
                    return

                # Read anthropic-beta header from client request
                beta_header = self.headers.get('anthropic-beta')
                if not beta_header:
                    # If not provided, check if web_search is in post_data tools
                    try:
                        import json as pyjson
                        body_json = pyjson.loads(post_data.decode('utf-8'))
                        has_search = False
                        for tool in body_json.get('tools', []):
                            if tool.get('type') == 'web_search_20250305':
                                has_search = True
                                break
                        if has_search:
                            beta_header = "web-search-2025-03-05"
                    except Exception:
                        pass

                # Build Anthropic API request
                headers = {
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "anthropic-dangerously-allow-browser": "true"
                }
                if beta_header:
                    headers["anthropic-beta"] = beta_header
                
                req = urllib.request.Request(
                    "https://api.anthropic.com/v1/messages",
                    data=post_data,
                    headers=headers,
                    method="POST"
                )
                
                try:
                    with urllib.request.urlopen(req) as response:
                        res_data = response.read()
                        self.send_response(response.status)
                        # Forward non-hop-by-hop headers
                        for key, val in response.headers.items():
                            if key.lower() not in ('content-encoding', 'transfer-encoding', 'content-length', 'connection'):
                                self.send_header(key, val)
                        self.send_header('Content-Length', str(len(res_data)))
                        self.end_headers()
                        self.wfile.write(res_data)
                except urllib.error.HTTPError as e:
                    err_data = e.read()
                    self.send_response(e.code)
                    for key, val in e.headers.items():
                        if key.lower() not in ('content-encoding', 'transfer-encoding', 'content-length', 'connection'):
                            self.send_header(key, val)
                    self.send_header('Content-Length', str(len(err_data)))
                    self.end_headers()
                    self.wfile.write(err_data)
                except Exception as e:
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "error": {
                            "message": f"Proxy request failed: {str(e)}"
                        }
                    }).encode('utf-8'))
                return

        elif self.path == '/api/granola':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            
            granola_key = self.headers.get('X-Granola-API-Key')
            if not granola_key:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing X-Granola-API-Key header"}).encode('utf-8'))
                return
                
            try:
                import json as pyjson
                body_json = pyjson.loads(post_data.decode('utf-8'))
                target_url = body_json.get('url')
                method = body_json.get('method', 'GET')
                
                print(f"[Proxy Granola] Requesting {method} {target_url}")
                
                headers = {
                    "Authorization": f"Bearer {granola_key}",
                    "Accept": "application/json"
                }
                
                req = urllib.request.Request(
                    target_url,
                    headers=headers,
                    method=method
                )
                
                with urllib.request.urlopen(req) as response:
                    res_data = response.read()
                    print(f"[Proxy Granola] Response Status: {response.status}")
                    
                    try:
                        parsed = pyjson.loads(res_data.decode('utf-8'))
                        if "/v1/notes/" in target_url:
                            print(f"[Proxy Note Detail] ID: {parsed.get('id')}, Title: {parsed.get('title')}, Calendar Event: {parsed.get('calendar_event', {}).get('event_title') if parsed.get('calendar_event') else 'None'}")
                            attendees = parsed.get('attendees', [])
                            emails = [a.get('email') for a in attendees if a.get('email')]
                            print(f"  Attendees: {emails}")
                        elif "/v1/notes" in target_url:
                            notes_list = parsed.get('notes', [])
                            titles = [n.get('title') for n in notes_list]
                            print(f"[Proxy Notes List] Fetched {len(notes_list)} notes. Titles: {titles}")
                    except Exception as json_err:
                        print(f"[Proxy Note Log Error] {json_err}")
                        
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(res_data)
            except urllib.error.HTTPError as e:
                err_data = e.read()
                print(f"[Proxy Granola] HTTPError Code: {e.code}, URL: {target_url}, Body: {err_data.decode('utf-8', errors='ignore')}")
                self.send_response(e.code)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(err_data)
            except Exception as e:
                print(f"[Proxy Granola] General Error: {str(e)}")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return

        elif self.path == '/api/parse_docsend':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                params = json.loads(post_data.decode('utf-8'))
                url = params.get('url')
                email = params.get('email', 'simon@bnvtcapital.com')
                if not url:
                    raise Exception("Missing 'url' parameter")
                
                import subprocess
                os.makedirs('uploads', exist_ok=True)
                import uuid
                unique_id = uuid.uuid4().hex[:8]
                pdf_path = os.path.join('uploads', f"docsend_{unique_id}.pdf")
                
                password = params.get('password', '')
                
                print(f"Running docsend_scraper.py for {url} with email {email} (has password: {bool(password)})")
                proc = subprocess.run(
                    [sys.executable, 'docsend_scraper.py', url, email, pdf_path, password],
                    capture_output=True,
                    text=True
                )
                
                lines = proc.stdout.strip().split('\n')
                result_json = None
                for line in reversed(lines):
                    if line.strip().startswith('{') and line.strip().endswith('}'):
                        try:
                            result_json = json.loads(line.strip())
                            break
                        except Exception:
                            continue
                
                if result_json and result_json.get('success'):
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(result_json).encode('utf-8'))
                else:
                    error_msg = result_json.get('error') if result_json else None
                    if not error_msg:
                        error_msg = f"Scraper error (code {proc.returncode}): {proc.stderr}"
                    raise Exception(error_msg)
                    
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(e)
                }).encode('utf-8'))
            return

        elif self.path == '/api/extract_text':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                params = json.loads(post_data.decode('utf-8'))
                file_url = params.get('url')
                if not file_url:
                    raise Exception("Missing 'url' parameter")
                
                local_path = file_url.lstrip('/')
                if not os.path.exists(local_path):
                    raise Exception(f"File not found on server: {local_path}")
                
                ext = os.path.splitext(local_path)[1].lower()
                text = ""
                if ext == '.pdf':
                    import pypdf
                    reader = pypdf.PdfReader(local_path)
                    for idx, page in enumerate(reader.pages):
                        text += f"\n--- Page {idx+1} ---\n"
                        text += page.extract_text() or ""
                elif ext == '.docx':
                    text = extract_docx_text(local_path)
                elif ext == '.pptx':
                    text = extract_pptx_text(local_path)
                elif ext == '.xlsx':
                    text = extract_xlsx_text(local_path)
                elif ext in ('.txt', '.csv', '.md', '.json', '.xml', '.html'):
                    with open(local_path, 'r', encoding='utf-8', errors='ignore') as f:
                        text = f.read()
                else:
                    text = f"[Non-textual file: {os.path.basename(local_path)}]"
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "text": text
                }).encode('utf-8'))
                
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(e)
                }).encode('utf-8'))
        elif self.path == '/api/storage':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                data = json.loads(post_data.decode('utf-8'))
                key = data.get('key')
                value = data.get('value')
                if not key:
                    raise Exception("Missing key")
                
                if key == 'bnvt-dealflow-v1':
                    with db_lock:
                        # 1. Read latest database state from server
                        db_latest = read_db()
                        
                        # 2. Parse client-submitted state
                        try:
                            client_db = json.loads(value)
                        except Exception:
                            client_db = {"companies": [], "investors": [], "deepDives": [], "founderProfiles": []}
                            
                        # 3. Perform smart list-level merge to prevent clobbering
                        merged_db = merge_databases(client_db, db_latest)
                        merged_str = json.dumps(merged_db, ensure_ascii=False)
                        
                        # 4. Save merged state to SQL / local JSON file
                        if os.environ.get("DATABASE_URL"):
                            set_sql_value(key, merged_str)
                            
                        os.makedirs('db_storage', exist_ok=True)
                        path = os.path.join('db_storage', f"{key}.json")
                        outer_data = {
                            "key": key,
                            "value": merged_str
                        }
                        with open(path, 'w', encoding='utf-8') as f:
                            json.dump(outer_data, f, indent=2, ensure_ascii=False)
                            
                        # 5. Return success and the merged state back to client
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.end_headers()
                        self.wfile.write(json.dumps({
                            "success": True,
                            "value": merged_str,
                            "merged_value": merged_str
                        }).encode('utf-8'))
                        
                elif key == 'llm_config':
                    try:
                        config_data = json.loads(value)
                    except Exception:
                        config_data = {"provider": "groq", "model": "llama-3.3-70b-versatile", "search_model": "llama-3.3-70b-versatile"}
                        
                    os.makedirs('db_storage', exist_ok=True)
                    path = os.path.join('db_storage', 'llm_config.json')
                    with open(path, 'w', encoding='utf-8') as f:
                        json.dump(config_data, f, indent=2, ensure_ascii=False)
                        
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True}).encode('utf-8'))
                    
                else:
                    # Write key directly to file (e.g. granola_api_key, groq_api_key, etc.)
                    if os.environ.get("DATABASE_URL"):
                        set_sql_value(key, value)
                        
                    os.makedirs('db_storage', exist_ok=True)
                    path = os.path.join('db_storage', f"{key}.json")
                    with open(path, 'w', encoding='utf-8') as f:
                        json.dump({"key": key, "value": value}, f, indent=2, ensure_ascii=False)
                    
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True}).encode('utf-8'))
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return

        elif self.path == '/api/whatsapp':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                payload = json.loads(post_data.decode('utf-8'))
                print(f"[WhatsApp Webhook] Received payload: {json.dumps(payload)}")
                
                message_text = ""
                sender_phone = ""
                sender_name = ""
                
                try:
                    entries = payload.get('entry', [])
                    for entry in entries:
                        changes = entry.get('changes', [])
                        for change in changes:
                            val = change.get('value', {})
                            messages = val.get('messages', [])
                            for msg in messages:
                                if msg.get('type') == 'text':
                                    message_text = msg.get('text', {}).get('body', '')
                                    sender_phone = msg.get('from', '')
                                if val.get('contacts'):
                                    sender_name = val.get('contacts')[0].get('profile', {}).get('name', '')
                except Exception as e:
                    print(f"Error parsing WhatsApp payload: {e}")
                
                if message_text:
                    print(f"[WhatsApp Webhook] Message from {sender_name} ({sender_phone}): {message_text}")
                    success, response_msg = process_whatsapp_message(message_text, sender_phone, sender_name)
                    print(f"[WhatsApp Webhook] Process result: success={success}, msg={response_msg}")
                    res_body = {"success": success, "message": response_msg}
                else:
                    print("[WhatsApp Webhook] No text message found in payload")
                    res_body = {"success": False, "message": "No text message found"}
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(res_body).encode('utf-8'))
            except Exception as e:
                print(f"[WhatsApp Webhook] Error: {e}")
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": str(e)}).encode('utf-8'))
            return

        elif self.path == '/api/anthropic_founder_assessment':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                params = json.loads(post_data.decode('utf-8'))
                profile_id = params.get('id')
                name = params.get('name', '').strip()
                company_name = params.get('company', '').strip()
                title = params.get('title', '').strip()
                raw_profile = params.get('rawProfile', '').strip()
                
                if not name:
                    raise Exception("Missing 'name' parameter")
                
                # Retrieve API key
                api_key = self.headers.get('X-Anthropic-API-Key')
                if not api_key:
                    api_key = get_stored_value('anthropic_api_key')
                if not api_key:
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                if not api_key:
                    raise Exception("Anthropic API key not configured on server or client")
                
                # Read db
                db = read_db()
                
                db.setdefault('founderProfiles', [])
                
                # Prepare prompt
                company_context = f"Company/Affiliation: {company_name}" + (f" ({title})" if title else "")
                raw_profile_section = f"Raw Resume / LinkedIn Profile / Bio:\n{raw_profile}\n" if raw_profile else ""
                
                prompt = f"""You are a principal investment analyst at BNVT Capital, a premium early-stage AI venture capital fund.
We need a rigorous, qualitative Founder Profile Assessment for:
Founder Name: {name}
{company_context}

{raw_profile_section}

Instructions:
1. Conduct research on the founder listed above. Look up their academic history (e.g. PhDs, top tier universities), past employment credentials (e.g. ex-DeepMind, OpenAI, Google, Meta, or successful startup exits), and general reputation.
2. Evaluate qualitative variables for the founder on a scale from 1 to 5 (decimals allowed like 4.5, keep it standard 1-5 scale):
   - ambition: Visionary scope, audacity, scale of their target outcome.
   - execution: History of shipping, velocity, technical competence.
   - resilience: Grit, ability to navigate pivots and hardships.
   - chemistry: Co-founder alignment, history of working together (or collaboration skills).
   - hungerness: Drive, hunger to win, high energy.
   - domainExpertise: Deep domain familiarity, technical or commercial alignment.
3. Write a highly concise, high-density, analytical founder evaluation summary (maximum of 250-300 words). Focus strictly on core credentials/pedigree, chemistry/collaboration, and key risks. Avoid conversational fluff.
4. Classify the founder profile with up to three of the following tags if applicable based on their background (return them in a 'tags' string array):
   - 'Second-time Founder': If they have founded a company or startup previously.
   - 'Young/Fearless/Creative': If they operate in new/emerging industries or are a young, fearless founder or creative engineer.
   - 'Deep Problem Understanding': If they operate in old/legacy industries and understand the industry/problem deeply.
5. Output your response as a valid JSON object ONLY. Do not include markdown code block syntax (like ```json). The JSON object must match this schema:
{{
  "background": "Qualitative Context & Background summary here...",
  "tags": ["Second-time Founder", "Young/Fearless/Creative"],
  "scores": {{
    "ambition": 4.5,
    "execution": 4.2,
    "resilience": 4.0,
    "chemistry": 4.5,
    "hungerness": 4.8,
    "domainExpertise": 4.7
  }},
  "rationale": {{
    "ambition": "One-sentence qualitative justification explaining why this specific score was given for ambition.",
    "execution": "One-sentence qualitative justification explaining why this specific score was given for execution.",
    "resilience": "One-sentence qualitative justification explaining why this specific score was given for resilience.",
    "chemistry": "One-sentence qualitative justification explaining why this specific score was given for chemistry.",
    "hungerness": "One-sentence qualitative justification explaining why this specific score was given for hungerness.",
    "domainExpertise": "One-sentence qualitative justification explaining why this specific score was given for domain expertise."
  }}
}}
"""

                response_text = call_claude_api(api_key, prompt, use_search=True)
                if not response_text:
                    raise Exception("Failed to get response from Claude")
                
                parsed = extract_json(response_text)
                background = parsed.get("background", "")
                scores = parsed.get("scores", {})
                rationale = parsed.get("rationale", {})
                tags = parsed.get("tags", [])
                
                # Build or update founder profile
                assessment_scores = {
                    "ambition": float(scores.get("ambition", 3.0)),
                    "execution": float(scores.get("execution", 3.0)),
                    "resilience": float(scores.get("resilience", 3.0)),
                    "chemistry": float(scores.get("chemistry", 3.0)),
                    "hungerness": float(scores.get("hungerness", 3.0)),
                    "domainExpertise": float(scores.get("domainExpertise", 3.0))
                }
                assessment_rationale = {
                    "ambition": str(rationale.get("ambition", "")),
                    "execution": str(rationale.get("execution", "")),
                    "resilience": str(rationale.get("resilience", "")),
                    "chemistry": str(rationale.get("chemistry", "")),
                    "hungerness": str(rationale.get("hungerness", "")),
                    "domainExpertise": str(rationale.get("domainExpertise", ""))
                }
                
                # Check if we should update or insert in a thread-safe way
                with db_lock:
                    db_latest = read_db()
                    profile = None
                    if profile_id:
                        for p in db_latest.get('founderProfiles', []):
                            if p.get('id') == profile_id:
                                profile = p
                                break
                                
                    if not profile:
                        # Check by name and company as fallback to prevent duplicates
                        for p in db_latest.get('founderProfiles', []):
                            if p.get('name', '').lower() == name.lower() and p.get('company', '').lower() == company_name.lower():
                                profile = p
                                break
                    
                    if profile:
                        # Update
                        profile['name'] = name
                        profile['company'] = company_name
                        profile['title'] = title
                        profile['background'] = background
                        profile['scores'] = assessment_scores
                        profile['rationale'] = assessment_rationale
                        profile['rawProfile'] = raw_profile
                        profile['tags'] = tags
                    else:
                        # Insert new
                        import uuid
                        new_id = f"fp_{uuid.uuid4().hex[:8]}"
                        profile = {
                            "id": new_id,
                            "name": name,
                            "company": company_name,
                            "title": title,
                            "background": background,
                            "scores": assessment_scores,
                            "rationale": assessment_rationale,
                            "rawProfile": raw_profile,
                            "tags": tags,
                            "createdAt": int(time.time() * 1000)
                        }
                        db_latest.setdefault('founderProfiles', []).insert(0, profile)
                    
                    # Save database
                    write_db(db_latest)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "profile": profile,
                    "founderProfiles": db['founderProfiles']
                }).encode('utf-8'))
                
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(e)
                }).encode('utf-8'))
            return

        elif self.path == '/api/log_error':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                err_info = json.loads(post_data.decode('utf-8'))
                print(f"\n[REMOTE BROWSER ERROR] {err_info.get('message')}")
                if err_info.get('stack'):
                    print(f"Stack: {err_info.get('stack')}")
                if err_info.get('source'):
                    print(f"Source: {err_info.get('source')}:{err_info.get('lineno')}:{err_info.get('colno')}")
                print()
            except Exception as e:
                print(f"[Remote Log Parser Error] {e}")
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": True}).encode('utf-8'))
            return

        else:
            self.send_response(404)
            self.end_headers()

    # Log only API proxy requests and errors to keep terminal output clean
    def log_message(self, format, *args):
        try:
            if len(args) >= 2:
                # If first arg is a string (request line), it's a standard access log
                if isinstance(args[0], str):
                    path = args[0]
                    status_code = str(args[1])
                    if "/api/anthropic" in path or "/api/azava" in path or status_code >= '400':
                        super().log_message(format, *args)
                else:
                    # It's an error log (code, message), always print error logs
                    super().log_message(format, *args)
            else:
                super().log_message(format, *args)
        except Exception:
            super().log_message(format, *args)

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

def run():
    if os.environ.get("DATABASE_URL"):
        print("[PostgreSQL] Initializing database...")
        init_postgres()
        
    socketserver.TCPServer.allow_reuse_address = True
    server_address = ('', PORT)
    httpd = ThreadingTCPServer(server_address, DealflowHandler)
    
    print("=================================================================")
    print(f" BNVT Dealflow Atelier local server running at:")
    print(f" http://localhost:{PORT}")
    print("=================================================================")
    print(" Press Ctrl+C to stop the server.")
    print(" To configure Claude API support, either:")
    print("   - Enter your API Key in the top header input on the page")
    print("   - Run: export ANTHROPIC_API_KEY=\"your-key\" before running this script")
    print("=================================================================")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server...")
        httpd.server_close()

if __name__ == '__main__':
    run()
