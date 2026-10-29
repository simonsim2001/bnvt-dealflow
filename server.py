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
import base64
from urllib.parse import urlparse, parse_qs, unquote, quote
import threading
import shutil
from datetime import datetime

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

def get_stored_value_file(key):
    path = os.path.join('db_storage', f"{key}.json")
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                val = data.get('value')
                if isinstance(val, str):
                    val = val.strip()
                    if val == "configured_via_provider":
                        val = None
                return val
        except Exception:
            pass
    return None

def get_db_connection():
    db_url = (
        os.environ.get("DATABASE_URL") or 
        os.environ.get("SUPABASE_DATABASE_URL") or 
        os.environ.get("SUPABASE_DB_URL") or 
        get_stored_value_file("supabase_db_url")
    )
    if not db_url:
        return None
    try:
        import psycopg2
        if db_url.startswith("postgres://"):
            db_url = db_url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(db_url, connect_timeout=5)
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"[PostgreSQL/Supabase Connection Error] {e}")
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
        print("[PostgreSQL/Supabase] Table initialized successfully.")
    except Exception as e:
        print(f"[PostgreSQL/Supabase Init Error] {e}")
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
        print(f"[PostgreSQL/Supabase Get Error for {key}] {e}")
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
        print(f"[PostgreSQL/Supabase Set Error for {key}] {e}")
        return False
    finally:
        conn.close()

def sb_headers(sb_key, extra=None):
    """Legacy JWT keys (eyJ...) go in apikey + Bearer; new sb_secret_/sb_publishable_ keys only in apikey."""
    h = {"apikey": sb_key}
    if sb_key.startswith("eyJ"):
        h["Authorization"] = f"Bearer {sb_key}"
    if extra:
        h.update(extra)
    return h

def get_supabase_rest_value(key):
    sb_url = os.environ.get("SUPABASE_URL") or get_stored_value_file("supabase_url")
    sb_key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or 
        os.environ.get("SUPABASE_KEY") or 
        os.environ.get("SUPABASE_ANON_KEY") or 
        get_stored_value_file("supabase_key")
    )
    if not sb_url or not sb_key:
        return None
    try:
        url = f"{sb_url.rstrip('/')}/rest/v1/key_value_store?key=eq.{quote(key)}&select=value"
        req = urllib.request.Request(
            url,
            headers=sb_headers(sb_key, {"Accept": "application/json"}),
            method="GET"
        )
        with urllib.request.urlopen(req, timeout=5) as resp:
            data = json.loads(resp.read().decode('utf-8'))
            if isinstance(data, list) and len(data) > 0:
                return data[0].get("value")
    except Exception as e:
        print(f"[Supabase REST Get Error for {key}] {e}")
    return None

def set_supabase_rest_value(key, value):
    sb_url = os.environ.get("SUPABASE_URL") or get_stored_value_file("supabase_url")
    sb_key = (
        os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or 
        os.environ.get("SUPABASE_KEY") or 
        os.environ.get("SUPABASE_ANON_KEY") or 
        get_stored_value_file("supabase_key")
    )
    if not sb_url or not sb_key:
        return False
    try:
        url = f"{sb_url.rstrip('/')}/rest/v1/key_value_store"
        payload = json.dumps({"key": key, "value": value}).encode('utf-8')
        req = urllib.request.Request(
            url,
            data=payload,
            headers=sb_headers(sb_key, {"Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}),
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status in (200, 201, 204)
    except Exception as e:
        print(f"[Supabase REST Set Error for {key}] {e}")
        return False

def get_stored_value(key):
    # 1. Local file first: always present for this machine and never blocked by the network.
    val = get_stored_value_file(key)

    # 2. Remote copies (fresh deployments with no local files yet)
    if val is None:
        val = get_sql_value(key)
    if val is None:
        val = get_supabase_rest_value(key)

    # 4. Fallback to Environment Variables
    if val is None or val == "":
        env_map = {
            "anthropic_api_key": "ANTHROPIC_API_KEY",
            "granola_api_key": "GRANOLA_API_KEY",
            "groq_api_key": "GROQ_API_KEY",
            "openrouter_api_key": "OPENROUTER_API_KEY",
            "deepseek_api_key": "DEEPSEEK_API_KEY",
            "openai_api_key": "OPENAI_API_KEY",
            "supabase_url": "SUPABASE_URL",
            "supabase_key": "SUPABASE_KEY",
            "supabase_db_url": "DATABASE_URL"
        }
        if key in env_map:
            val = os.environ.get(env_map[key])

    if isinstance(val, str):
        val = val.strip()
        if val == "configured_via_provider":
            return None
    return val

def get_llm_config():
    config = {"provider": "anthropic", "model": "claude-sonnet-5", "search_model": "claude-sonnet-5"}
    
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
    if get_stored_value("anthropic_api_key"):
        config["provider"] = "anthropic"
        config["model"] = "claude-sonnet-5"
        config["search_model"] = "claude-sonnet-5"
    elif get_stored_value("groq_api_key"):
        config["provider"] = "groq"
        config["model"] = "llama-3.3-70b-versatile"
        config["search_model"] = "llama-3.3-70b-versatile"
    elif get_stored_value("openrouter_api_key"):
        config["provider"] = "openrouter"
        config["model"] = "google/gemini-2.5-flash"
        config["search_model"] = "google/gemini-2.5-flash"
    elif get_stored_value("deepseek_api_key"):
        config["provider"] = "deepseek"
        config["model"] = "deepseek-chat"
        config["search_model"] = "deepseek-chat"
    elif get_stored_value("openai_api_key"):
        config["provider"] = "openai"
        config["model"] = "gpt-4o-mini"
        config["search_model"] = "gpt-4o-mini"
        
    return config

def call_claude_api(api_key, prompt, use_search=False):
    if not api_key or api_key == "configured_via_provider":
        api_key = get_stored_value("anthropic_api_key")
    if isinstance(api_key, str):
        api_key = api_key.strip()
        
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
            "model": config.get("search_model", "claude-sonnet-4-6") if use_search else config.get("model", "claude-sonnet-4-6"),
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
                    
                    if stop_reason == 'end_turn':
                        text_blocks = [b.get('text', '') for b in content if b.get('type') == 'text']
                        if text_blocks:
                            text_val = "\n".join(text_blocks).strip()
                            print(f"[DEBUG CLAUDE] Loop {loop_idx} returning end_turn text: {text_val[:100]}...")
                            return text_val
                        raise Exception(f"Claude resolved request, but no text output was returned: {res_json}")
                    
                    elif stop_reason == 'pause_turn':
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
        prov_key = get_stored_value(f"{provider}_api_key")
        if not prov_key or prov_key == "configured_via_provider":
            prov_key = os.environ.get(f"{provider.upper()}_API_KEY")
        if not prov_key or prov_key == "configured_via_provider":
            prov_key = api_key
        if isinstance(prov_key, str):
            prov_key = prov_key.strip()
            
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
            url = "https://api.groq.com/openai/v1/chat/completions"
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

MASK_PREFIX = "\u2022\u2022\u2022\u2022"
SAFE_KEY_RE = re.compile(r"^[A-Za-z0-9_.-]{1,80}$")

def is_secret_key(key):
    k = (key or "").lower()
    return (k.endswith("_api_key") or k.endswith("_secret") or k.endswith("_token")
            or k in ("supabase_key", "supabase_db_url", "azava_secret", "app_password"))

def mask_secret(val):
    if not isinstance(val, str) or not val.strip():
        return val
    v = val.strip()
    return MASK_PREFIX + (v[-4:] if len(v) > 8 else "")

def is_masked(val):
    return isinstance(val, str) and val.startswith(MASK_PREFIX)

def safe_storage_key(key):
    return bool(key) and bool(SAFE_KEY_RE.match(key)) and ".." not in key

def client_api_key(handler, header="X-Anthropic-API-Key"):
    """A key sent by the browser is ignored if it is just the masked placeholder."""
    v = handler.headers.get(header)
    if not v or is_masked(v) or v == "configured_via_provider" or not v.isascii():
        return None
    return v

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

def get_note_key(n):
    if isinstance(n, dict):
        return str(n.get("id") or n.get("ts") or n.get("date") or n.get("text") or "")
    elif isinstance(n, str):
        return n.strip()
    return str(n)

def normalize_text_notes(notes):
    """Collapse free-text notes (string, or a legacy list of versions) into one string of
    unique paragraphs. Idempotent, so it can safely run on every save."""
    parts = notes if isinstance(notes, list) else [notes]
    paragraphs = []
    for p in parts:
        if isinstance(p, dict):
            p = p.get("text") or ""
        if not isinstance(p, str):
            continue
        for para in re.split(r"\n\s*\n", p):
            para = re.sub(r"(\[object Object\]\s*,?\s*)+", "", para)
            para = re.sub(r"Research failed:[^\n]*", "", para)
            para = para.strip(" ,\n\t")
            if para:
                paragraphs.append(para)
    seen, unique = set(), []
    for para in paragraphs:
        if para not in seen:
            seen.add(para)
            unique.append(para)
    # Drop paragraphs fully contained in a longer one (left over from earlier concatenations).
    unique.sort(key=len, reverse=True)
    kept = []
    for para in unique:
        if not any(para in k for k in kept):
            kept.append(para)
    first_seen = {}
    for i, p in enumerate(paragraphs):
        first_seen.setdefault(p, i)
    kept.sort(key=lambda p: first_seen.get(p, 0))
    return "\n\n".join(kept)

def merge_notes(client_notes, server_notes):
    # Free-text notes (a string on either side) are a single field: last write wins.
    # Unioning text versions is what made notes double in size on every save.
    if isinstance(client_notes, str) or isinstance(server_notes, str):
        if isinstance(client_notes, str) and client_notes.strip():
            return client_notes
        if isinstance(client_notes, list) and client_notes:
            return client_notes
        return server_notes if server_notes is not None else client_notes
    if not isinstance(client_notes, list):
        client_notes = [client_notes] if client_notes else []
    if not isinstance(server_notes, list):
        server_notes = [server_notes] if server_notes else []
        
    notes_map = {}
    for n in server_notes:
        if n is not None:
            key = get_note_key(n)
            if key:
                notes_map[key] = n
                
    for n in client_notes:
        if n is not None:
            key = get_note_key(n)
            if key:
                notes_map[key] = n
                
    res = list(notes_map.values())
    def get_time(n):
        if isinstance(n, dict):
            return n.get("date") or n.get("ts") or n.get("createdAt") or 0
        return 0
    try:
        res.sort(key=get_time, reverse=True)
    except Exception:
        pass
    return res

def merge_sub_list(client_sub, server_sub, identity_fields=("id", "name", "url", "text")):
    if not isinstance(client_sub, list):
        client_sub = [client_sub] if client_sub else []
    if not isinstance(server_sub, list):
        server_sub = [server_sub] if server_sub else []

    item_map = {}
    def get_item_key(item):
        if isinstance(item, dict):
            for field in identity_fields:
                val = item.get(field)
                if val:
                    return str(val)
        return str(item)

    for item in server_sub:
        if item is not None:
            k = get_item_key(item)
            item_map[k] = item

    for item in client_sub:
        if item is not None:
            k = get_item_key(item)
            if k in item_map and isinstance(item, dict) and isinstance(item_map[k], dict):
                merged = dict(item_map[k])
                merged.update(item)
                item_map[k] = merged
            else:
                item_map[k] = item

    return list(item_map.values())

def merge_single_item(client_item, server_item):
    if not server_item:
        return client_item
    if not client_item:
        return server_item

    merged = dict(server_item)
    c_ts = client_item.get("updatedAt") or client_item.get("ts") or 0
    s_ts = server_item.get("updatedAt") or server_item.get("ts") or 0

    for key, c_val in client_item.items():
        s_val = server_item.get(key)
        
        if key == "notes":
            merged["notes"] = merge_notes(c_val, s_val)
        elif key in ("founders", "assets", "documents", "qa", "deckIngestHistory"):
            merged[key] = merge_sub_list(c_val, s_val)
        elif isinstance(c_val, dict) and isinstance(s_val, dict):
            m_dict = dict(s_val)
            m_dict.update(c_val)
            merged[key] = m_dict
        else:
            if c_val is None or c_val == "" or (isinstance(c_val, list) and len(c_val) == 0):
                if s_val is not None and s_val != "" and not (isinstance(s_val, list) and len(s_val) == 0):
                    merged[key] = s_val
                else:
                    merged[key] = c_val
            elif s_ts > c_ts and s_ts > 0 and s_val is not None and s_val != "":
                merged[key] = s_val
            else:
                merged[key] = c_val

    for key, s_val in server_item.items():
        if key not in client_item and s_val is not None:
            merged[key] = s_val

    return merged

def merge_list(client_list, server_list, deleted_ids, key_field):
    deleted_set = set(str(d) for d in deleted_ids)
    client_filtered = [item for item in client_list if isinstance(item, dict) and item.get(key_field) and str(item.get(key_field)) not in deleted_set]
    server_filtered = [item for item in server_list if isinstance(item, dict) and item.get(key_field) and str(item.get(key_field)) not in deleted_set]

    client_map = {item.get(key_field): item for item in client_filtered if item.get(key_field)}
    server_map = {item.get(key_field): item for item in server_filtered if item.get(key_field)}
    
    merged_list = []
    
    for item in client_filtered:
        item_id = item.get(key_field)
        if not item_id:
            merged_list.append(item)
            continue
            
        if item_id in server_map:
            server_item = server_map[item_id]
            merged_item = merge_single_item(item, server_item)
            merged_list.append(merged_item)
        else:
            merged_list.append(item)
            
    for item_id, item in server_map.items():
        if item_id not in client_map:
            merged_list.insert(0, item)
            
    return merged_list

def merge_databases(client_db, server_db):
    client_deleted = client_db.get("deletedIds", []) if isinstance(client_db, dict) else []
    server_deleted = server_db.get("deletedIds", []) if isinstance(server_db, dict) else []
    combined_deleted = list(set(str(d) for d in client_deleted + server_deleted))
    
    investors = merge_list(client_db.get("investors", []), server_db.get("investors", []), combined_deleted, "id")
    for inv in investors:
        if isinstance(inv, dict) and inv.get("notes") is not None and not isinstance(inv.get("notes"), str):
            inv["notes"] = normalize_text_notes(inv["notes"])
        elif isinstance(inv, dict) and isinstance(inv.get("notes"), str) and len(inv["notes"]) > 20000:
            inv["notes"] = normalize_text_notes(inv["notes"])

    return {
        "companies": merge_list(client_db.get("companies", []), server_db.get("companies", []), combined_deleted, "id"),
        "investors": investors,
        "deepDives": merge_list(client_db.get("deepDives", []), server_db.get("deepDives", []), combined_deleted, "id"),
        "founderProfiles": merge_list(client_db.get("founderProfiles", []), server_db.get("founderProfiles", []), combined_deleted, "id"),
        "readingItems": merge_list(client_db.get("readingItems", []), server_db.get("readingItems", []), combined_deleted, "id"),
        "reports": merge_list(client_db.get("reports", []), server_db.get("reports", []), combined_deleted, "id"),
        "voiceSessions": merge_list(client_db.get("voiceSessions", []), server_db.get("voiceSessions", []), combined_deleted, "id"),
        "readingColumns": client_db.get("readingColumns") or server_db.get("readingColumns") or None,
        "deletedIds": combined_deleted
    }

DB_KEY = "bnvt-dealflow-v1"
DB_PATH = os.path.join('db_storage', f"{DB_KEY}.json")
BACKUP_DIR = os.path.join('db_storage', 'backups')
BACKUP_KEEP = 72  # hourly snapshots (~3 days)

def _load_db_file(path):
    with open(path, 'r', encoding='utf-8') as f:
        outer = json.load(f)
    value_str = outer.get('value', '')
    return json.loads(value_str) if value_str else None

def atomic_write_json(path, obj):
    """Write to a temp file then rename, so readers never see a half-written file."""
    os.makedirs(os.path.dirname(path) or '.', exist_ok=True)
    tmp = f"{path}.tmp-{os.getpid()}-{threading.get_ident()}"
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)

def snapshot_db_if_due():
    try:
        if not os.path.exists(DB_PATH):
            return
        os.makedirs(BACKUP_DIR, exist_ok=True)
        stamp = datetime.now().strftime('%Y%m%d-%H')
        target = os.path.join(BACKUP_DIR, f"{DB_KEY}.{stamp}.json")
        if not os.path.exists(target):
            shutil.copy2(DB_PATH, target)
            backups = sorted(p for p in os.listdir(BACKUP_DIR) if p.startswith(DB_KEY))
            for old in backups[:-BACKUP_KEEP]:
                os.remove(os.path.join(BACKUP_DIR, old))
    except Exception as e:
        print(f"[Backup snapshot error] {e}")

def read_db():
    empty = {"companies": [], "investors": [], "deepDives": [], "founderProfiles": []}
    # 1. Local file is the primary copy (always written on every save).
    if os.path.exists(DB_PATH):
        try:
            db = _load_db_file(DB_PATH)
            if db is not None:
                return db
        except Exception as e:
            print(f"[DB file unreadable, trying backups] {e}")
            # Never silently return an empty DB: that would wipe server-only records on merge.
            if os.path.isdir(BACKUP_DIR):
                for name in sorted(os.listdir(BACKUP_DIR), reverse=True):
                    if name.startswith(DB_KEY):
                        try:
                            db = _load_db_file(os.path.join(BACKUP_DIR, name))
                            if db is not None:
                                print(f"[DB recovered from backup {name}]")
                                return db
                        except Exception:
                            continue
            raise RuntimeError("Database file is corrupt and no readable backup was found")
    # 2. No local file yet: try remote copies (fresh install restoring from Supabase).
    stored_str = get_sql_value(DB_KEY) or get_supabase_rest_value(DB_KEY)
    if stored_str:
        try:
            return json.loads(stored_str)
        except Exception as e:
            print(f"[Error parsing remote DB JSON] {e}")
    return empty

def write_db(db):
    db_str = json.dumps(db, ensure_ascii=False)
    try:
        atomic_write_json(DB_PATH, {"key": DB_KEY, "value": db_str})
    except Exception as e:
        print(f"[Error writing db] {e}")
        return False
    snapshot_db_if_due()
    queue_remote_mirror(db_str)
    return True

# ---- Supabase mirror: one worker, always uploads the newest snapshot -------------
_mirror_cond = threading.Condition()
_mirror_pending = {"payload": None}
mirror_status = {"lastSyncAt": None, "lastError": None, "mode": None, "records": None, "recordsError": None}

def supabase_configured():
    has_db = bool(os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")
                  or os.environ.get("SUPABASE_DB_URL") or get_stored_value_file("supabase_db_url"))
    has_rest = bool((os.environ.get("SUPABASE_URL") or get_stored_value_file("supabase_url")) and
                    (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
                     or os.environ.get("SUPABASE_ANON_KEY") or get_stored_value_file("supabase_key")))
    return has_db or has_rest

def queue_remote_mirror(db_str=None):
    if not supabase_configured():
        return
    if db_str is None:
        try:
            db_str = json.dumps(read_db(), ensure_ascii=False)
        except Exception as e:
            mirror_status["lastError"] = f"Could not read local DB: {e}"
            return
    with _mirror_cond:
        _mirror_pending["payload"] = db_str
        _mirror_cond.notify()

def _mirror_worker():
    while True:
        with _mirror_cond:
            while _mirror_pending["payload"] is None:
                _mirror_cond.wait()
            payload = _mirror_pending["payload"]
            _mirror_pending["payload"] = None
        ok_sql = set_sql_value(DB_KEY, payload)
        ok_rest = False if ok_sql else set_supabase_rest_value(DB_KEY, payload)
        try:
            mirror_status["records"] = sync_records(json.loads(payload))
            mirror_status["recordsError"] = None
        except urllib.error.HTTPError as e:
            _remote_hashes.clear()
            mirror_status["recordsError"] = (f"Record tables unavailable (HTTP {e.code}). Run supabase/schema.sql."
                                             if e.code in (404, 400) else f"Record sync HTTP {e.code}")
        except Exception as e:
            _remote_hashes.clear()
            mirror_status["recordsError"] = f"Record sync failed: {e}"
        if ok_sql or ok_rest:
            mirror_status.update(lastSyncAt=datetime.now().isoformat(timespec="seconds"),
                                 lastError=None, mode="postgresql" if ok_sql else "rest")
        else:
            mirror_status["lastError"] = "Upload to Supabase failed (see server log)"
            with _mirror_cond:
                if _mirror_pending["payload"] is None:  # retry unless a newer snapshot arrived
                    _mirror_pending["payload"] = payload
            time.sleep(30)

threading.Thread(target=_mirror_worker, daemon=True, name="supabase-mirror").start()

SUPABASE_SETUP_SQL = ("create table if not exists public.key_value_store (key text primary key, value text); "
                      "alter table public.key_value_store enable row level security;")

def check_supabase_connection():
    """Probe Supabase and explain failures in plain language."""
    if not supabase_configured():
        return {"status": "error", "msg": "Supabase is not configured. Add your project URL and secret key in Settings."}

    # 1. PostgreSQL connection string (creates the table automatically)
    db_url = (os.environ.get("DATABASE_URL") or os.environ.get("SUPABASE_DATABASE_URL")
              or os.environ.get("SUPABASE_DB_URL") or get_stored_value_file("supabase_db_url"))
    pg_error = None
    if db_url:
        conn = get_db_connection()
        if conn:
            conn.close()
            init_postgres()
            return {"status": "ok", "mode": "postgresql", "msg": "Connected to Supabase (PostgreSQL)."}
        pg_error = "The PostgreSQL connection string was rejected or unreachable. Use the Session pooler string from Supabase > Connect."

    # 2. REST API
    sb_url = os.environ.get("SUPABASE_URL") or get_stored_value_file("supabase_url")
    sb_key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
              or os.environ.get("SUPABASE_ANON_KEY") or get_stored_value_file("supabase_key"))
    if not (sb_url and sb_key):
        return {"status": "error", "msg": pg_error or "Missing Supabase URL or key."}
    try:
        req = urllib.request.Request(
            f"{sb_url.rstrip('/')}/rest/v1/key_value_store?select=key&limit=1",
            headers=sb_headers(sb_key))
        urllib.request.urlopen(req, timeout=10).read()
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", "ignore")
        if e.code in (401, 403):
            msg = "Supabase rejected the key. Use the secret (service_role) key from Project Settings > API Keys."
        elif e.code == 404 or "PGRST205" in body or "does not exist" in body:
            msg = "Connected, but the table is missing. Run this in the Supabase SQL Editor: " + SUPABASE_SETUP_SQL
        else:
            msg = f"Supabase returned HTTP {e.code}: {body[:200]}"
        return {"status": "error", "mode": "rest", "msg": msg, "setupSql": SUPABASE_SETUP_SQL}
    except urllib.error.URLError as e:
        return {"status": "error", "mode": "rest",
                "msg": f"Cannot reach {sb_url}. Check the project URL; the project may be paused or deleted. ({e.reason})"}
    except Exception as e:
        return {"status": "error", "mode": "rest", "msg": f"Supabase check failed: {e}"}

    if not set_supabase_rest_value("supabase_test_ping", "ping_ok"):
        return {"status": "error", "mode": "rest",
                "msg": "Read works but writes are blocked. Use the secret (service_role) key, not the anon/publishable key."}
    return {"status": "ok", "mode": "rest", "msg": "Connected to Supabase."}

# ---- Per-record Supabase tables ----------------------------------------------------
RECORD_TABLES = {"companies": "companies", "investors": "investors", "deepDives": "deep_dives",
                 "founderProfiles": "founder_profiles", "readingItems": "reading_items", "reports": "reports"}
_remote_hashes = {}  # table -> {id: hash}, refreshed from Supabase when empty

def _sb_creds():
    url = os.environ.get("SUPABASE_URL") or get_stored_value_file("supabase_url")
    key = (os.environ.get("SUPABASE_SERVICE_ROLE_KEY") or os.environ.get("SUPABASE_KEY")
           or os.environ.get("SUPABASE_ANON_KEY") or get_stored_value_file("supabase_key"))
    return (url.rstrip("/"), key) if url and key else (None, None)

def _sb_request(method, path, body=None, extra=None, timeout=60):
    url, key = _sb_creds()
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(f"{url}/rest/v1/{path}", data=data, method=method,
                                 headers=sb_headers(key, {"Content-Type": "application/json", **(extra or {})}))
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        raw = resp.read()
        return json.loads(raw) if raw else None

def record_hash(rec):
    return hashlib.sha1(json.dumps(rec, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()

def fetch_remote_hashes(table):
    out, offset = {}, 0
    while True:
        rows = _sb_request("GET", f"{table}?select=id,hash&order=id&limit=1000&offset={offset}")
        for r in rows or []:
            out[r["id"]] = r["hash"]
        if not rows or len(rows) < 1000:
            return out
        offset += 1000

def _in_filter(ids):
    return "(" + ",".join('"' + str(i).replace('"', '\\"') + '"' for i in ids) + ")"

def _strip_nul(v):
    if isinstance(v, str):
        return v.replace("\x00", "")
    if isinstance(v, dict):
        return {k: _strip_nul(x) for k, x in v.items()}
    if isinstance(v, list):
        return [_strip_nul(x) for x in v]
    return v

def sync_records(db):
    """Upsert changed records and delete removed ones. Returns a summary dict."""
    url, _ = _sb_creds()
    if not url:
        return {"skipped": "no REST credentials"}
    summary = {}
    for coll, table in RECORD_TABLES.items():
        local = {str(r["id"]): r for r in (db.get(coll) or []) if isinstance(r, dict) and r.get("id")}
        if table not in _remote_hashes:
            _remote_hashes[table] = fetch_remote_hashes(table)
        remote = _remote_hashes[table]
        now = datetime.utcnow().isoformat() + "Z"
        changed = [{"id": i, "data": _strip_nul(r), "hash": record_hash(r), "updated_at": now}
                   for i, r in local.items() if remote.get(i) != record_hash(r)]
        for n in range(0, len(changed), 100):
            batch = changed[n:n + 100]
            _sb_request("POST", f"{table}?on_conflict=id", batch,
                        {"Prefer": "resolution=merge-duplicates,return=minimal"}, timeout=120)
            for row in batch:
                remote[row["id"]] = row["hash"]
        removed = [i for i in list(remote) if i not in local]
        for n in range(0, len(removed), 100):
            batch = removed[n:n + 100]
            _sb_request("DELETE", f"{table}?id=in.{quote(_in_filter(batch), safe='(),')}", None,
                        {"Prefer": "return=minimal"})
            for i in batch:
                remote.pop(i, None)
        summary[table] = {"upserted": len(changed), "deleted": len(removed), "total": len(local)}
    return summary

def verify_records():
    """Compare every local record with Supabase by id + content hash."""
    db = read_db()
    report, all_ok = {}, True
    for coll, table in RECORD_TABLES.items():
        local = {str(r["id"]): record_hash(r) for r in (db.get(coll) or []) if isinstance(r, dict) and r.get("id")}
        try:
            remote = fetch_remote_hashes(table)
            _remote_hashes[table] = dict(remote)
        except urllib.error.HTTPError as e:
            return {"ok": False, "error": f"Table '{table}' missing or inaccessible (HTTP {e.code}). Run supabase/schema.sql in the Supabase SQL Editor."}
        missing = [i for i in local if i not in remote]
        different = [i for i in local if i in remote and remote[i] != local[i]]
        extra = [i for i in remote if i not in local]
        ok = not (missing or different or extra)
        all_ok &= ok
        report[table] = {"local": len(local), "supabase": len(remote), "missing": missing[:20],
                         "different": different[:20], "extra": extra[:20], "ok": ok}
    return {"ok": all_ok, "checkedAt": datetime.now().isoformat(timespec="seconds"), "tables": report}

def get_openai_key():
    path = os.path.join('db_storage', 'openai_api_key.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return (data.get('value') or data.get('key') or "").strip()
        except Exception:
            pass
    val = os.environ.get("OPENAI_API_KEY")
    if isinstance(val, str):
        return val.strip()
    return val

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
    total_companies_cnt = sum(1 for c in companies if c.get('entityType', 'Company') == 'Company')
    total_individuals_cnt = sum(1 for c in companies if c.get('entityType') == 'Individual')

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
        etype = "👤 Individual" if c.get('entityType') == 'Individual' else "🏢 Company"
        created_val = get_created_at(c)
        date_str = ""
        if created_val > 0:
            date_str = time.strftime('%m/%d/%Y', time.localtime(created_val / 1000))
        recent_lines.append(f"{idx}. [{etype}] {name} ({stage}) - {date_str}")
        
    summary_parts = [
        "📊 *BNVT Pipeline Summary*",
        f"Total Opportunities: {total_deals} ({total_companies_cnt} Companies · {total_individuals_cnt} Individuals)",
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


# ---- Affinity CRM Integration Service ----
AFFINITY_DEFAULT_API_KEY = "MIcwWj8tEWh4b7Cth_1HljnCIUFQHud9igdDF1VAYdk"
DEALS_LIST_ID = 149159

STATUS_FIELD_ID = 2949467      # Dropdown / Status
OWNERS_FIELD_ID = 2949468      # Person field
SECTOR_FIELD_ID = 3497697      # Dropdown (string value)
ROUND_FIELD_ID = 2949470       # Dropdown (string value)
AMOUNT_FIELD_ID = 2949469      # Number (float value)
GEOGRAPHY_FIELD_ID = 3497698   # String value (Country)

GEOGRAPHY_COUNTRY_MAP = {
    'us': 'United States', 'usa': 'United States', 'united states': 'United States', 'america': 'United States',
    'uk': 'United Kingdom', 'united kingdom': 'United Kingdom', 'britain': 'United Kingdom', 'london': 'United Kingdom',
    'france': 'France', 'paris': 'France',
    'germany': 'Germany', 'berlin': 'Germany', 'munich': 'Germany',
    'sweden': 'Sweden', 'stockholm': 'Sweden',
    'nordics': 'Sweden',
    'switzerland': 'Switzerland',
    'netherlands': 'Netherlands', 'holland': 'Netherlands', 'amsterdam': 'Netherlands',
    'spain': 'Spain', 'barcelona': 'Spain', 'madrid': 'Spain',
    'finland': 'Finland', 'helsinki': 'Finland',
    'denmark': 'Denmark', 'copenhagen': 'Denmark',
    'norway': 'Norway', 'oslo': 'Norway',
    'ireland': 'Ireland', 'dublin': 'Ireland',
    'italy': 'Italy', 'milan': 'Italy',
    'israel': 'Israel', 'tel aviv': 'Israel',
    'canada': 'Canada', 'toronto': 'Canada',
    'singapore': 'Singapore',
    'eu': 'Europe', 'europe': 'Europe'
}

def normalize_geography_val(geo_val):
    if not geo_val:
        return None
    s = str(geo_val).strip()
    s_lower = s.lower()
    if s_lower in GEOGRAPHY_COUNTRY_MAP:
        return GEOGRAPHY_COUNTRY_MAP[s_lower]
    for key, country in GEOGRAPHY_COUNTRY_MAP.items():
        if key in s_lower or s_lower in key:
            return country
    return s.title()

SIMON_LEVY_PERSON_ID = 250639688

STATUS_MAP = {
    'New': 6133675,                  # Active opportunity / Reach Out
    'Active Opportunity': 6133675,
    'Active opportunity': 6133675,
    'Reach Out': 6133675,
    'Contacted': 6133675,
    'First Meeting': 14358268,       # Meeting Scheduled
    'Meeting Scheduled': 14358268,
    'Second Meeting': 6133676,
    'Due Diligence': 11000819,       # Due Diligence
    'Term Sheet': 6133680,           # Invested
    'Invested': 6133680,             # Invested
    'Portfolio': 6133680,
    'Passed': 6133681,               # Killed / Passed
    'Killed': 6133681,
    'To Kill': 6133681,
    'Watchlist': 6133682,
    'Tracking': 6133682,
    'Pre-emption': 23363837,
    'Pre-Emption': 23363837
}

VALID_ROUNDS = ['Pre-Seed', 'Seed', 'Series A', 'Series B', 'Series C', 'Series D', 'Series E']

def normalize_round_string(round_val):
    if not round_val:
        return None
    s = str(round_val).strip()
    s_clean = re.sub(r'[-_\s]+', '', s.lower())
    if 'pre' in s_clean and 'seed' in s_clean:
        return 'Pre-Seed'
    elif 'seed' in s_clean:
        return 'Seed'
    elif 'seriesa' in s_clean or 'rounda' in s_clean:
        return 'Series A'
    elif 'seriesb' in s_clean or 'roundb' in s_clean:
        return 'Series B'
    elif 'seriesc' in s_clean or 'roundc' in s_clean:
        return 'Series C'
    elif 'seriesd' in s_clean or 'roundd' in s_clean:
        return 'Series D'
    elif 'seriese' in s_clean or 'rounde' in s_clean:
        return 'Series E'
    for vr in VALID_ROUNDS:
        if vr.lower() == s.lower():
            return vr
    return None

def parse_amount_number(amt_val):
    if amt_val is None or amt_val == '':
        return None
    if isinstance(amt_val, (int, float)):
        return float(amt_val)
    s = str(amt_val).strip().upper()
    s = s.replace('$', '').replace(',', '').replace('€', '').replace('£', '').strip()
    if not s:
        return None
    try:
        if s.endswith('M'):
            return float(s[:-1]) * 1_000_000
        elif s.endswith('K'):
            return float(s[:-1]) * 1_000
        elif s.endswith('B'):
            return float(s[:-1]) * 1_000_000_000
        return float(s)
    except Exception:
        return None

COMMON_PLATFORM_DOMAINS = {
    'linkedin.com', 'twitter.com', 'x.com', 'github.com', 'docsend.com', 
    'notion.so', 'medium.com', 'substack.com', 'google.com', 'drive.google.com',
    'youtube.com', 'facebook.com', 'instagram.com'
}

def extract_domain_from_url(url_str):
    if not url_str:
        return None
    url_str = str(url_str).strip()
    if not url_str.startswith(('http://', 'https://')):
        url_str = 'https://' + url_str
    try:
        parsed = urlparse(url_str)
        host = parsed.netloc or parsed.path
        host = host.split(':')[0].lower()
        if host.startswith('www.'):
            host = host[4:]
        if host in COMMON_PLATFORM_DOMAINS or any(host.endswith('.' + d) for d in COMMON_PLATFORM_DOMAINS):
            return None
        if '.' in host and len(host.split('.')[-1]) >= 2:
            return host
    except Exception:
        pass
    return None

def extract_fv_text(res_val):
    if isinstance(res_val, list) and len(res_val) > 0:
        res_val = res_val[0]
    if isinstance(res_val, dict):
        val = res_val.get('value')
        if isinstance(val, dict):
            return val.get('text', '')
        if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
            return val[0].get('text', '')
        return str(val or "")
    return str(res_val or "")

def update_or_create_fv(auth_header, list_entry_id, org_id, existing_fvs, field_id, value):
    if value is None or value == "":
        return None
    existing = next((fv for fv in existing_fvs if isinstance(fv, dict) and fv.get('field_id') == field_id), None)
    if existing:
        put_url = f'https://api.affinity.co/field-values/{existing["id"]}'
        payload = json.dumps({'value': value}).encode()
        req = urllib.request.Request(put_url, data=payload, headers={'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/json'}, method='PUT')
    else:
        post_url = 'https://api.affinity.co/field-values'
        payload = json.dumps({'field_id': field_id, 'entity_id': org_id, 'list_entry_id': list_entry_id, 'value': value}).encode()
        req = urllib.request.Request(post_url, data=payload, headers={'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/json'}, method='POST')
        
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[Affinity Push] Error updating field {field_id} with {value}: {e}")
        return None

def push_affinity_note(auth_header, org_id, content):
    if not content or not content.strip():
        return None
    url = 'https://api.affinity.co/notes'
    payload = json.dumps({
        'organization_ids': [org_id],
        'content': content.strip()
    }).encode()
    req = urllib.request.Request(url, data=payload, headers={'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except Exception as e:
        print(f"[Affinity Push] Error creating note for org {org_id}: {e}")
        return None

def execute_affinity_push(item, api_key=None):
    key = (api_key or AFFINITY_DEFAULT_API_KEY).strip()
    auth_header = base64.b64encode(f':{key}'.encode()).decode()
    
    name = str(item.get('name', '') or '').strip()
    url_val = str(item.get('url', '') or '').strip()
    entity_type = item.get('entityType', 'Company')
    status = item.get('status', 'Reach Out')
    raw_stage = item.get('stage') or item.get('round') or (item.get('overview') or {}).get('stage')
    raw_amount = item.get('amount_raising') or item.get('amountRaising') or item.get('amount') or item.get('raising') or (item.get('overview') or {}).get('amount')
    
    if not name:
        raise Exception("Deal name is required to push to Affinity")
        
    option_id = STATUS_MAP.get(status, STATUS_MAP.get('Reach Out'))
    domain = extract_domain_from_url(url_val) if entity_type == 'Company' else None
    
    org_id = None
    
    # 1. Match existing company by domain if available
    if domain:
        search_req = urllib.request.Request(f'https://api.affinity.co/organizations?term={quote(domain)}')
        search_req.add_header('Authorization', f'Basic {auth_header}')
        with urllib.request.urlopen(search_req) as resp:
            res = json.loads(resp.read().decode())
            org_list = res.get('organizations', []) if isinstance(res, dict) else res
            for org in org_list:
                org_domains = [d.lower() for d in org.get('domains', []) if d]
                if domain.lower() in org_domains or org.get('domain', '').lower() == domain.lower():
                    org_id = org['id']
                    break
    
    # 2. Match existing company by name if domain not matched
    if not org_id and name:
        search_name_req = urllib.request.Request(f'https://api.affinity.co/organizations?term={quote(name)}')
        search_name_req.add_header('Authorization', f'Basic {auth_header}')
        with urllib.request.urlopen(search_name_req) as resp:
            res = json.loads(resp.read().decode())
            org_list = res.get('organizations', []) if isinstance(res, dict) else res
            for org in org_list:
                if org.get('name', '').strip().lower() == name.lower():
                    org_id = org['id']
                    break

    # 3. Create new Organization if not found
    if not org_id:
        create_payload = {'name': name}
        if domain:
            create_payload['domain'] = domain
        create_data = json.dumps(create_payload).encode()
        create_req = urllib.request.Request('https://api.affinity.co/organizations', data=create_data, headers={'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(create_req) as resp:
            new_org = json.loads(resp.read().decode())
            org_id = new_org['id']

    # 4. Find or create List Entry in Deals list (149159)
    entry_id = None
    list_entries_req = urllib.request.Request(f'https://api.affinity.co/lists/{DEALS_LIST_ID}/list-entries')
    list_entries_req.add_header('Authorization', f'Basic {auth_header}')
    with urllib.request.urlopen(list_entries_req) as resp:
        res_entries = json.loads(resp.read().decode())
        entries = res_entries.get('list_entries', []) if isinstance(res_entries, dict) else res_entries
        for ent in entries:
            if isinstance(ent, dict) and ent.get('entity_id') == org_id:
                entry_id = ent['id']
                break
                
    if not entry_id:
        add_entry_payload = json.dumps({'entity_id': org_id}).encode()
        add_entry_req = urllib.request.Request(f'https://api.affinity.co/lists/{DEALS_LIST_ID}/list-entries', data=add_entry_payload, headers={'Authorization': f'Basic {auth_header}', 'Content-Type': 'application/json'})
        with urllib.request.urlopen(add_entry_req) as resp:
            new_entry = json.loads(resp.read().decode())
            entry_id = new_entry['id']

    # 5. Retrieve existing Field Values for List Entry
    fv_req = urllib.request.Request(f'https://api.affinity.co/field-values?list_entry_id={entry_id}')
    fv_req.add_header('Authorization', f'Basic {auth_header}')
    with urllib.request.urlopen(fv_req) as resp:
        fvs = json.loads(resp.read().decode())
        if isinstance(fvs, dict):
            fvs = fvs.get('field_values', [])
            
    # 6. Update Status
    status_res = update_or_create_fv(auth_header, entry_id, org_id, fvs, STATUS_FIELD_ID, option_id)
    status_text = extract_fv_text(status_res) if status_res else status
    
    # 7. Update Owner -> Simon Levy (250639688)
    update_or_create_fv(auth_header, entry_id, org_id, fvs, OWNERS_FIELD_ID, SIMON_LEVY_PERSON_ID)

    # 8. Update Sector -> AI
    update_or_create_fv(auth_header, entry_id, org_id, fvs, SECTOR_FIELD_ID, "AI")

    # 9. Update Round if available
    round_str = normalize_round_string(raw_stage)
    if round_str:
        update_or_create_fv(auth_header, entry_id, org_id, fvs, ROUND_FIELD_ID, round_str)

    # 10. Update Amount Raising if available (solely plain number)
    amt_num = parse_amount_number(raw_amount)
    if amt_num is not None:
        update_or_create_fv(auth_header, entry_id, org_id, fvs, AMOUNT_FIELD_ID, amt_num)

    # 11. Update Geography if available (Country string)
    raw_geo = item.get('geo') or item.get('geography') or item.get('country') or (item.get('overview') or {}).get('geo')
    geo_str = normalize_geography_val(raw_geo)
    if geo_str:
        update_or_create_fv(auth_header, entry_id, org_id, fvs, GEOGRAPHY_FIELD_ID, geo_str)

    # 11. Create & Push Deal Summary Note
    summary_parts = [f"=== BNVT DEAL SUMMARY: {name} ==="]
    if item.get('entityType'):
        summary_parts.append(f"Entity Type: {item.get('entityType')}")
    if item.get('oneLiner'):
        summary_parts.append(f"One-Liner: {item.get('oneLiner')}")
    if url_val:
        summary_parts.append(f"Website / URL: {url_val}")
    if status_text:
        summary_parts.append(f"Status: {status_text}")
    if round_str or raw_stage:
        summary_parts.append(f"Stage / Round: {round_str or raw_stage}")
    if amt_num is not None or raw_amount:
        summary_parts.append(f"Amount Raising: {amt_num if amt_num is not None else raw_amount}")
    if item.get('geo'):
        summary_parts.append(f"Geography: {item.get('geo')}")
    if item.get('priority'):
        summary_parts.append(f"Priority: {item.get('priority')}")
    if item.get('source'):
        summary_parts.append(f"Source: {item.get('source')}")

    overview = item.get('overview') or {}
    if isinstance(overview, dict) and any(overview.values()):
        summary_parts.append("\n--- OVERVIEW ---")
        if overview.get('product'):
            summary_parts.append(f"• Product / Focus: {overview.get('product')}")
        if overview.get('market'):
            summary_parts.append(f"• Market: {overview.get('market')}")
        if overview.get('team'):
            summary_parts.append(f"• Team & Background: {overview.get('team')}")
        if overview.get('traction'):
            summary_parts.append(f"• Traction & Signals: {overview.get('traction')}")

    scores = item.get('scores') or {}
    rationale = item.get('rationale') or {}
    if isinstance(scores, dict) and any(scores.values()):
        summary_parts.append("\n--- DEFENSIBILITY & SCORING ---")
        metric_names = {
            'proprietaryTech': 'Proprietary Tech',
            'dataMoat': 'Data Moat',
            'distributionSwitching': 'Distribution / Switching',
            'modelAgnostic': 'Model Agnostic',
            'ipTimeToReplicate': 'IP / Time to Replicate',
            'tam': 'TAM / Market Scale'
        }
        for k, label in metric_names.items():
            if k in scores:
                score_val = scores[k]
                rat_val = rationale.get(k, '') if isinstance(rationale, dict) else ''
                summary_parts.append(f"• {label}: {score_val}/5{f' — {rat_val}' if rat_val else ''}")

    summary_note_text = "\n".join(summary_parts)
    push_affinity_note(auth_header, org_id, summary_note_text)

    # 12. Create & Push Granola Call Transcripts / Notes (as ONE note each)
    granola_list = item.get('granolaNotes') or item.get('granola_notes') or []
    if not isinstance(granola_list, list):
        granola_list = []
    
    # Also extract call/meeting notes from item.get('notes') if present
    crm_notes = item.get('notes') or []
    if isinstance(crm_notes, list):
        for n in crm_notes:
            if isinstance(n, dict):
                via = str(n.get('via', '')).lower()
                text = str(n.get('text', ''))
                if 'granola' in via or 'transcript' in text.lower() or 'meeting note' in text.lower():
                    if not any(g.get('summary') == text or g.get('text') == text for g in granola_list if isinstance(g, dict)):
                        granola_list.append({
                            'title': f"Meeting Note ({n.get('via', 'Note')})",
                            'summary': text
                        })

    pushed_granola_count = 0
    for gnote in granola_list:
        if not isinstance(gnote, dict):
            continue
        g_title = gnote.get('title') or gnote.get('name') or (gnote.get('calendar_event') or {}).get('event_title') or "Granola Meeting Note"
        g_date = gnote.get('created_at') or gnote.get('createdAt') or gnote.get('date') or ""
        g_summary = gnote.get('summary') or gnote.get('text') or gnote.get('overview') or ""
        g_transcript = gnote.get('transcript') or gnote.get('raw_transcript') or ""
        
        g_parts = [f"=== GRANOLA MEETING NOTE: {g_title} ==="]
        if g_date:
            g_parts.append(f"Date: {g_date}")
        if gnote.get('attendees'):
            att = gnote['attendees']
            if isinstance(att, list):
                g_parts.append(f"Attendees: {', '.join(str(a) for a in att)}")
            else:
                g_parts.append(f"Attendees: {att}")
                
        if g_summary:
            g_parts.append(f"\n--- SUMMARY / HIGHLIGHTS ---\n{g_summary}")
        if g_transcript:
            g_parts.append(f"\n--- FULL TRANSCRIPT ---\n{g_transcript}")
            
        note_content = "\n".join(g_parts).strip()
        if note_content:
            res_note = push_affinity_note(auth_header, org_id, note_content)
            if res_note:
                pushed_granola_count += 1

    affinity_url = f"https://9900.affinity.co/companies/{org_id}"

    return {
        'success': True,
        'affinity_org_id': org_id,
        'list_entry_id': entry_id,
        'status': status_text,
        'owner': 'Simon Levy',
        'sector': 'AI',
        'round': round_str,
        'amount_raising': amt_num,
        'granola_notes_pushed': pushed_granola_count,
        'affinity_url': affinity_url
    }



# ---- Access control --------------------------------------------------------------
import hmac, hashlib
from http import cookies as http_cookies

STATIC_FILES = {"/": "index.html", "/index.html": "index.html", "/bnvt-dealflow.js": "bnvt-dealflow.js",
                "/manifest.json": "manifest.json", "/app.webmanifest": "app.webmanifest", "/icon-180.png": "icon-180.png", "/icon-512.png": "icon-512.png"}
# Called by external services that carry their own token (or by Meta's webhook).
PUBLIC_PATHS = {"/healthz", "/login", "/logout", "/api/whatsapp", "/api/azava",
                "/ingest/voice", "/ingest/deck", "/pipeline/summary",
                "/app.webmanifest", "/icon-180.png", "/icon-512.png"}
SESSION_DAYS = 30

def app_password():
    return (os.environ.get("APP_PASSWORD") or get_stored_value_file("app_password") or "").strip()

def _session_secret():
    base = os.environ.get("SESSION_SECRET") or ("bnvt-session:" + app_password())
    return hashlib.sha256(base.encode()).digest()

def make_session_cookie():
    exp = str(int(time.time()) + SESSION_DAYS * 86400)
    sig = hmac.new(_session_secret(), exp.encode(), hashlib.sha256).hexdigest()
    return f"{exp}.{sig}"

def valid_session(handler):
    raw = handler.headers.get("Cookie")
    if not raw:
        return False
    try:
        c = http_cookies.SimpleCookie(raw)
        exp, sig = c["bnvt_session"].value.split(".", 1)
    except Exception:
        return False
    good = hmac.new(_session_secret(), exp.encode(), hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, good) and int(exp) > time.time()

LOGIN_HTML = """<!DOCTYPE html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>SL Deals · Sign in</title>
<style>body{margin:0;font-family:Arial,Helvetica,sans-serif;background:#fff;color:#111;display:flex;
align-items:center;justify-content:center;height:100vh}form{width:300px}h1{font-size:18px;margin:0 0 24px;
letter-spacing:.02em}input{width:100%;box-sizing:border-box;padding:10px 12px;border:1px solid #111;font-size:14px;
margin-bottom:12px}button{width:100%;padding:10px;background:#000;color:#fff;border:0;font-size:14px;cursor:pointer}
.err{color:#C00000;font-size:13px;margin-bottom:12px}</style></head><body>
<form method="POST" action="/login"><h1>SL Deals</h1>__ERR__
<input type="password" name="password" placeholder="Team password" autofocus required>
<button type="submit">Sign in</button></form></body></html>"""

def gate_request(handler, method):
    """Return True if the request may proceed; otherwise send the response and return False."""
    path = urlparse(handler.path).path
    if path in PUBLIC_PATHS:
        return True
    is_api = path.startswith("/api/")
    if not is_api and path not in STATIC_FILES:
        handler.send_response(404)
        handler.end_headers()
        return False
    if not app_password() or valid_session(handler):
        return True
    if is_api or method != "GET":
        handler.send_response(401)
        handler.send_header("Content-Type", "application/json")
        handler.end_headers()
        handler.wfile.write(b'{"error":"Not signed in"}')
    else:
        handler.send_response(302)
        handler.send_header("Location", "/login")
        handler.end_headers()
    return False

def handle_login_logout(handler, method):
    path = urlparse(handler.path).path
    if path == "/logout":
        handler.send_response(302)
        handler.send_header("Set-Cookie", "bnvt_session=; Path=/; Max-Age=0; HttpOnly; SameSite=Lax")
        handler.send_header("Location", "/login")
        handler.end_headers()
        return True
    if path != "/login":
        return False
    err = ""
    if method == "POST":
        length = int(handler.headers.get("Content-Length", 0) or 0)
        form = parse_qs(handler.rfile.read(length).decode("utf-8", "ignore"))
        given = (form.get("password", [""])[0]).strip()
        pw = app_password()
        if pw and hmac.compare_digest(given.encode(), pw.encode()):
            secure = "; Secure" if handler.headers.get("X-Forwarded-Proto") == "https" else ""
            handler.send_response(302)
            handler.send_header("Set-Cookie", f"bnvt_session={make_session_cookie()}; Path=/; Max-Age={SESSION_DAYS*86400}; HttpOnly; SameSite=Lax{secure}")
            handler.send_header("Location", "/")
            handler.end_headers()
            return True
        time.sleep(1)  # slow down guessing
        err = '<div class="err">Wrong password.</div>'
    body = LOGIN_HTML.replace("__ERR__", err).encode()
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.end_headers()
    handler.wfile.write(body)
    return True

def handle_healthz(handler):
    if urlparse(handler.path).path != "/healthz":
        return False
    ok, detail = True, {}
    try:
        db = read_db()
        detail["companies"] = len(db.get("companies", []))
    except Exception as e:
        ok, detail["db_error"] = False, str(e)
    detail["supabase"] = {"lastSyncAt": mirror_status.get("lastSyncAt"), "lastError": mirror_status.get("lastError")}
    handler.send_response(200 if ok else 503)
    handler.send_header("Content-Type", "application/json")
    handler.end_headers()
    handler.wfile.write(json.dumps({"ok": ok, **detail}).encode())
    return True


# ---------------- Reading library: files + link metadata ----------------
READING_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads", "reading")
READING_MAX_BYTES = 30 * 1024 * 1024

def _reading_safe_name(name):
    base = re.sub(r"[^A-Za-z0-9._-]+", "_", os.path.basename(name or "file"))[:80] or "file"
    return f"{int(time.time())}_{uuid.uuid4().hex[:8]}_{base}"

def reading_store_file(name, content_type, data):
    """Store in Supabase Storage bucket 'reading' when configured; always keep a local copy."""
    fname = _reading_safe_name(name)
    os.makedirs(READING_DIR, exist_ok=True)
    tmp = os.path.join(READING_DIR, fname + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, os.path.join(READING_DIR, fname))
    remote = False
    url, key = _sb_creds()
    if url and key:
        try:
            req = urllib.request.Request(f"{url}/storage/v1/object/reading/{quote(fname)}", data=data, method="POST",
                                         headers=sb_headers(key, {"Content-Type": content_type or "application/octet-stream", "x-upsert": "true"}))
            urllib.request.urlopen(req, timeout=60).read()
            remote = True
        except Exception as e:
            print("[reading] Supabase storage upload failed:", e)
    return {"path": fname, "remote": remote, "url": f"/api/reading/file?path={quote(fname)}"}

def reading_load_file(fname):
    if not fname or "/" in fname or "\\" in fname or fname.startswith("."):
        return None
    local = os.path.join(READING_DIR, fname)
    if os.path.isfile(local):
        with open(local, "rb") as f:
            return f.read()
    url, key = _sb_creds()
    if url and key:
        try:
            req = urllib.request.Request(f"{url}/storage/v1/object/reading/{quote(fname)}", headers=sb_headers(key))
            data = urllib.request.urlopen(req, timeout=60).read()
            os.makedirs(READING_DIR, exist_ok=True)
            with open(local, "wb") as f:
                f.write(data)
            return data
        except Exception as e:
            print("[reading] Supabase storage fetch failed:", e)
    return None

def reading_link_meta(link):
    import html as _html
    if not re.match(r"^https?://", link or ""):
        return {"ok": False, "error": "Link must start with http:// or https://"}
    req = urllib.request.Request(link, headers={"User-Agent": "Mozilla/5.0 (BNVT reading library)"})
    with urllib.request.urlopen(req, timeout=15) as resp:
        raw = resp.read(600000).decode("utf-8", "ignore")
    def meta(*names):
        for n in names:
            m = re.search(r'<meta[^>]+(?:property|name)=["\']' + re.escape(n) + r'["\'][^>]*content=["\']([^"\']*)', raw, re.I) \
                or re.search(r'<meta[^>]+content=["\']([^"\']*)["\'][^>]*(?:property|name)=["\']' + re.escape(n) + r'["\']', raw, re.I)
            if m and m.group(1).strip():
                return _html.unescape(m.group(1).strip())
        return ""
    title = meta("og:title", "twitter:title")
    if not title:
        m = re.search(r"<title[^>]*>(.*?)</title>", raw, re.I | re.S)
        title = _html.unescape(re.sub(r"\s+", " ", m.group(1)).strip()) if m else ""
    return {"ok": True, "title": title[:300], "description": meta("og:description", "description", "twitter:description")[:1000],
            "site": meta("og:site_name") or urlparse(link).netloc.replace("www.", "")}


class DealflowHandler(http.server.SimpleHTTPRequestHandler):
    def do_HEAD(self):
        if gate_request(self, "HEAD"):
            super().do_HEAD()
    def end_headers(self):
        # Always revalidate so a rebuilt app appears on a normal reload.
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Expires", "0")
        super().end_headers()

    def do_GET(self):
        if handle_healthz(self) or handle_login_logout(self, "GET"):
            return
        if not gate_request(self, "GET"):
            return
        parsed_url = urlparse(self.path)
        query = parse_qs(parsed_url.query)
        
        if parsed_url.path == '/api/affinity/auth':
            try:
                api_key = query.get('key', [None])[0] or self.headers.get('X-Affinity-Api-Key')
                if not api_key or api_key == "configured_via_provider":
                    api_key = get_stored_value('affinity_api_key')
                if isinstance(api_key, str):
                    api_key = api_key.strip()
                if not api_key:
                    raise Exception("No Affinity API Key provided")
                
                auth_b64 = base64.b64encode(f":{api_key}".encode('utf-8')).decode('utf-8')
                req = urllib.request.Request(
                    "https://api.affinity.co/auth/whoami",
                    headers={
                        "Authorization": f"Basic {auth_b64}",
                        "Content-Type": "application/json"
                    }
                )
                with urllib.request.urlopen(req) as resp:
                    who_data = json.loads(resp.read().decode('utf-8'))
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                        "tenant": who_data.get('tenant'),
                        "user": who_data.get('user')
                    }).encode('utf-8'))
                    return
            except Exception as auth_err:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(auth_err)
                }).encode('utf-8'))
                return

        elif parsed_url.path == '/api/storage':
            key = query.get('key', [None])[0]
            if key and not safe_storage_key(key):
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Invalid key"}).encode('utf-8'))
                return
            if key and is_secret_key(key):
                # Secrets never leave the server: report only whether one is configured.
                val = get_stored_value(key)
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"key": key, "value": mask_secret(val) if val else None, "masked": True}).encode('utf-8'))
                return
            if not key:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Missing key"}).encode('utf-8'))
                return
                
            if os.environ.get("DATABASE_URL"):
                sql_val = get_sql_value(key)
                if sql_val is not None:
                    if isinstance(sql_val, str):
                        sql_val = sql_val.strip()
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
                    # Strip if string value exists in data dict
                    if data and isinstance(data.get('value'), str):
                        data['value'] = data['value'].strip()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode('utf-8'))
                    return
                except Exception:
                    pass
            
            # Fallback to environment variables for API keys
            env_map = {
                "anthropic_api_key": "ANTHROPIC_API_KEY",
                "groq_api_key": "GROQ_API_KEY",
                "openrouter_api_key": "OPENROUTER_API_KEY",
                "deepseek_api_key": "DEEPSEEK_API_KEY",
                "openai_api_key": "OPENAI_API_KEY",
                "granola_api_key": "GRANOLA_API_KEY"
            }
            if key in env_map:
                env_val = os.environ.get(env_map[key])
                if env_val:
                    if isinstance(env_val, str):
                        env_val = env_val.strip()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"key": key, "value": env_val}).encode('utf-8'))
                    return
            
            # Key not found
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"value": None}).encode('utf-8'))
        elif parsed_url.path == '/api/reading/file':
            fname = unquote(parse_qs(parsed_url.query).get("path", [""])[0])
            data = reading_load_file(fname)
            if data is None:
                self.send_response(404); self.end_headers(); return
            import mimetypes
            ctype = mimetypes.guess_type(fname)[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Content-Disposition", 'inline; filename="%s"' % fname.split("_", 2)[-1])
            self.end_headers()
            self.wfile.write(data)
            return
        elif parsed_url.path == '/api/supabase/verify':
            try:
                result = verify_records() if _sb_creds()[0] else {"ok": False, "error": "Supabase not configured"}
            except Exception as e:
                result = {"ok": False, "error": str(e)}
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode('utf-8'))
            return
        elif parsed_url.path == '/api/supabase/status':
            result = check_supabase_connection()
            if result["status"] == "ok":
                queue_remote_mirror()  # push the full current database right away
            result.update(lastSyncAt=mirror_status["lastSyncAt"], lastSyncError=mirror_status["lastError"],
                          records=mirror_status.get("records"), recordsError=mirror_status.get("recordsError"))
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode('utf-8'))
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
        if handle_login_logout(self, "POST"):
            return
        if not gate_request(self, "POST"):
            return
        parsed_url = urlparse(self.path)
        if parsed_url.path in ('/api/reading/upload', '/api/reading/meta'):
            try:
                n = int(self.headers.get('Content-Length', 0))
                if n > READING_MAX_BYTES * 1.4:
                    raise ValueError("File too large (max 30 MB)")
                body = json.loads(self.rfile.read(n).decode('utf-8') or "{}")
                if parsed_url.path == '/api/reading/upload':
                    data = base64.b64decode(body.get("data", ""))
                    if not data:
                        raise ValueError("Empty file")
                    result = {"ok": True, **reading_store_file(body.get("name"), body.get("type"), data)}
                else:
                    result = reading_link_meta(body.get("url", "").strip())
                code = 200
            except Exception as e:
                result, code = {"ok": False, "error": str(e)}, 400
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(result).encode('utf-8'))
            return
        
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
                                api_key = get_stored_value('anthropic_api_key')
                                if isinstance(api_key, str):
                                    api_key = api_key.strip()
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
                            api_key = get_stored_value('anthropic_api_key')
                            if isinstance(api_key, str):
                                api_key = api_key.strip()
                                
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
                    
                    prov_key = get_stored_value(f"{provider}_api_key")
                    if not prov_key or prov_key == "configured_via_provider":
                        prov_key = os.environ.get(f"{provider.upper()}_API_KEY")
                    if not prov_key or prov_key == "configured_via_provider":
                        # Fallback to incoming auth headers if no local key exists
                        auth_header = self.headers.get('Authorization', '')
                        if auth_header.startswith('Bearer '):
                            prov_key = auth_header[7:].strip()
                    if isinstance(prov_key, str):
                        prov_key = prov_key.strip()
                            
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
                        url = "https://api.groq.com/openai/v1/chat/completions"
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
                api_key = client_api_key(self)
                if not api_key or api_key == "configured_via_provider":
                    api_key = get_stored_value('anthropic_api_key')
                if not api_key or api_key == "configured_via_provider":
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                if isinstance(api_key, str):
                    api_key = api_key.strip().strip('"').strip("'")
                    
                if not api_key:
                    self.send_response(400)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "error": {
                            "message": "No Anthropic API Key provided. Please enter it in Settings or set ANTHROPIC_API_KEY."
                        }
                    }).encode('utf-8'))
                    return

                try:
                    import json as pyjson
                    body_json = pyjson.loads(post_data.decode('utf-8'))
                    has_search = False
                    for tool in body_json.get('tools', []):
                        if tool.get('type') == 'web_search_20250305' or tool.get('name') == 'web_search':
                            has_search = True
                            break
                    
                    if has_search:
                        # Extract prompt text
                        prompt_text = ""
                        messages = body_json.get('messages', [])
                        if messages:
                            last_msg = messages[-1]
                            content = last_msg.get('content')
                            if isinstance(content, str):
                                prompt_text = content
                            elif isinstance(content, list):
                                prompt_text = "".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
                        
                        resolved_text = call_claude_api(api_key, prompt_text, use_search=True)
                        response_obj = {
                            "id": f"msg_resolved_{uuid.uuid4().hex[:10]}",
                            "type": "message",
                            "role": "assistant",
                            "model": body_json.get("model", "claude-sonnet-4-6"),
                            "content": [{"type": "text", "text": resolved_text}],
                            "stop_reason": "end_turn"
                        }
                        res_bytes = pyjson.dumps(response_obj).encode('utf-8')
                        self.send_response(200)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(res_bytes)))
                        self.end_headers()
                        self.wfile.write(res_bytes)
                        return
                except Exception as parse_err:
                    print(f"[Anthropic Proxy Search Auto-Resolve Exception] {parse_err}")

                # Standard Anthropic API proxy fallback via urllib.request
                beta_header = self.headers.get('anthropic-beta')
                headers = {
                    "Content-Type": "application/json",
                    "x-api-key": api_key,
                    "anthropic-version": "2023-06-01",
                    "anthropic-dangerously-allow-browser": "true"
                }
                if beta_header:
                    headers["anthropic-beta"] = beta_header
                
                try:
                    req = urllib.request.Request(
                        "https://api.anthropic.com/v1/messages",
                        data=post_data,
                        headers=headers,
                        method="POST"
                    )
                    with urllib.request.urlopen(req, timeout=180) as response:
                        res_bytes = response.read()
                        self.send_response(response.status)
                        self.send_header("Content-Type", "application/json")
                        self.send_header("Content-Length", str(len(res_bytes)))
                        self.end_headers()
                        self.wfile.write(res_bytes)
                        return
                except urllib.error.HTTPError as http_err:
                    err_bytes = http_err.read()
                    self.send_response(http_err.code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err_bytes)))
                    self.end_headers()
                    self.wfile.write(err_bytes)
                    return
                except Exception as e:
                    err_msg = json.dumps({"error": {"message": f"Proxy request failed: {str(e)}"}}).encode('utf-8')
                    self.send_response(500)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(err_msg)))
                    self.end_headers()
                    self.wfile.write(err_msg)
                    return

        elif self.path == '/api/granola':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            
            granola_key = client_api_key(self, 'X-Granola-API-Key')
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
            return
            
        elif self.path == '/api/affinity/push':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                payload = json.loads(post_data.decode('utf-8'))
                api_key = payload.get('api_key') or self.headers.get('X-Affinity-Api-Key')
                item = payload.get('company') or payload.get('item') or payload
                
                print(f"[Affinity Push] Pushing item '{item.get('name')}' (Type: {item.get('entityType', 'Company')}, Status: {item.get('status')}) to Affinity Deals list...")
                push_res = execute_affinity_push(item, api_key=api_key)
                
                # If company ID is provided, automatically update the DB record with Affinity metadata
                comp_id = item.get('id')
                if comp_id:
                    with db_lock:
                        db_latest = read_db()
                        for c in db_latest.get('companies', []):
                            if str(c.get('id')) == str(comp_id):
                                c['affinityOrgId'] = push_res.get('affinity_org_id')
                                c['affinityListEntryId'] = push_res.get('list_entry_id')
                                c['affinityUrl'] = push_res.get('affinity_url')
                                c['affinityPushedAt'] = int(time.time() * 1000)
                                c['affinityStatus'] = push_res.get('status')
                                break
                        write_db(db_latest)

                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "affinity_org_id": push_res.get('affinity_org_id'),
                    "list_entry_id": push_res.get('list_entry_id'),
                    "status": push_res.get('status'),
                    "affinity_url": push_res.get('affinity_url')
                }).encode('utf-8'))
            except Exception as push_err:
                traceback.print_exc()
                self.send_response(500)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(push_err)
                }).encode('utf-8'))
        elif self.path == '/api/affinity/add_note':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                payload = json.loads(post_data.decode('utf-8')) if post_data else {}
                api_key = payload.get('api_key') or self.headers.get('X-Affinity-Api-Key')
                if not api_key or api_key == "configured_via_provider":
                    api_key = get_stored_value('affinity_api_key')
                if not api_key:
                    api_key = AFFINITY_DEFAULT_API_KEY
                if isinstance(api_key, str):
                    api_key = api_key.strip()
                
                auth_header = base64.b64encode(f':{api_key}'.encode()).decode()
                org_id = payload.get('org_id') or payload.get('affinityOrgId')
                comp_name = payload.get('company_name') or payload.get('name')
                url_val = payload.get('url')
                note_content = payload.get('note_text') or payload.get('content') or payload.get('text')
                note_title = payload.get('title') or "BNVT Note"
                
                if not note_content or not note_content.strip():
                    raise Exception("Note content is empty")

                # If org_id not provided directly, match or create organization
                if not org_id and comp_name:
                    item_stub = {'name': comp_name, 'url': url_val or ''}
                    push_res = execute_affinity_push(item_stub, api_key=api_key)
                    org_id = push_res.get('affinity_org_id')

                if not org_id:
                    raise Exception("Could not find or create Affinity organization for note")

                formatted_note = f"=== BNVT NOTE: {note_title.upper()} ===\n\n{note_content.strip()}"
                res_note = push_affinity_note(auth_header, org_id, formatted_note)
                
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": True,
                    "note_id": res_note.get('id') if isinstance(res_note, dict) else None,
                    "org_id": org_id
                }).encode('utf-8'))
                return
            except Exception as note_err:
                traceback.print_exc()
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(note_err)
                }).encode('utf-8'))
                return

        elif self.path == '/api/affinity/auth':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                payload = json.loads(post_data.decode('utf-8')) if post_data else {}
                api_key = payload.get('api_key') or self.headers.get('X-Affinity-Api-Key')
                if not api_key or api_key == "configured_via_provider":
                    api_key = get_stored_value('affinity_api_key')
                if isinstance(api_key, str):
                    api_key = api_key.strip()
                if not api_key:
                    raise Exception("No Affinity API Key provided")
                
                auth_b64 = base64.b64encode(f":{api_key}".encode('utf-8')).decode('utf-8')
                req = urllib.request.Request(
                    "https://api.affinity.co/auth/whoami",
                    headers={
                        "Authorization": f"Basic {auth_b64}",
                        "Content-Type": "application/json"
                    }
                )
                with urllib.request.urlopen(req) as resp:
                    who_data = json.loads(resp.read().decode('utf-8'))
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "success": True,
                        "tenant": who_data.get('tenant'),
                        "user": who_data.get('user')
                    }).encode('utf-8'))
                    return
            except Exception as auth_err:
                self.send_response(400)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({
                    "success": False,
                    "error": str(auth_err)
                }).encode('utf-8'))
                return

        elif self.path == '/api/storage':
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length) if content_length > 0 else b''
            try:
                data = json.loads(post_data.decode('utf-8'))
                key = data.get('key')
                value = data.get('value')
                if not key:
                    raise Exception("Missing key")
                if not safe_storage_key(key):
                    raise Exception("Invalid key")
                if is_masked(value):
                    # The browser sent back the masked placeholder: keep the real secret.
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True, "unchanged": True}).encode('utf-8'))
                    return
                
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
                        # 4. Save merged state atomically (local file primary, remote mirrors async)
                        if not write_db(merged_db):
                            raise Exception("Failed to write database file")
                        merged_str = json.dumps(merged_db, ensure_ascii=False)
                            
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
                        config_data = {"provider": "anthropic", "model": "claude-sonnet-5", "search_model": "claude-sonnet-5"}
                        
                    os.makedirs('db_storage', exist_ok=True)
                    path = os.path.join('db_storage', 'llm_config.json')
                    with open(path, 'w', encoding='utf-8') as f:
                        json.dump(config_data, f, indent=2, ensure_ascii=False)
                        
                    set_sql_value(key, value)
                    set_supabase_rest_value(key, value)
                    
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"success": True}).encode('utf-8'))
                    
                else:
                    # Write key directly to file (e.g. granola_api_key, groq_api_key, supabase_url, etc.)
                    set_sql_value(key, value)
                    set_supabase_rest_value(key, value)
                        
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
                api_key = client_api_key(self)
                if not api_key or api_key == "configured_via_provider":
                    api_key = get_stored_value('anthropic_api_key')
                if not api_key or api_key == "configured_via_provider":
                    api_key = os.environ.get("ANTHROPIC_API_KEY")
                if isinstance(api_key, str):
                    api_key = api_key.strip()
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
    queue_remote_mirror()  # bring Supabase up to date after any downtime (background, non-blocking)
        
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
