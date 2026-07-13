#!/usr/bin/env python3
import sys
import os
import time
import requests
from io import BytesIO
from PIL import Image

def extract_pdf_text(pdf_path):
    try:
        import pypdf
        reader = pypdf.PdfReader(pdf_path)
        text = ""
        for idx, page in enumerate(reader.pages):
            text += f"\n--- Slide {idx+1} ---\n"
            text += page.extract_text() or ""
        return text
    except Exception as e:
        print(f"Error extracting text from PDF: {e}")
        return ""

def extract_text_from_svg_url(url):
    try:
        if url.startswith('//'):
            url = 'https:' + url
        elif url.startswith('/'):
            url = 'https://docsend.com' + url
            
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        res = requests.get(url, headers=headers, timeout=5)
        if res.status_code == 200:
            svg_data = res.content
            import xml.etree.ElementTree as ET
            root = ET.fromstring(svg_data)
            texts = []
            for elem in root.iter():
                tag = elem.tag.split('}')[-1] if '}' in elem.tag else elem.tag
                if tag in ('text', 'tspan') and elem.text:
                    texts.append(elem.text.strip())
            return " ".join(filter(None, texts))
    except Exception:
        pass
    return ""

def main():
    if len(sys.argv) < 3:
        print("Usage: docsend_scraper.py <url> <email> [output_pdf] [password]")
        sys.exit(1)

    url = sys.argv[1]
    email = sys.argv[2]
    
    if len(sys.argv) >= 4:
        output_pdf = sys.argv[3]
    else:
        # Generate default name
        os.makedirs('uploads', exist_ok=True)
        import uuid
        output_pdf = os.path.join('uploads', f"docsend_{uuid.uuid4().hex[:8]}.pdf")

    password = sys.argv[4] if len(sys.argv) >= 5 else ""

    # Check if direct PDF link (either via extension, Content-Type header, or %PDF magic bytes)
    is_pdf = False
    if url.lower().endswith('.pdf') or '.pdf?' in url.lower():
        is_pdf = True
    else:
        try:
            res_head = requests.head(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}, timeout=5, allow_redirects=True)
            if 'application/pdf' in res_head.headers.get('Content-Type', '').lower():
                is_pdf = True
        except Exception:
            try:
                res_get = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}, timeout=5, stream=True)
                peek = res_get.raw.read(4)
                if peek == b'%PDF':
                    is_pdf = True
            except Exception:
                pass

    if is_pdf:
        print(f"[Scraper] Direct PDF URL detected: {url}. Downloading...")
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            }
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                with open(output_pdf, 'wb') as f:
                    f.write(res.content)
                text = extract_pdf_text(output_pdf)
                import json
                print(json.dumps({
                    "success": True,
                    "url": f"/{output_pdf}",
                    "name": os.path.basename(output_pdf),
                    "text": text
                }))
                sys.exit(0)
            else:
                raise Exception(f"HTTP {res.status_code}")
        except Exception as e:
            print(f"[Scraper] Direct PDF download failed: {e}. Falling back to browser scraper.")

    # Install Playwright dynamically if not available
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("[Scraper] Playwright not found. Installing package...")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "playwright"])
        subprocess.check_call([sys.executable, "-m", "playwright", "install", "chromium"])
        from playwright.sync_api import sync_playwright

    print(f"[Scraper] Scraping URL: {url}")
    print(f"[Scraper] Using email: {email}")
    if password:
        print(f"[Scraper] Using password: [PROVIDED]")

    success = False
    try:
        with sync_playwright() as p:
            # Launch browser
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = context.new_page()
            
            # Go to URL
            page.goto(url)
            page.wait_for_load_state("networkidle")
            time.sleep(2)

            # Check for email and password gates
            email_selectors = [
                "input[type='email']", 
                "input[name*='email']", 
                "#link_auth_email", 
                ".link-auth-email",
                "input[id*='email']"
            ]
            password_selectors = [
                "input[type='password']",
                "input[name*='password']",
                "input[name*='pass']",
                "#link_auth_password",
                ".link-auth-password",
                "input[id*='pass']"
            ]
            submit_selectors = [
                "input[type='submit']",
                "button[type='submit']",
                "input[value*='View']",
                "button:has-text('View')",
                "button:has-text('Continue')",
                "input[value*='Continue']",
                "button:has-text('Submit')",
                "input[type='button'][value*='Continue']"
            ]
            
            email_field = None
            for sel in email_selectors:
                el = page.locator(sel)
                if el.count() > 0:
                    email_field = el.first
                    break

            if email_field:
                print(f"[Scraper] Email gate page detected. Filling email: {email}")
                email_field.fill(email)
                time.sleep(0.5)

            password_field = None
            for sel in password_selectors:
                el = page.locator(sel)
                if el.count() > 0:
                    password_field = el.first
                    break

            if password_field and password:
                print("[Scraper] Password field detected. Filling password...")
                password_field.fill(password)
                time.sleep(0.5)

            # Submit first screen form
            if email_field or password_field:
                submit_btn = None
                for sel in submit_selectors:
                    el = page.locator(sel)
                    if el.count() > 0:
                        submit_btn = el.first
                        break
                if submit_btn:
                    submit_btn.click()
                else:
                    if email_field:
                        email_field.press("Enter")
                    elif password_field:
                        password_field.press("Enter")
                
                print("[Scraper] Submitted initial credentials form, waiting...")
                time.sleep(4)
                page.wait_for_load_state("networkidle")

            # Check if password gate appears on a second screen (multi-step gate)
            password_field = None
            for sel in password_selectors:
                el = page.locator(sel)
                if el.count() > 0:
                    password_field = el.first
                    break

            if password_field and password:
                print("[Scraper] Password gate detected on second screen. Filling password...")
                password_field.fill(password)
                time.sleep(0.5)
                
                submit_btn = None
                for sel in submit_selectors:
                    el = page.locator(sel)
                    if el.count() > 0:
                        submit_btn = el.first
                        break
                if submit_btn:
                    submit_btn.click()
                else:
                    password_field.press("Enter")
                
                print("[Scraper] Submitted password, waiting...")
                time.sleep(4)
                page.wait_for_load_state("networkidle")
            
            # Wait for slides or page viewer
            viewer_selectors = [
                ".page-container",
                ".viewer-page",
                ".web-viewer",
                ".viewer-wrapper",
                "[class*='page-container']",
                ".slide-container",
                "img",
                "svg"
            ]
            
            loaded_selector = None
            for sel in viewer_selectors:
                try:
                    page.wait_for_selector(sel, timeout=3000)
                    loaded_selector = sel
                    print(f"[Scraper] Viewer element detected: {sel}")
                    break
                except Exception:
                    continue

            # Fallback scroll to trigger lazy loads
            page.evaluate("window.scrollTo(0, document.body.scrollHeight / 2)")
            time.sleep(0.5)
            page.evaluate("window.scrollTo(0, 0)")
            time.sleep(0.5)

            # Locate slide elements
            slide_selectors = [
                "div.page-container",
                "div.viewer-page-wrapper",
                "div.viewer-page",
                "[class*='page-container']",
                ".slide",
                ".viewer-wrapper > div"
            ]
            
            slides = []
            for sel in slide_selectors:
                elements = page.query_selector_all(sel)
                valid_elements = []
                for el in elements:
                    box = el.bounding_box()
                    if box and box['width'] > 200 and box['height'] > 200:
                        valid_elements.append(el)
                
                if len(valid_elements) > 0:
                    slides = valid_elements
                    print(f"[Scraper] Found {len(slides)} slides using selector: {sel}")
                    break
            
            if not slides:
                # If no container slide elements are found, try fallback to all images in the viewer
                images = page.query_selector_all("img")
                valid_images = []
                for img in images:
                    src = img.get_attribute("src")
                    if src and ("amazonaws.com" in src or "rendered-pdfs" in src or "docsend" in src):
                        valid_images.append(img)
                if valid_images:
                    slides = valid_images
                    print(f"[Scraper] Fallback: Found {len(slides)} slide image elements")

            pil_images = []
            slide_texts = []

            if not slides:
                # Scroll-based viewport screenshot aggregation for other page types!
                print("[Scraper] No slide elements detected. Scrolling page by page...")
                viewport_height = page.viewport_size['height']
                scroll_height = page.evaluate("document.body.scrollHeight")
                num_pages = min(20, max(1, int(scroll_height / viewport_height) + 1))
                print(f"[Scraper] Scroll height: {scroll_height}px, viewport: {viewport_height}px. Generating {num_pages} slices...")
                
                for i in range(num_pages):
                    page.evaluate(f"window.scrollTo(0, {i * viewport_height})")
                    time.sleep(0.8)
                    img_bytes = page.screenshot()
                    img = Image.open(BytesIO(img_bytes)).convert("RGB")
                    pil_images.append(img)
                
                full_text = page.evaluate("() => document.body.innerText") or ""
                slide_texts = [f"\n--- Webpage Content ---\n{full_text}"]
                if pil_images:
                    pil_images[0].save(output_pdf, save_all=True, append_images=pil_images[1:])
                    success = True
            else:
                for idx, slide in enumerate(slides):
                    print(f"[Scraper] Screenshotting & extracting slide {idx+1}/{len(slides)}...")
                    try:
                        slide.scroll_into_view_if_needed()
                        time.sleep(0.5)
                    except Exception:
                        pass
                    
                    dom_text = ""
                    try:
                        dom_text = page.evaluate("(el) => el.innerText", slide) or ""
                    except Exception as e:
                        print(f"Error getting innerText: {e}")

                    svg_text = ""
                    try:
                        srcs = []
                        tag_name = page.evaluate("(el) => el.tagName", slide).lower()
                        if tag_name == 'img':
                            srcs.append(page.evaluate("(el) => el.src", slide))
                        
                        img_elements = slide.query_selector_all("img")
                        for img_el in img_elements:
                            src = page.evaluate("(el) => el.src", img_el)
                            if src:
                                srcs.append(src)
                        
                        for src in srcs:
                            if ".svg" in src.lower() or "format=svg" in src.lower() or "s3" in src.lower():
                                txt = extract_text_from_svg_url(src)
                                if txt:
                                    svg_text += " " + txt
                    except Exception as e:
                        print(f"Error getting SVG text: {e}")

                    combined_text = (dom_text.strip() + " " + svg_text.strip()).strip()
                    if combined_text:
                        slide_texts.append(f"\n--- Slide {idx+1} ---\n{combined_text}")
                    else:
                        slide_texts.append(f"\n--- Slide {idx+1} ---\n[No text detected]")
                    
                    try:
                        img_bytes = slide.screenshot()
                        img = Image.open(BytesIO(img_bytes)).convert("RGB")
                        pil_images.append(img)
                    except Exception as e:
                        print(f"Failed to screenshot slide {idx+1}: {e}")
                
                if pil_images:
                    pil_images[0].save(output_pdf, save_all=True, append_images=pil_images[1:])
                    success = True
            
            browser.close()
    except Exception as e:
        print(f"[Scraper] Scraper execution failed: {e}")
        import traceback
        traceback.print_exc()

    if success and os.path.exists(output_pdf):
        extracted_text = "\n".join(slide_texts)
        import json
        print(json.dumps({
            "success": True,
            "url": f"/{output_pdf}",
            "name": os.path.basename(output_pdf),
            "text": extracted_text
        }))
        sys.exit(0)
    else:
        import json
        print(json.dumps({
            "success": False,
            "error": "Failed to extract slides or render PDF"
        }))
        sys.exit(1)

if __name__ == '__main__':
    main()
