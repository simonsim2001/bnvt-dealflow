import urllib.request
import urllib.error
import json
import os

def test():
    url = "https://app.azava.com/api/v1/mcp/automation"
    
    # 1. Try with no headers
    print("Testing with no headers:")
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req) as res:
            print("Status:", res.status)
            print("Content:", res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print("HTTP Error:", e.code)
        print("Headers:", dict(e.headers))
        try:
            print("Response:", e.read().decode('utf-8'))
        except Exception:
            pass
            
    print("\n" + "="*50 + "\n")
    
    # 2. Try with our Azava secret as Bearer
    print("Testing with Bearer azava_super_secret_token_2026:")
    try:
        headers = {
            "Authorization": "Bearer azava_super_secret_token_2026"
        }
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as res:
            print("Status:", res.status)
            print("Content:", res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print("HTTP Error:", e.code)
        print("Response:", e.read().decode('utf-8'))
        
    print("\n" + "="*50 + "\n")
    
    # 3. Try with our Claude API Key
    # Let's get stored Claude API key
    claude_key = ""
    key_path = "db_storage/anthropic_api_key.json"
    if os.path.exists(key_path):
        with open(key_path, 'r') as f:
            data = json.load(f)
            claude_key = data.get("value") or data.get("key") or ""
            
    if claude_key:
        print("Testing with Bearer CLAUDE_API_KEY:")
        try:
            headers = {
                "Authorization": f"Bearer {claude_key}"
            }
            req = urllib.request.Request(url, headers=headers, method="GET")
            with urllib.request.urlopen(req) as res:
                print("Status:", res.status)
                print("Content:", res.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            print("HTTP Error:", e.code)
            print("Response:", e.read().decode('utf-8'))

if __name__ == '__main__':
    test()
