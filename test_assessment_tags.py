import urllib.request
import json
import os

def test():
    # Read the stored API key to send it in the request headers
    api_key = ""
    key_path = "db_storage/anthropic_api_key.json"
    if os.path.exists(key_path):
        with open(key_path, 'r') as f:
            data = json.load(f)
            api_key = data.get("value") or data.get("key") or ""
            
    body = {
        "name": "Karri Saarinen",
        "company": "Linear",
        "title": "Co-founder & CEO",
        "rawProfile": "Karri Saarinen is the designer and co-founder of Linear. Previously, he was Principal Designer at Airbnb, where he led the design of the design system, and co-founded Kippt and Spool. Spool was acquired by Facebook. Spool was a mobile bookmarking service. Spool's founders joined Facebook. Kippt was a bookmarking site for developers. Spool was seed funded."
    }
    
    headers = {
        "Content-Type": "application/json"
    }
    if api_key:
        headers["X-Anthropic-API-Key"] = api_key
        
    req = urllib.request.Request(
        "http://localhost:8000/api/anthropic_founder_assessment",
        data=json.dumps(body).encode('utf-8'),
        headers=headers,
        method="POST"
    )
    
    try:
        print("Sending request to /api/anthropic_founder_assessment...")
        with urllib.request.urlopen(req) as res:
            print("Response Status:", res.status)
            content = res.read().decode('utf-8')
            parsed = json.loads(content)
            print("\nAssessment success:", parsed.get("success"))
            profile = parsed.get("profile", {})
            print("Founder Name:", profile.get("name"))
            print("Company:", profile.get("company"))
            print("Generated Tags:", profile.get("tags"))
            print("Scores:", profile.get("scores"))
            print("Background:", profile.get("background")[:200] + "...")
    except Exception as e:
        print("Error during request:", e)
        if hasattr(e, 'read'):
            print("Response details:", e.read().decode('utf-8'))

if __name__ == '__main__':
    test()
