import urllib.request
import urllib.error
import json

def test():
    url = "https://app.azava.com/api/v1/mcp/automation"
    token = "az_9nrMUWxEgZN8pFULZTgCVnB8PBs9SeK7fHziKd8D1kQ"
    headers = {
        "Authorization": f"Bearer {token}"
    }
    
    print(f"Testing with access_token: {token}")
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as res:
            print("Status:", res.status)
            print("Response:", res.read().decode('utf-8'))
    except urllib.error.HTTPError as e:
        print("HTTP Error:", e.code)
        try:
            print("Response:", e.read().decode('utf-8'))
        except Exception:
            pass

if __name__ == '__main__':
    test()
