import urllib.request
import urllib.error
import json

def test():
    url = "https://app.azava.com/api/v1/mcp/automation"
    token = "az_GkIwL4xXe3qQtevPO68tEtTh1aQ92OAjzhMRVQSKtfg"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/event-stream"
    }
    
    print("Testing with Azava Token:")
    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req) as res:
            print("Status:", res.status)
            print("Response Headers:", dict(res.headers))
            content = res.readline().decode('utf-8')
            print("First line:", content)
            print("="*50)
    except urllib.error.HTTPError as e:
        print("HTTP Error:", e.code)
        try:
            print("Response:", e.read().decode('utf-8'))
        except Exception:
            pass
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    test()
