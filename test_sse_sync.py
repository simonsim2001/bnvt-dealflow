import requests

def test():
    url = "https://app.azava.com/api/v1/mcp/automation"
    token = "az_GkIwL4xXe3qQtevPO68tEtTh1aQ92OAjzhMRVQSKtfg"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "text/event-stream"
    }
    print("Connecting...")
    try:
        r = requests.get(url, headers=headers, stream=True, timeout=10)
        print("Status:", r.status_code)
        print("Headers:", dict(r.headers))
        print("Reading first 3 lines of stream:")
        count = 0
        for line in r.iter_lines():
            if line:
                print("Line:", line.decode('utf-8'))
                count += 1
                if count >= 3:
                    break
    except Exception as e:
        print("Error:", e)

if __name__ == '__main__':
    test()
