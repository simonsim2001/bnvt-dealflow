import urllib.request
import urllib.parse
import json

url = "https://app.azava.com/api/v1/mcp/automation"
token = "az_GkIwL4xXe3qQtevPO68tEtTh1aQ92OAjzhMRVQSKtfg"

headers = {
    "Authorization": f"Bearer {token}",
    "Accept": "text/event-stream"
}

print("Connecting to SSE endpoint...")
req = urllib.request.Request(url, headers=headers)
try:
    with urllib.request.urlopen(req) as res:
        endpoint = None
        event_type = None
        # We only need the first few lines to get the endpoint
        for _ in range(50):
            line = res.readline()
            if not line:
                break
            line_str = line.decode('utf-8').strip()
            if not line_str:
                continue
            print("Received line:", line_str)
            if line_str.startswith("event:"):
                event_type = line_str.split(":", 1)[1].strip()
            elif line_str.startswith("data:"):
                data_val = line_str.split(":", 1)[1].strip()
                if event_type == "endpoint":
                    # The endpoint data is URL-encoded or absolute URL
                    # Sometimes it contains ?t=... parameter
                    endpoint = data_val
                    break

        if not endpoint:
            print("Could not find message endpoint URL.")
            exit(1)
            
        print("Message endpoint URL:", endpoint)
        endpoint_url = urllib.parse.urljoin(url, endpoint)
        print("Full message endpoint URL:", endpoint_url)
        
        # Send tools/list request
        payload = {
            "jsonrpc": "2.0",
            "method": "tools/list",
            "params": {},
            "id": 1
        }
        
        post_req = urllib.request.Request(
            endpoint_url,
            data=json.dumps(payload).encode('utf-8'),
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            },
            method="POST"
        )
        
        print("Listing tools...")
        with urllib.request.urlopen(post_req) as post_res:
            print("Response status:", post_res.status)
            response_data = post_res.read().decode('utf-8')
            print("Response data:")
            try:
                parsed = json.loads(response_data)
                print(json.dumps(parsed, indent=2))
            except Exception:
                print(response_data)
except Exception as e:
    print("Error during MCP request:", e)
