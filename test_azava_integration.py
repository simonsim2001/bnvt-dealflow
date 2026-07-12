import urllib.request
import json
import sys

def run_test():
    secret = "azava_super_secret_token_2026"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {secret}"
    }
    
    # 1. Test GET /pipeline/summary (Text)
    try:
        print("Testing GET /pipeline/summary (Text)...")
        req = urllib.request.Request("http://localhost:8000/pipeline/summary", headers={"Authorization": f"Bearer {secret}"}, method="GET")
        with urllib.request.urlopen(req) as res:
            text = res.read().decode('utf-8')
            print("Status:", res.status)
            print("Summary Text:\n", text)
            print("-" * 50)
    except Exception as e:
        print("GET /pipeline/summary (Text) failed:", e)
        
    # 2. Test GET /pipeline/summary (JSON)
    try:
        print("Testing GET /pipeline/summary (JSON)...")
        req = urllib.request.Request("http://localhost:8000/pipeline/summary?format=json", headers={"Authorization": f"Bearer {secret}"}, method="GET")
        with urllib.request.urlopen(req) as res:
            data = json.loads(res.read().decode('utf-8'))
            print("Status:", res.status)
            print("JSON Result:", data)
            print("-" * 50)
    except Exception as e:
        print("GET /pipeline/summary (JSON) failed:", e)

    # 3. Test Bad Auth
    try:
        print("Testing Bad Auth on GET /pipeline/summary...")
        req = urllib.request.Request("http://localhost:8000/pipeline/summary", headers={"Authorization": "Bearer bad_secret"}, method="GET")
        with urllib.request.urlopen(req) as res:
            print("Unexpected success:", res.status)
    except urllib.error.HTTPError as e:
        print("Expected auth failure status:", e.code)
        print("-" * 50)
    except Exception as e:
        print("Auth check failed with unexpected error:", e)

    # 4. Test POST /api/azava - manifest
    try:
        print("Testing POST /api/azava (manifest)...")
        body = {"method": "manifest", "params": {}}
        req = urllib.request.Request(
            "http://localhost:8000/api/azava",
            data=json.dumps(body).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as res:
            data = json.loads(res.read().decode('utf-8'))
            print("Status:", res.status)
            print("Manifest output ok:", data.get("ok"))
            print("Manifest methods:", data.get("result", {}).get("methods"))
            print("-" * 50)
    except Exception as e:
        print("POST /api/azava (manifest) failed:", e)

    # 5. Test POST /api/azava - describe
    try:
        print("Testing POST /api/azava (describe)...")
        body = {"method": "describe", "params": {"typeId": "Deal"}}
        req = urllib.request.Request(
            "http://localhost:8000/api/azava",
            data=json.dumps(body).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as res:
            data = json.loads(res.read().decode('utf-8'))
            print("Status:", res.status)
            print("Describe output ok:", data.get("ok"))
            fields = data.get("result", {}).get("fields", [])
            print("Fields count:", len(fields))
            print("First field:", fields[0] if fields else None)
            print("-" * 50)
    except Exception as e:
        print("POST /api/azava (describe) failed:", e)

    # 6. Test POST /api/azava - createRecord
    try:
        print("Testing POST /api/azava (createRecord)...")
        body = {
            "method": "createRecord",
            "params": {
                "typeId": "Deal",
                "fields": {
                    "name": "Acme Ventures Inc",
                    "stage": "Seed",
                    "source": "WhatsApp",
                    "notes": "Met the founder at a startup event. Interested in seed round.",
                    "contactName": "John Acme",
                    "contactPhone": "+1555001122",
                    "amount": "$1.2M",
                    "sector": "SaaS"
                }
            }
        }
        req = urllib.request.Request(
            "http://localhost:8000/api/azava",
            data=json.dumps(body).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req) as res:
            data = json.loads(res.read().decode('utf-8'))
            print("Status:", res.status)
            print("CreateRecord output ok:", data.get("ok"))
            record_id = data.get("result", {}).get("id")
            print("Created record ID:", record_id)
            print("-" * 50)
            
            # Explicit verification against BNVT app's storage API
            print("Verifying deal is visible via BNVT storage API (/api/storage)...")
            req_storage = urllib.request.Request("http://localhost:8000/api/storage?key=bnvt-dealflow-v1", method="GET")
            with urllib.request.urlopen(req_storage) as res_storage:
                storage_data = json.loads(res_storage.read().decode('utf-8'))
                db_value = json.loads(storage_data.get("value", "{}"))
                companies = db_value.get("companies", [])
                matched_company = None
                for c in companies:
                    if c.get("id") == record_id:
                        matched_company = c
                        break
                if matched_company:
                    print("SUCCESS: Deal found in pipeline database via storage API!")
                    print("  Name:", matched_company.get("name"))
                    print("  Stage:", matched_company.get("stage"))
                    print("  Sector:", matched_company.get("sector"))
                    print("  Contact Phone:", matched_company.get("contactPhone"))
                else:
                    print("ERROR: Created deal ID not found in pipeline database!")
                    sys.exit(1)
            print("-" * 50)
            
            # Now resolve it
            print("Testing POST /api/azava (resolveEntity)...")
            body_resolve = {
                "method": "resolveEntity",
                "params": {
                    "typeId": "Deal",
                    "fields": {
                        "name": "Acme Ventures",
                        "contactPhone": "+1555001122"
                    }
                }
            }
            req_resolve = urllib.request.Request(
                "http://localhost:8000/api/azava",
                data=json.dumps(body_resolve).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            with urllib.request.urlopen(req_resolve) as res_resolve:
                data_resolve = json.loads(res_resolve.read().decode('utf-8'))
                print("Resolve output ok:", data_resolve.get("ok"))
                print("Resolved entries:", data_resolve.get("result"))
                print("-" * 50)
                
            # Now update it
            print("Testing POST /api/azava (updateRecord)...")
            body_update = {
                "method": "updateRecord",
                "params": {
                    "typeId": "Deal",
                    "id": record_id,
                    "fields": {
                        "stage": "Series A",
                        "amount": "$2.5M"
                    }
                }
            }
            req_update = urllib.request.Request(
                "http://localhost:8000/api/azava",
                data=json.dumps(body_update).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            with urllib.request.urlopen(req_update) as res_update:
                data_update = json.loads(res_update.read().decode('utf-8'))
                print("Update output ok:", data_update.get("ok"))
                print("Updated record ID:", data_update.get("result", {}).get("id"))
                print("-" * 50)

            # Get field value
            print("Testing POST /api/azava (getFieldValue)...")
            body_get = {
                "method": "getFieldValue",
                "params": {
                    "typeId": "Deal",
                    "id": record_id,
                    "fieldId": "stage"
                }
            }
            req_get = urllib.request.Request(
                "http://localhost:8000/api/azava",
                data=json.dumps(body_get).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            with urllib.request.urlopen(req_get) as res_get:
                data_get = json.loads(res_get.read().decode('utf-8'))
                print("Get field value ok:", data_get.get("ok"))
                print("Stage field value:", data_get.get("result"))
                print("-" * 50)

            # Delete it
            print("Testing POST /api/azava (deleteRecord)...")
            body_delete = {
                "method": "deleteRecord",
                "params": {
                    "typeId": "Deal",
                    "id": record_id
                }
            }
            req_delete = urllib.request.Request(
                "http://localhost:8000/api/azava",
                data=json.dumps(body_delete).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            with urllib.request.urlopen(req_delete) as res_delete:
                data_delete = json.loads(res_delete.read().decode('utf-8'))
                print("Delete output ok:", data_delete.get("ok"))
                print("Delete result:", data_delete.get("result"))
                print("-" * 50)
                
            # Verify updateRecord returns notFound when trying to update deleted ID
            print("Testing POST /api/azava (updateRecord notFound)...")
            body_update_missing = {
                "method": "updateRecord",
                "params": {
                    "typeId": "Deal",
                    "id": record_id,
                    "fields": {
                        "stage": "Series A"
                    }
                }
            }
            req_update_missing = urllib.request.Request(
                "http://localhost:8000/api/azava",
                data=json.dumps(body_update_missing).encode('utf-8'),
                headers=headers,
                method="POST"
            )
            with urllib.request.urlopen(req_update_missing) as res_update_missing:
                data_update_missing = json.loads(res_update_missing.read().decode('utf-8'))
                print("Update missing ok:", data_update_missing.get("ok"))
                print("Update missing result (should contain notFound: True):", data_update_missing.get("result"))
                print("-" * 50)

    except Exception as e:
        print("Deal CRUD flow failed:", e)

if __name__ == '__main__':
    run_test()
