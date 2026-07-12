import json
import os

db_path = 'db_storage/bnvt-dealflow-v1.json'
if os.path.exists(db_path):
    with open(db_path, 'r', encoding='utf-8') as f:
        outer = json.load(f)
        value_str = outer.get('value', '')
        if value_str:
            db = json.loads(value_str)
            print(f"Loaded database from storage.")
            print(f"Total companies: {len(db.get('companies', []))}")
            print(f"Total investors: {len(db.get('investors', []))}")
            print(f"Total founder profiles: {len(db.get('founderProfiles', []))}")
            
            # Check company tags
            companies_with_tags = [c for c in db.get('companies', []) if c.get('tags')]
            print(f"\nCompanies with tags: {len(companies_with_tags)}")
            for c in companies_with_tags[:5]:
                print(f" - {c.get('name')}: {c.get('tags')}")
                
            # Check founder profiles tags
            fps_with_tags = [fp for fp in db.get('founderProfiles', []) if fp.get('tags')]
            print(f"\nFounder profiles with tags: {len(fps_with_tags)}")
            for fp in fps_with_tags[:5]:
                print(f" - {fp.get('name')} ({fp.get('company')}): {fp.get('tags')}")
        else:
            print("Database has no value string.")
else:
    print(f"Database path {db_path} does not exist.")
