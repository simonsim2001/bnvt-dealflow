#!/usr/bin/env python3
import json
import sys
from server import merge_databases, merge_single_item, merge_notes, merge_sub_list

def test_memory_reload():
    print("--- Running Memory & Reload Verification Tests ---")

    # 1. Test string notes merging (prevents AttributeError)
    c_notes = ["Met founder on Tuesday", {"id": "n1", "text": "Call scheduled"}]
    s_notes = ["Deck received via email", {"id": "n1", "text": "Call scheduled for 3pm"}]
    merged_notes = merge_notes(c_notes, s_notes)
    assert len(merged_notes) == 3, f"Expected 3 merged notes, got {len(merged_notes)}"
    print("✓ Test 1 Passed: Safe string & dict notes merging works without crash")

    # 2. Test preserving server-side ingested fields when client reloads with stale snapshot
    server_item = {
        "id": "comp_123",
        "name": "Acme AI",
        "stage": "Seed",
        "deckUrl": "https://docsend.com/view/test1234",
        "analysis": "Extracted pitch deck summary: Acme AI builds foundation models.",
        "documents": [{"id": "doc_1", "name": "Pitch Deck.pdf", "url": "https://..."}],
        "notes": [{"id": "n_wa", "text": "Ingested via WhatsApp DocSend link"}],
        "updatedAt": 1700000100
    }
    client_item_stale = {
        "id": "comp_123",
        "name": "Acme AI",
        "stage": "Seed",
        "deckUrl": "",
        "analysis": "",
        "documents": [],
        "notes": [],
        "updatedAt": 1700000000
    }
    merged_comp = merge_single_item(client_item_stale, server_item)
    assert merged_comp["analysis"] == server_item["analysis"], "Server analysis was lost!"
    assert merged_comp["deckUrl"] == server_item["deckUrl"], "Server deckUrl was lost!"
    assert len(merged_comp["documents"]) == 1, "Server documents were lost!"
    assert len(merged_comp["notes"]) == 1, "Server notes were lost!"
    print("✓ Test 2 Passed: Ingested server fields (deck, analysis, docs) preserved on reload")

    # 3. Test client stage update preserved alongside server ingested notes
    client_updated_stage = {
        "id": "comp_123",
        "name": "Acme AI",
        "stage": "Series A", # user changed stage to Series A
        "updatedAt": 1700000200
    }
    merged_comp_2 = merge_single_item(client_updated_stage, server_item)
    assert merged_comp_2["stage"] == "Series A", "Client stage update was lost!"
    assert merged_comp_2["analysis"] == server_item["analysis"], "Server analysis was lost during client update!"
    print("✓ Test 3 Passed: Client stage update and server analysis co-exist perfectly")

    # 4. Test database-level merge with deletedIds
    client_db = {
        "companies": [
            {"id": "comp_123", "name": "Acme AI", "stage": "Series A"},
            {"id": "comp_to_delete", "name": "Killed Corp"}
        ],
        "investors": [],
        "deepDives": [],
        "founderProfiles": [],
        "deletedIds": ["comp_to_delete"]
    }
    server_db = {
        "companies": [
            {"id": "comp_123", "name": "Acme AI", "analysis": "Strong team"},
            {"id": "comp_to_delete", "name": "Killed Corp"},
            {"id": "comp_new", "name": "New WhatsApp Lead"}
        ],
        "investors": [],
        "deepDives": [],
        "founderProfiles": [],
        "deletedIds": []
    }
    merged_db = merge_databases(client_db, server_db)
    comp_ids = [c["id"] for c in merged_db["companies"]]
    assert "comp_to_delete" not in comp_ids, "Deleted company resurrected on reload!"
    assert "comp_123" in comp_ids, "Existing company missing!"
    assert "comp_new" in comp_ids, "New server lead missing!"
    print("✓ Test 4 Passed: Full DB merge respects deletedIds and prepends new server leads")

    print("\n🎉 ALL MEMORY & RELOAD TESTS PASSED PERFECTLY!")

if __name__ == "__main__":
    test_memory_reload()
