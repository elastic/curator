#!/usr/bin/env python3
"""Fix incorrect date ranges for specific repositories"""

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from elasticsearch8 import Elasticsearch

# Connect to Elasticsearch (adjust if needed)
client = Elasticsearch(
    ["https://192.168.10.81:9200"],
    verify_certs=False
)

STATUS_INDEX = "deepfreeze-status"

# Repositories to fix (set start=None, end=None to clear bad dates)
repos_to_fix = {
    "deepfreeze-000093": {"start": None, "end": None},
}

for repo_name, new_dates in repos_to_fix.items():
    print(f"\nFixing {repo_name}...")

    # Find the repo document
    query = {"query": {"term": {"name.keyword": repo_name}}}
    try:
        response = client.search(index=STATUS_INDEX, body=query)

        if response["hits"]["total"]["value"] == 0:
            print(f"  Repository {repo_name} not found in status index")
            continue

        doc_id = response["hits"]["hits"][0]["_id"]
        current_doc = response["hits"]["hits"][0]["_source"]

        print(f"  Current dates: {current_doc.get('start')} to {current_doc.get('end')}")

        # Update with new dates
        update_body = {"doc": new_dates}
        client.update(index=STATUS_INDEX, id=doc_id, body=update_body)

        print(f"  Updated to: {new_dates['start']} to {new_dates['end']}")

    except Exception as e:
        print(f"  Error: {e}")

print("\nDone!")
