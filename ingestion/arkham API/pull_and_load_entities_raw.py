import os
import json
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

# --- Config ---
API_KEY = os.environ["ARKHAM_API_KEY"]
BASE_URL = "https://api.arkm.com"
HEADERS = {"API-Key": API_KEY}

PROJECT_ID = "blockops-stablecoin-africa"      # <-- change this
DATASET = "raw_arkham"
TABLE = "entities_raw"

ENTITIES = ["valr", "busha", "quidax", "yellow-card", "paycrest", "luno"]

def fetch_entity(slug):
    url = f"{BASE_URL}/intelligence/entity/{slug}"
    resp = requests.get(url, headers=HEADERS, timeout=15)
    resp.raise_for_status()
    return resp.json()

def main():
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    rows_to_insert = []
    for slug in ENTITIES:
        try:
            data = fetch_entity(slug)
            rows_to_insert.append({
                "entity_slug": slug,
                "raw_json": json.dumps(data),
                "pulled_at": datetime.now(timezone.utc).isoformat(),
            })
            print(f"✓ fetched {slug}")
        except Exception as e:
            print(f"✗ failed on {slug}: {e}")

    if not rows_to_insert:
        print("Nothing to load — check errors above.")
        return

    schema = [
        bigquery.SchemaField("entity_slug", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("pulled_at", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"Created table {table_id}")

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print("Errors while inserting:", errors)
    else:
        print(f"Loaded {len(rows_to_insert)} rows into {table_id}")

if __name__ == "__main__":
    main()