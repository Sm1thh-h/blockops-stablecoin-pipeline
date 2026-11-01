"""
Pull entity-level data from the Arkham API and land it, unmodified, in BigQuery.

Endpoints used (credit cost per call, from Arkham's credit pricing page):
  /balances/entity/{entity}        5 credits
  /flow/entity/{entity}            3 credits
  /volume/entity/{entity}          2 credits
  /counterparties/entity/{entity} 50 credits

Everything lands in ONE raw table (entity_endpoints_raw) with an `endpoint`
column, so dbt can filter per endpoint later. Each run appends a new snapshot
(pulled_at tells you when), which is what you want for balances that change.

Before running:
  export ARKHAM_API_KEY="your_key_here"
  gcloud auth application-default login
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

# --- Config -----------------------------------------------------------------
API_KEY = os.environ["ARKHAM_API_KEY"]
BASE_URL = "https://api.arkm.com"
HEADERS = {"API-Key": API_KEY}

PROJECT_ID = "blockops-stablecoin-africa"
DATASET = "raw_arkham"
TABLE = "entity_endpoints_raw"

# Only slugs that returned data earlier. Add tether / circle / cngn once you
# have confirmed their slugs (the /intelligence/search endpoint costs 30 credits).
ENTITIES = ["valr", "busha", "quidax", "yellow-card", "luno"]

# name -> (path template, credits per call)
ENDPOINTS = {
    "balances": ("/balances/entity/{entity}", 5),
    "flow": ("/flow/entity/{entity}", 3),
    "volume": ("/volume/entity/{entity}", 2),
    "counterparties": ("/counterparties/entity/{entity}", 50),
}

PAUSE_SECONDS = 1  # small pause between calls to stay inside rate limits
BACKUP_DIR = "arkham_raw_backup"  # local folder for raw responses (add to .gitignore)


# --- Helpers ----------------------------------------------------------------
def estimate_credits():
    per_entity = sum(credits for _, credits in ENDPOINTS.values())
    return per_entity * len(ENTITIES)


def call_endpoint(path_template, entity):
    url = BASE_URL + path_template.format(entity=entity)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    if resp.status_code == 429:  # rate limited: wait once, then retry
        print("  rate limited, waiting 10s and retrying...")
        time.sleep(10)
        resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()  # failed (4xx/5xx) requests are not charged
    return resp.json()


def ensure_table(client, table_id):
    schema = [
        bigquery.SchemaField("entity_slug", "STRING"),
        bigquery.SchemaField("endpoint", "STRING"),
        bigquery.SchemaField("raw_json", "STRING"),
        bigquery.SchemaField("pulled_at", "TIMESTAMP"),
    ]
    try:
        client.get_table(table_id)
    except Exception:
        client.create_table(bigquery.Table(table_id, schema=schema))
        print(f"Created table {table_id}")


# --- Main -------------------------------------------------------------------
def main():
    estimate = estimate_credits()
    print(f"{len(ENTITIES)} entities x {len(ENDPOINTS)} endpoints "
          f"= about {estimate} credits.")
    if input("Proceed? (y/n): ").strip().lower() != "y":
        print("Cancelled. No credits used.")
        return

    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    os.makedirs(BACKUP_DIR, exist_ok=True)
    run_stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    rows = []
    for entity in ENTITIES:
        for name, (path_template, _credits) in ENDPOINTS.items():
            try:
                data = call_endpoint(path_template, entity)
                # Local backup: if the BigQuery load fails, you keep the data
                # and don't have to spend API credits again.
                with open(f"{BACKUP_DIR}/{entity}_{name}_{run_stamp}.json", "w") as f:
                    json.dump(data, f)
                rows.append({
                    "entity_slug": entity,
                    "endpoint": name,
                    "raw_json": json.dumps(data),
                    "pulled_at": datetime.now(timezone.utc).isoformat(),
                })
                print(f"✓ {entity} / {name}")
            except Exception as e:
                print(f"✗ {entity} / {name}: {e}")
            time.sleep(PAUSE_SECONDS)

    if not rows:
        print("Nothing to load. Check the errors above.")
        return

    ensure_table(client, table_id)

    # A load job (not streaming insert) because streaming requests are capped
    # at 10 MB and the counterparties responses can be large.
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("entity_slug", "STRING"),
            bigquery.SchemaField("endpoint", "STRING"),
            bigquery.SchemaField("raw_json", "STRING"),
            bigquery.SchemaField("pulled_at", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job = client.load_table_from_json(rows, table_id, job_config=job_config)
    job.result()  # waits for the job to finish; raises an error if it fails
    print(f"Loaded {len(rows)} rows into {table_id}")


if __name__ == "__main__":
    main()
