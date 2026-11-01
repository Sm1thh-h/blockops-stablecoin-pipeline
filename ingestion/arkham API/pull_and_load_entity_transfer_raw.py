"""
Pull the Arkham transfer histogram for chains missed in the first pull
(avalanche, polygon) and load it into BigQuery.

Same window, entities, tokens and flows as the original histogram pull, so the
rows slot straight into raw_arkham.entity_transfers_raw (endpoint = 'histogram').

Safety features:
  * writes every API response to a backup file immediately (a crash cannot lose data)
  * refuses to run if histogram rows for these chains are already in BigQuery
  * prints the credit estimate and asks for confirmation
  * loads from the backup file at the end (same pattern as load_arkham_backup.py)

Usage:
    export ARKHAM_API_KEY="..."
    python pull_arkham_histogram_extra_chains.py            # asks before spending credits
    python pull_arkham_histogram_extra_chains.py --dry-run  # lists calls, spends nothing
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import requests
from google.cloud import bigquery

# ------------------------------------------------------------------ config
BASE_URL = "https://api.arkm.com"
ENDPOINT = "/transfers/histogram"
CREDITS_PER_CALL = 4

# !! Match this to the auth header used in pull_arkham_stablecoin_activity.py !!
AUTH_HEADER_NAME = "API-Key"

PROJECT = "blockops-stablecoin-africa"
TABLE = f"{PROJECT}.raw_arkham.entity_transfers_raw"
BACKUP_DIR = "arkham_raw_backup"

ENTITIES = ["valr", "luno", "quidax", "busha", "yellow-card"]
CHAINS = ["avalanche", "polygon"]          # ONLY the chains not already pulled
TOKENS = {"tether": "USDT", "usd-coin": "USDC"}
FLOWS = ["in", "out", "self"]

TIME_GTE = "2026-09-19"
TIME_LTE = "2026-10-18T23:59:59Z"
GRANULARITY = "1d"

SLEEP_SECONDS = 1.3
MAX_RETRIES = 3
# ------------------------------------------------------------------------


def build_calls():
    calls = []
    for entity in ENTITIES:
        for chain in CHAINS:
            for token_id, symbol in TOKENS.items():
                for flow in FLOWS:
                    calls.append((entity, chain, token_id, symbol, flow))
    return calls


def already_loaded(client):
    """Return the (entity, chain) pairs that already have histogram rows."""
    sql = f"""
        select distinct entity_slug, chain
        from `{TABLE}`
        where endpoint = 'histogram' and chain in UNNEST(@chains)
    """
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ArrayQueryParameter("chains", "STRING", CHAINS)]
        ),
    )
    return [(r.entity_slug, r.chain) for r in job.result()]


def fetch(session, params):
    """GET with retry on 429 / 5xx. Returns parsed JSON (list or None)."""
    for attempt in range(1, MAX_RETRIES + 1):
        resp = session.get(BASE_URL + ENDPOINT, params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code in (429, 500, 502, 503, 504):
            wait = 5 * attempt
            print(f"    HTTP {resp.status_code}, retrying in {wait}s (attempt {attempt})")
            time.sleep(wait)
            continue
        # 4xx other than 429: not charged, but do not hide it
        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:300]}")
    raise RuntimeError("Gave up after retries")


def main():
    dry_run = "--dry-run" in sys.argv
    calls = build_calls()
    credits = len(calls) * CREDITS_PER_CALL
    print(f"{len(calls)} calls, about {credits} credits "
          f"({len(ENTITIES)} entities x {len(CHAINS)} chains x {len(TOKENS)} tokens x {len(FLOWS)} flows)")

    if dry_run:
        for c in calls:
            print("  ", c)
        return

    api_key = os.environ.get("ARKHAM_API_KEY")
    if not api_key:
        sys.exit("ARKHAM_API_KEY is not set")

    client = bigquery.Client(project=PROJECT)
    existing = already_loaded(client)
    if existing:
        sys.exit(f"Histogram rows already loaded for {existing}. Aborting to avoid duplicates.")

    if input(f"Spend about {credits} credits? Type yes: ").strip().lower() != "yes":
        sys.exit("Cancelled")

    os.makedirs(BACKUP_DIR, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    backup_path = os.path.join(BACKUP_DIR, f"entity_transfers_extra_chains_{stamp}.ndjson")

    session = requests.Session()
    session.headers.update({AUTH_HEADER_NAME: api_key})

    n_ok = n_nonempty = 0
    failures = []
    with open(backup_path, "a") as backup:
        for i, (entity, chain, token_id, symbol, flow) in enumerate(calls, 1):
            params = {
                "base": entity,
                "chains": chain,
                "tokens": token_id,
                "flow": flow,
                "timeGte": TIME_GTE,
                "timeLte": TIME_LTE,
                "granularity": GRANULARITY,
            }
            try:
                data = fetch(session, params)
            except Exception as exc:  # keep going; report at the end
                print(f"[{i}/{len(calls)}] FAILED {entity} {chain} {symbol} {flow}: {exc}")
                failures.append((entity, chain, symbol, flow))
                time.sleep(SLEEP_SECONDS)
                continue

            record = {
                "entity_slug": entity,
                "endpoint": "histogram",
                "chain": chain,
                "token_symbol": symbol,
                "flow": flow,
                "request_params": json.dumps(params),
                # Arkham sometimes returns null instead of []; store [] either way
                "raw_json": json.dumps(data if data is not None else []),
                "pulled_at": datetime.now(timezone.utc).isoformat(),
            }
            backup.write(json.dumps(record) + "\n")
            backup.flush()
            os.fsync(backup.fileno())

            n_ok += 1
            if data:
                n_nonempty += 1
            print(f"[{i}/{len(calls)}] {entity:<12} {chain:<10} {symbol} {flow:<5} "
                  f"-> {len(data) if data else 0} days")
            time.sleep(SLEEP_SECONDS)

    print(f"\nPulled {n_ok} responses ({n_nonempty} with data). Backup: {backup_path}")
    if failures:
        print(f"{len(failures)} calls failed and are NOT in the backup:")
        for f in failures:
            print("  ", f)
        print("Fix and re-run only those before loading, or load what you have and pull the rest later.")
        if input("Load the successful rows now anyway? Type yes: ").strip().lower() != "yes":
            print(f"Not loaded. Load later with: bq load --source_format=NEWLINE_DELIMITED_JSON "
                  f"raw_arkham.entity_transfers_raw {backup_path}")
            return

    job = client.load_table_from_file(
        open(backup_path, "rb"),
        TABLE,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        ),
    )
    job.result()
    print(f"Loaded {job.output_rows} rows into {TABLE}")


if __name__ == "__main__":
    main()