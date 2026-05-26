import pandas as pd
import os
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# Credentials handled by Docker environment — no action needed
client = bigquery.Client()

# ── CONFIG ────────────────────────────────────────────────────
# Reads from env var — set in .env for local, mounted volume in Docker
CSV_FOLDER = os.getenv("FACEBOOK_CSV_FOLDER", "./data/facebook")

BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "goshen-analytics")
BQ_DATASET = os.getenv("BQ_DATASET", "analytics")

FILES = {
    "Views.csv"        : "page_views",
    "Visits.csv"       : "page_visits",
    "Follows.csv"      : "page_follows_daily",
    "Viewers.csv"      : "page_viewers",
    "Interactions.csv" : "page_interactions",
    "Link clicks.csv"  : "page_link_clicks",
}

# ── READ & STACK ALL CSVs ─────────────────────────────────────
rows = []
for filename, metric_name in FILES.items():
    filepath = os.path.join(CSV_FOLDER, filename)

    if not os.path.exists(filepath):
        print(f"⚠️  Skipping {filename} — file not found at {filepath}")
        continue

    df = pd.read_csv(filepath, encoding='utf-16', skiprows=2)
    df.columns = ['date', 'value']

    df['metric_name'] = metric_name
    df['date']        = pd.to_datetime(df['date']).dt.date
    df['value']       = pd.to_numeric(df['value'], errors='coerce').fillna(0).astype(int)
    df['ingested_at'] = datetime.now(timezone.utc)

    rows.append(df)
    print(f"✅ Read {len(df)} rows from {filename}")

if not rows:
    print("⚠️  No Facebook CSV files found — skipping load")
    exit(0)

df_all = pd.concat(rows, ignore_index=True)
print(f"\nTotal rows: {len(df_all)}")
print(df_all.head(10).to_string())

# ── LOAD TO BIGQUERY ──────────────────────────────────────────
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
full_table = f"{BQ_PROJECT}.{BQ_DATASET}.facebook_csv_insights"
job = client.load_table_from_dataframe(df_all, full_table, job_config=job_config)
job.result()
print(f"\n✅ Loaded {len(df_all)} rows → {full_table}")
