import pandas as pd
import os
from datetime import datetime, timezone
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\gadis\Downloads\goshen-analytics-1a4583133e1e.json"
client = bigquery.Client()

# ── CONFIG ────────────────────────────────────────────────────
CSV_FOLDER = r"C:\Users\gadis\Downloads\Facebook data"

FILES = {
    "Views.csv"        : "page_views",
    "Visits.csv"       : "page_visits",
    "Follows.csv"      : "page_follows_daily",
    "Viewers.csv"      : "page_viewers",
    "Interactions.csv" : "page_interactions",
    "Link clicks.csv"  : "page_link_clicks",   # ← space, not underscore
}

# ── READ & STACK ALL CSVs ─────────────────────────────────────
rows = []
for filename, metric_name in FILES.items():
    filepath = os.path.join(CSV_FOLDER, filename)
    
    df = pd.read_csv(filepath, encoding='utf-16', skiprows=2)
    df.columns = ['date', 'value']
    
    df['metric_name'] = metric_name
    df['date']        = pd.to_datetime(df['date']).dt.date
    df['value']       = pd.to_numeric(df['value'], errors='coerce').fillna(0).astype(int)
    df['ingested_at'] = datetime.now(timezone.utc)
    
    rows.append(df)
    print(f"✅ Read {len(df)} rows from {filename}")

# stack all into one DataFrame
df_all = pd.concat(rows, ignore_index=True)
print(f"\nTotal rows: {len(df_all)}")
print(df_all.head(10).to_string())

# ── LOAD TO BIGQUERY ──────────────────────────────────────────
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
job = client.load_table_from_dataframe(
    df_all,
    "goshen-analytics.analytics.facebook_csv_insights",
    job_config=job_config
)
job.result()
print(f"\n✅ Loaded {len(df_all)} rows → facebook_csv_insights")