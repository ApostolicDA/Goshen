import pandas as pd
import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client = bigquery.Client()

access_token = os.getenv("FACEBOOK_ACCESS_TOKEN")

r = requests.get(
    "https://graph.facebook.com/v25.0/me/accounts",
    params={"access_token": access_token}
)
page_data  = r.json()["data"][0]
PAGE_TOKEN = page_data["access_token"]
PAGE_ID    = page_data["id"]
# ── 1. PAGE INSIGHTS ─────────────────────────────────────────
response = requests.get(
    f"https://graph.facebook.com/v25.0/{PAGE_ID}/insights",
    params={
        "metric"       : "page_total_actions,page_impressions_unique,page_posts_impressions,page_video_views,page_follows,page_views_total,page_actions_post_reactions_total",
        "period"       : "day",
        "access_token" : PAGE_TOKEN
    }
)
data = response.json()
rows = []
for metric in data["data"]:
    for v in metric["values"]:
        rows.append({
            "METRIC"     : metric["name"],
            "DATE"       : v["end_time"][:10],
            "VALUE"      : str(v["value"]),
            "ingested_at": datetime.now(timezone.utc)
        })
df_insights = pd.DataFrame(rows)
print(df_insights)
# ── 2. POSTS (all pages) ──────────────────────────────────────
post_rows = []
url = f"https://graph.facebook.com/v25.0/{PAGE_ID}/feed"
params = {
    "fields"       : "id,message,created_time,story,attachments{type,media_type}",
    "limit"        : 100,
    "access_token" : PAGE_TOKEN
}
while True:
    response = requests.get(url, params=params)
    post_data = response.json()
    for post in post_data.get("data", []):
        attachment_type = None
        attachments = post.get("attachments", {}).get("data", [])
        if attachments:
            attachment_type = attachments[0].get("type")
        post_rows.append({
            "post_id"      : post["id"],
            "message"      : post.get("message", post.get("story", ""))[:500],
            "created_time" : post["created_time"][:10],
            "content_type" : attachment_type,
            "ingested_at"  : datetime.now(timezone.utc)
        })
    next_url = post_data.get("paging", {}).get("next")
    if not next_url:
        break
    url    = next_url
    params = {}
df_posts = pd.DataFrame(post_rows)
print(f"Total posts: {len(df_posts)}")
# ── 3. LOAD TO BIGQUERY ───────────────────────────────────────
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
for df, table in [
    (df_insights, "goshen-analytics.analytics.facebook"),
    (df_posts,    "goshen-analytics.analytics.facebook_posts"),
]:
    if df.empty:
        print(f"⚠️ Skipping {table} — no data")
        continue
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows → {table}")
