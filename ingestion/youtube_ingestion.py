import pandas as pd
import os
import requests
from datetime import datetime, timezone
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\gadis\Downloads\goshen-analytics-1a4583133e1e.json"

client = bigquery.Client()

YOUTUBE_API_KEY = "AIzaSyCvV5MKAqq3ED10wJpjTRh8Qx_t8KjD4ts"
CHANNEL_ID      = "UCgntCquF4w2Gx5ZzzOj1KOw"

BASE_URL = "https://www.googleapis.com/youtube/v3"

# ── Channel Stats ─────────────────────────────────────────────
r = requests.get(f"{BASE_URL}/channels", params={"part": "snippet,statistics", "id": CHANNEL_ID, "key": YOUTUBE_API_KEY})
item    = r.json()["items"][0]
snippet = item["snippet"]
stats   = item["statistics"]

df_channel = pd.DataFrame([{
    "channel_id"       : CHANNEL_ID,
    "channel_title"    : snippet["title"],
    "subscriber_count" : stats["subscriberCount"],
    "view_count"       : stats["viewCount"],
    "video_count"      : stats["videoCount"],
    "ingested_at"      : datetime.now(timezone.utc),
}])
print(df_channel)

# ── Video IDs ─────────────────────────────────────────────────
video_ids = []
next_page = None

while True:
    params = {"part": "id", "channelId": CHANNEL_ID, "type": "video", "maxResults": 50, "key": YOUTUBE_API_KEY}
    if next_page:
        params["pageToken"] = next_page
    r         = requests.get(f"{BASE_URL}/search", params=params)
    data      = r.json()
    video_ids += [item["id"]["videoId"] for item in data.get("items", [])]
    next_page  = data.get("nextPageToken")
    if not next_page:
        break

print(f"Found {len(video_ids)} videos")

# ── Video Performance ─────────────────────────────────────────
rows = []
for i in range(0, len(video_ids), 50):
    chunk = video_ids[i:i+50]
    r = requests.get(f"{BASE_URL}/videos", params={"part": "snippet,statistics", "id": ",".join(chunk), "key": YOUTUBE_API_KEY})
    for item in r.json().get("items", []):
        s = item["snippet"]
        v = item.get("statistics", {})
        rows.append({
            "video_id"      : item["id"],
            "title"         : s.get("title"),
            "published_at"  : s.get("publishedAt")[:10],
            "view_count"    : v.get("viewCount", 0),
            "like_count"    : v.get("likeCount", 0),
            "comment_count" : v.get("commentCount", 0),
            "ingested_at"   : datetime.now(timezone.utc),
        })

df_videos = pd.DataFrame(rows)
print(df_videos.head())

# ── Comments ──────────────────────────────────────────────────
rows = []
for video_id in video_ids:
    try:
        r = requests.get(f"{BASE_URL}/commentThreads", params={"part": "snippet", "videoId": video_id, "maxResults": 100, "key": YOUTUBE_API_KEY})
        for item in r.json().get("items", []):
            c = item["snippet"]["topLevelComment"]["snippet"]
            rows.append({
                "video_id"    : video_id,
                "author"      : c.get("authorDisplayName"),
                "comment"     : c.get("textDisplay"),
                "like_count"  : c.get("likeCount", 0),
                "published_at": c.get("publishedAt")[:10],
                "ingested_at" : datetime.now(timezone.utc),
            })
    except Exception:
        pass

df_comments = pd.DataFrame(rows)
print(f"Total comments: {len(df_comments)}")

# ── Load to BigQuery ──────────────────────────────────────────
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

for df, table in [
    (df_channel,  "goshen-analytics.analytics.youtube_channel"),
    (df_videos,   "goshen-analytics.analytics.youtube_videos"),
    (df_comments, "goshen-analytics.analytics.youtube_comments"),
]:
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows → {table}")