import pandas as pd
import os
import re
import requests
from datetime import datetime, timezone
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
client          = bigquery.Client()
YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
CHANNEL_ID      = os.getenv("YOUTUBE_CHANNEL_ID")
BASE_URL        = "https://www.googleapis.com/youtube/v3"


# ── HELPER ────────────────────────────────────────────────────
def parse_duration_seconds(iso_duration):
    """Convert ISO 8601 duration (PT12M34S) to total seconds.
    Kept as raw int here — bucketing is dbt's job."""
    if not iso_duration:
        return None
    pattern = re.compile(
        r'P(?:(\d+)D)?T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'
    )
    m = pattern.match(iso_duration)
    if not m:
        return None
    days    = int(m.group(1) or 0)
    hours   = int(m.group(2) or 0)
    minutes = int(m.group(3) or 0)
    seconds = int(m.group(4) or 0)
    return days * 86400 + hours * 3600 + minutes * 60 + seconds


# ── 1. Channel Stats ──────────────────────────────────────────
print("📡 Fetching channel stats...")
r = requests.get(f"{BASE_URL}/channels", params={
    "part" : "snippet,statistics,contentDetails",
    "id"   : CHANNEL_ID,
    "key"  : YOUTUBE_API_KEY
})
item    = r.json()["items"][0]
snippet = item["snippet"]
stats   = item["statistics"]

# Grab uploads playlist ID — no extra quota cost
uploads_playlist_id = item["contentDetails"]["relatedPlaylists"]["uploads"]

df_channel = pd.DataFrame([{
    "channel_id"       : CHANNEL_ID,
    "channel_title"    : snippet["title"],
    "subscriber_count" : stats.get("subscriberCount"),
    "view_count"       : stats.get("viewCount"),
    "video_count"      : stats.get("videoCount"),
    "ingested_at"      : datetime.now(timezone.utc),
}])
print(f"✅ Channel: {snippet['title']}")
print(f"📋 Uploads playlist: {uploads_playlist_id}")


# ── 2. Video IDs via playlistItems (complete inventory) ───────
# Fix: search() silently drops older videos — it's a search index, not inventory.
# playlistItems() on uploads playlist returns every video ever uploaded.
print("\n📡 Fetching all video IDs...")
video_ids = []
next_page = None

while True:
    params = {
        "part"       : "contentDetails",
        "playlistId" : uploads_playlist_id,
        "maxResults" : 50,
        "key"        : YOUTUBE_API_KEY
    }
    if next_page:
        params["pageToken"] = next_page

    r         = requests.get(f"{BASE_URL}/playlistItems", params=params)
    data      = r.json()
    video_ids += [item["contentDetails"]["videoId"] for item in data.get("items", [])]
    next_page  = data.get("nextPageToken")
    if not next_page:
        break

print(f"✅ Found {len(video_ids)} videos (full channel inventory)")


# ── 3. Video Performance + Metadata ──────────────────────────
# snippet        → title, published_at, tags, categoryId, defaultLanguage, liveBroadcastContent
# statistics     → viewCount, likeCount, commentCount, favoriteCount
# contentDetails → duration, caption, definition
print("\n📡 Fetching video details...")
rows = []
for i in range(0, len(video_ids), 50):
    chunk = video_ids[i:i+50]
    r = requests.get(f"{BASE_URL}/videos", params={
        "part" : "snippet,statistics,contentDetails",
        "id"   : ",".join(chunk),
        "key"  : YOUTUBE_API_KEY
    })
    for item in r.json().get("items", []):
        s  = item["snippet"]
        v  = item.get("statistics", {})
        cd = item.get("contentDetails", {})

        rows.append({
            # identifiers
            "video_id"               : item["id"],
            "title"                  : s.get("title"),
            "published_at"           : s.get("publishedAt")[:10],

            # performance
            "view_count"             : v.get("viewCount", 0),
            "like_count"             : v.get("likeCount", 0),
            "comment_count"          : v.get("commentCount", 0),
            "favorite_count"         : v.get("favoriteCount", 0),

            # content metadata
            "duration_raw"           : cd.get("duration"),                          # e.g. "PT12M34S" — keep raw for dbt
            "duration_seconds"       : parse_duration_seconds(cd.get("duration")),  # pre-parsed int
            "caption"                : cd.get("caption"),                           # "true" / "false"
            "definition"             : cd.get("definition"),                        # "hd" / "sd"
            "live_broadcast_content" : s.get("liveBroadcastContent"),               # "live" / "none" / "upcoming"
            "category_id"            : s.get("categoryId"),                         # YouTube category number
            "default_language"       : s.get("defaultLanguage"),
            "tags_raw"               : ",".join(s.get("tags", [])) if s.get("tags") else None,  # comma-separated, split in dbt

            "ingested_at"            : datetime.now(timezone.utc),
        })

df_videos = pd.DataFrame(rows)
print(f"✅ Fetched details for {len(df_videos)} videos")
print(df_videos[["title", "published_at", "view_count", "duration_seconds", "live_broadcast_content"]].head(10))


# ── 4. Comments ───────────────────────────────────────────────
print("\n📡 Fetching comments...")
rows = []
skipped = 0
for video_id in video_ids:
    try:
        r = requests.get(f"{BASE_URL}/commentThreads", params={
            "part"       : "snippet",
            "videoId"    : video_id,
            "maxResults" : 100,
            "key"        : YOUTUBE_API_KEY
        })
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
        skipped += 1  # comments disabled on some videos — skip silently
        pass

df_comments = pd.DataFrame(rows)
print(f"✅ Total comments: {len(df_comments)} (skipped {skipped} videos with disabled comments)")


# ── 5. Load to BigQuery (WRITE_APPEND) ────────────────────────
# Deduplication handled in dbt staging using video_id + row_number()
print("\n📤 Loading to BigQuery...")
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

for df, table in [
    (df_channel,  "goshen-analytics.analytics.youtube_channel"),
    (df_videos,   "goshen-analytics.analytics.youtube_videos"),
    (df_comments, "goshen-analytics.analytics.youtube_comments"),
]:
    if df.empty:
        print(f"⚠️  Skipping {table} — no data")
        continue
    job = client.load_table_from_dataframe(df, table, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows → {table}")

print("\n🎉 Ingestion complete. Now run: dbt run --select stg_youtube_videos+")