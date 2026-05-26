import re
import os
from datetime import datetime, timezone
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

# ── CREDENTIALS ───────────────────────────────────────────────
# GOOGLE_APPLICATION_CREDENTIALS is set by Docker via environment variable
# No hardcoded path — works both locally and in container
client = bigquery.Client()

# ── CONFIG ────────────────────────────────────────────────────
# Reads from env var — set in .env for local, mounted volume in Docker
TIKTOK_FOLDER = os.getenv("TIKTOK_FOLDER", "./data/tiktok")

LIVE_HISTORY_FILE  = os.path.join(TIKTOK_FOLDER, "Go_LIVE_History.txt")
LIVE_COMMENTS_FILE = os.path.join(TIKTOK_FOLDER, "LiveStream_Comment.txt")
POSTS_FILE         = os.path.join(TIKTOK_FOLDER, "Posts.txt")
WATCH_LIVE_FILE    = os.path.join(TIKTOK_FOLDER, "Watch_LIVE_History.txt")
FOLLOWER_FILE      = os.path.join(TIKTOK_FOLDER, "Follower.txt")

BQ_PROJECT = os.getenv("GCP_PROJECT_ID", "goshen-analytics")
BQ_DATASET = os.getenv("BQ_DATASET", "analytics")

# ── HELPER ────────────────────────────────────────────────────
def now_utc():
    return datetime.now(timezone.utc)

def extract(pattern, text, default=None):
    match = re.search(pattern, text)
    return match.group(1).strip() if match else default


# ── 1. LIVE HISTORY ───────────────────────────────────────────
def parse_live_history(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    sessions = [s.strip() for s in content.strip().split("------------") if s.strip()]

    rows = []
    for session in sessions:
        duration_raw = extract(r"LIVE duration: (.+)", session)
        if not duration_raw:
            continue

        rows.append({
            "room_id"          : extract(r"Room Id: (.+)", session),
            "live_title_raw"   : extract(r"LIVE title: (.+)", session),
            "duration_raw"     : duration_raw,
            "total_view_raw"   : extract(r"Total view: (.+)", session),
            "total_gifter_raw" : extract(r"Total gifter: (.+)", session),
            "total_likes_raw"  : extract(r"Total likes received: (.+)", session),
            "ingested_at"      : now_utc(),
        })

    df = pd.DataFrame(rows)
    print(f"✅ Parsed {len(df)} live sessions")
    return df


# ── 2. LIVE COMMENTS ──────────────────────────────────────────
def parse_live_comments(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    sessions = [s.strip() for s in content.strip().split("-----------------------") if s.strip()]

    rows = []
    for session in sessions:
        comment = extract(r"Comment: (.+)", session)
        if not comment:
            continue

        rows.append({
            "room_id"          : extract(r"Room ID: (.+)", session),
            "comment_time_raw" : extract(r"Comment Time: (.+)", session),
            "comment_text"     : comment,
            "ingested_at"      : now_utc(),
        })

    df = pd.DataFrame(rows)
    print(f"✅ Parsed {len(df)} live comments")
    return df


# ── 3. POSTS ──────────────────────────────────────────────────
def parse_posts(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    sessions = [s.strip() for s in re.split(r"\n\n(?=Date:)", content.strip()) if s.strip()]

    rows = []
    for session in sessions:
        date_raw = extract(r"Date: (.+)", session)
        if not date_raw:
            continue

        rows.append({
            "date_raw"       : date_raw,
            "likes_raw"      : extract(r"Like\(s\): (.+)", session),
            "visibility_raw" : extract(r"Who can view: (.+)", session),
            "sound_raw"      : extract(r"Sound: (.+)", session),
            "location_raw"   : extract(r"Location: (.+)", session),
            "title_raw"      : extract(r"Title: (.+)", session),
            "ingested_at"    : now_utc(),
        })

    df = pd.DataFrame(rows)
    print(f"✅ Parsed {len(df)} posts")
    return df


# ── 4. WATCH LIVE HISTORY ─────────────────────────────────────
def parse_watch_live_history(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    sessions = [s.strip() for s in re.split(r"\n(?=Date and time:)", content.strip()) if s.strip()]

    rows = []
    for session in sessions:
        date_raw = extract(r"Date and time: (.+)", session)
        if not date_raw:
            continue

        raw_comment_lines = re.findall(r"(\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\] .+)", session)

        rows.append({
            "watched_at_raw"    : date_raw,
            "link_raw"          : extract(r"Link: (.+)", session),
            "raw_comments_blob" : "\n".join(raw_comment_lines) if raw_comment_lines else None,
            "ingested_at"       : now_utc(),
        })

    df = pd.DataFrame(rows)
    print(f"✅ Parsed {len(df)} watched live sessions")
    return df


# ── 5. FOLLOWERS ──────────────────────────────────────────────
def parse_followers(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    blocks = [b.strip() for b in re.split(r"\n\n(?=Date:)", content.strip()) if b.strip()]

    rows = []
    for block in blocks:
        date_raw = extract(r"Date: (.+)", block)
        username = extract(r"Username: (.+)", block)
        if not date_raw or not username:
            continue

        rows.append({
            "followed_at_raw" : date_raw,
            "username"        : username,
            "ingested_at"     : now_utc(),
        })

    df = pd.DataFrame(rows)
    print(f"✅ Parsed {len(df)} followers")
    return df


# ── PARSE ALL ─────────────────────────────────────────────────
df_live       = parse_live_history(LIVE_HISTORY_FILE)
df_comments   = parse_live_comments(LIVE_COMMENTS_FILE)
df_posts      = parse_posts(POSTS_FILE)
df_watch_live = parse_watch_live_history(WATCH_LIVE_FILE)
df_followers  = parse_followers(FOLLOWER_FILE)


# ── LOAD TO BIGQUERY ──────────────────────────────────────────
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

for df, table_name in [
    (df_live,       "tiktok_live_history"),
    (df_comments,   "tiktok_live_comments"),
    (df_posts,      "tiktok_posts"),
    (df_watch_live, "tiktok_watch_live_history"),
    (df_followers,  "tiktok_followers"),
]:
    full_table = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    if df.empty:
        print(f"⚠️  Skipping {full_table} — no data")
        continue
    job = client.load_table_from_dataframe(df, full_table, job_config=job_config)
    job.result()
    print(f"✅ Loaded {len(df)} rows → {full_table}")
