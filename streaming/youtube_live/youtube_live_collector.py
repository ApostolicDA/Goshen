"""
Goshen Analytics - YouTube Live Collector
-----------------------------------------------------------------------------
Architecture:
  YouTube Data API v3 (polling) -> async handlers -> in-memory buffer
  -> 30s flush -> publish to Pub/Sub topic (youtube-live-events)
  -> GCP BigQuery subscription writes rows to BQ automatically
  -> post-session: HTML report -> email to leadership

OAuth2 note:
  First run opens a browser window to authenticate with the Goshen
  Google account. A token file is saved locally and auto-refreshed.
  Never need to authenticate again unless you delete the token file.

Environment variables (same .env.local as TikTok collector):
  GOOGLE_APPLICATION_CREDENTIALS  - path to GCP service account JSON
  YOUTUBE_OAUTH_CREDENTIALS        - path to OAuth2 client secret JSON
  YOUTUBE_CHANNEL_ID               - Goshen YouTube channel ID
  GCP_PROJECT_ID                   - GCP project ID (goshen-analytics)
  PUBSUB_TOPIC_ID                  - Pub/Sub topic ID (youtube-live-events)
  GMAIL_SENDER                     - sending Gmail address
  GMAIL_APP_PASSWORD               - Gmail App Password
  EMAIL_RECIPIENTS                 - comma-separated recipient addresses
-----------------------------------------------------------------------------
"""

import asyncio
import random
import json
import os
import smtplib
import ssl
import uuid
from collections import Counter
from datetime import datetime, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery, pubsub_v1
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

load_dotenv(r"C:\Users\gadis\goshen\streaming\tiktok_live\.env.local")


# ==============================================================================
# CONFIG
# ==============================================================================
CHANNEL_ID       = os.environ["YOUTUBE_CHANNEL_ID"]
OAUTH_CREDS_FILE = os.environ["YOUTUBE_OAUTH_CREDENTIALS"]
TOKEN_FILE       = "youtube_token.json"
SCOPES           = ["https://www.googleapis.com/auth/youtube.readonly"]

POLL_INTERVAL_S      = 60     # how often to check if stream is live
CHAT_POLL_INTERVAL_S = 5      # how often to fetch new chat messages
VIEWER_POLL_INTERVAL_S = 30   # how often to fetch viewer count
FLUSH_INTERVAL_S     = 30
BATCH_SIZE           = 200

GCP_PROJECT   = os.environ["GCP_PROJECT_ID"]
TOPIC_ID      = os.environ.get("PUBSUB_TOPIC_ID", "youtube-live-events")
TOPIC_PATH    = f"projects/{GCP_PROJECT}/topics/{TOPIC_ID}"

BQ_DATASET    = os.environ["BQ_DATASET"]
BQ_TABLE      = "raw_youtube_live_events"
FULL_TABLE_ID = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

SMTP_HOST        = "smtp.gmail.com"
SMTP_PORT        = 465
EMAIL_SENDER     = os.environ["GMAIL_SENDER"]
EMAIL_PASSWORD   = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_RECIPIENTS = [e.strip() for e in os.environ.get("EMAIL_RECIPIENTS", EMAIL_SENDER).split(",")]

SESSIONS_ROOT = Path("./youtube_live_sessions")
SESSIONS_ROOT.mkdir(exist_ok=True)

publisher = pubsub_v1.PublisherClient()
bq_client = bigquery.Client(project=GCP_PROJECT)

# ==============================================================================
# BQ FALLBACK SCHEMA
# ==============================================================================
SCHEMA = [
    bigquery.SchemaField("event_id",      "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("session_id",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_type",    "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_ts",      "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",   "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("username",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("user_id",       "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("comment_text",  "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("viewer_count",  "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("like_count",    "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("video_id",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("live_chat_id",  "STRING",    mode="NULLABLE"),
]


def ensure_fallback_table_exists():
    try:
        bq_client.get_table(FULL_TABLE_ID)
        log(f"BQ fallback table exists: {FULL_TABLE_ID}")
    except Exception:
        log(f"Creating BQ fallback table: {FULL_TABLE_ID}")
        bq_client.create_table(bigquery.Table(FULL_TABLE_ID, schema=SCHEMA))
        log("BQ fallback table created")


# ==============================================================================
# LOGGING
# ==============================================================================
def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ==============================================================================
# OAUTH2 AUTHENTICATION
# ==============================================================================
def get_youtube_client():
    """
    Authenticate with OAuth2 and return a YouTube API client.
    First run: opens browser for Goshen Google account login.
    Subsequent runs: loads saved token, refreshes if expired.
    """
    creds = None

    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log("Refreshing YouTube OAuth2 token...")
            creds.refresh(Request())
        else:
            log("Opening browser for YouTube authentication...")
            flow = InstalledAppFlow.from_client_secrets_file(OAUTH_CREDS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as f:
            f.write(creds.to_json())
        log(f"Token saved to {TOKEN_FILE}")

    return build("youtube", "v3", credentials=creds)


# ==============================================================================
# STREAM DETECTION
# ==============================================================================
def get_live_stream(youtube):
    """
    Check if the Goshen channel is currently live.
    Returns (video_id, live_chat_id) if live, (None, None) if not.
    """
    try:
        response = youtube.search().list(
            part="snippet",
            channelId=CHANNEL_ID,
            eventType="live",
            type="video",
            maxResults=1
        ).execute()

        items = response.get("items", [])
        if not items:
            return None, None

        video_id = items[0]["id"]["videoId"]
        title    = items[0]["snippet"]["title"]
        log(f"Live stream detected: '{title}' (videoId: {video_id})")

        # Get the live chat ID from the video details
        video_response = youtube.videos().list(
            part="liveStreamingDetails",
            id=video_id
        ).execute()

        video_items = video_response.get("items", [])
        if not video_items:
            return video_id, None

        live_chat_id = video_items[0].get("liveStreamingDetails", {}).get("activeLiveChatId")
        return video_id, live_chat_id

    except Exception as e:
        log(f"Error checking live status: {e}")
        return None, None


def get_viewer_count(youtube, video_id):
    """Fetch current concurrent viewer count."""
    try:
        response = youtube.videos().list(
            part="liveStreamingDetails",
            id=video_id
        ).execute()
        items = response.get("items", [])
        if items:
            details = items[0].get("liveStreamingDetails", {})
            return int(details.get("concurrentViewers", 0))
    except Exception as e:
        log(f"Error fetching viewer count: {e}")
    return 0


# ==============================================================================
# SESSION
# ==============================================================================
class LiveSession:

    def __init__(self, video_id, live_chat_id):
        self.session_id    = str(uuid.uuid4())
        self.started_at    = datetime.now(timezone.utc)
        self.video_id      = video_id
        self.live_chat_id  = live_chat_id
        self.folder        = SESSIONS_ROOT / self.session_id
        self.folder.mkdir(exist_ok=True)

        self._buffer         = []
        self._lock           = asyncio.Lock()
        self._all_events     = []
        self.total_published = 0
        self.reconnections   = 0

        # YouTube chat polling state
        self._next_page_token = None

    def _make_row(self, event_type, **kwargs):
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "event_id":     str(uuid.uuid4()),
            "session_id":   self.session_id,
            "event_type":   event_type,
            "event_ts":     now,
            "ingested_at":  now,
            "video_id":     self.video_id,
            "live_chat_id": self.live_chat_id,
            **kwargs,
        }
        self._all_events.append(row)
        return row

    async def add(self, event_type, **kwargs):
        async with self._lock:
            self._buffer.append(self._make_row(event_type, **kwargs))
            if len(self._buffer) >= BATCH_SIZE:
                await self._flush(label="batch-size")

    async def flush(self, label="scheduled"):
        async with self._lock:
            await self._flush(label=label)

    async def _flush(self, label):
        if not self._buffer:
            return

        rows = self._buffer.copy()
        self._buffer.clear()

        is_final  = "final" in label
        flush_ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        stem      = f"{self.session_id}_final_{flush_ts}" if is_final else f"{self.session_id}_{flush_ts}"
        file_path = self.folder / f"{stem}.jsonl"

        with open(file_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        log(f"[{label}] Flushing {len(rows)} events -> {file_path.name}")

        pubsub_ok = await self._publish_to_pubsub(rows, label)
        if not pubsub_ok:
            log(f"[{label}] Falling back to BQ load job...")
            await self._load_to_bq(file_path, rows, label)

    async def _publish_to_pubsub(self, rows, label):
        futures = []
        try:
            for row in rows:
                data  = json.dumps(row).encode("utf-8")
                attrs = {"event_type": row["event_type"], "session_id": self.session_id}
                futures.append(publisher.publish(TOPIC_PATH, data=data, **attrs))
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._resolve_futures, futures)
            self.total_published += len(rows)
            log(f"[{label}] [PRIMARY] Published {len(rows)} msgs to {TOPIC_ID} (total: {self.total_published})")
            return True
        except Exception as e:
            log(f"[{label}] [PRIMARY] Pub/Sub failed: {type(e).__name__}: {e}")
            return False

    async def _load_to_bq(self, file_path, rows, label):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=SCHEMA,
        )
        try:
            with open(file_path, "rb") as f:
                load_job = bq_client.load_table_from_file(f, FULL_TABLE_ID, job_config=job_config)
            await asyncio.get_running_loop().run_in_executor(None, load_job.result)
            self.total_published += len(rows)
            log(f"[{label}] [FALLBACK] BQ load job: {len(rows)} rows -> {FULL_TABLE_ID}")
        except Exception as e:
            log(f"[{label}] [FALLBACK] BQ load job also failed: {e}")
            log(f"[{label}] Data is safe locally: {file_path}")

    @staticmethod
    def _resolve_futures(futures):
        for f in futures:
            f.result()

    async def flush_loop(self, stop):
        while not stop.is_set():
            await asyncio.sleep(FLUSH_INTERVAL_S)
            if not stop.is_set():
                await self.flush(label="30s-flush")

    # ==============================================================================
    # CHAT POLLING
    # ==============================================================================
    async def poll_chat(self, youtube, stop):
        """
        Poll YouTube live chat every 5 seconds for new messages.
        YouTube returns a nextPageToken to track where we left off.
        """
        log(f"Starting chat polling for liveChatId: {self.live_chat_id}")

        while not stop.is_set():
            try:
                params = {
                    "liveChatId": self.live_chat_id,
                    "part": "snippet,authorDetails",
                    "maxResults": 200,
                }
                if self._next_page_token:
                    params["pageToken"] = self._next_page_token

                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: youtube.liveChatMessages().list(**params).execute()
                )

                self._next_page_token = response.get("nextPageToken")
                items = response.get("items", [])

                for item in items:
                    snippet       = item.get("snippet", {})
                    author        = item.get("authorDetails", {})
                    message_type  = snippet.get("type", "textMessageEvent")
                    published_at  = snippet.get("publishedAt", "")
                    display_name  = author.get("displayName", "unknown")
                    channel_id    = author.get("channelId", "")
                    message_text  = snippet.get("displayMessage", "")

                    await self.add(
                        "chat_message",
                        username=display_name,
                        user_id=channel_id,
                        comment_text=message_text,
                    )
                    log(f"Chat: {display_name}: {message_text}")

                # YouTube tells us how long to wait before next poll
                polling_interval = response.get("pollingIntervalMillis", 5000) / 1000
                await asyncio.sleep(max(polling_interval, CHAT_POLL_INTERVAL_S))

            except Exception as e:
                log(f"Chat poll error: {type(e).__name__}: {e} - retrying in 10s...")
                await asyncio.sleep(10)

    # ==============================================================================
    # VIEWER COUNT POLLING
    # ==============================================================================
    async def poll_viewers(self, youtube, stop):
        """Poll viewer count every 30 seconds."""
        log("Starting viewer count polling...")

        while not stop.is_set():
            try:
                loop = asyncio.get_running_loop()
                count = await loop.run_in_executor(
                    None,
                    lambda: get_viewer_count(youtube, self.video_id)
                )
                await self.add("viewer_count", viewer_count=count)
                log(f"Viewers: {count}")
            except Exception as e:
                log(f"Viewer poll error: {e}")

            await asyncio.sleep(VIEWER_POLL_INTERVAL_S)

    # ==============================================================================
    # STREAM END DETECTION
    # ==============================================================================
    async def watch_for_stream_end(self, youtube, stop):
        """Check every 60 seconds if the stream is still live."""
        while not stop.is_set():
            await asyncio.sleep(POLL_INTERVAL_S)
            try:
                loop = asyncio.get_running_loop()
                video_id, _ = await loop.run_in_executor(
                    None,
                    lambda: get_live_stream(youtube)
                )
                if not video_id:
                    log("Stream ended - shutting down collectors...")
                    stop.set()
                    return
            except Exception as e:
                log(f"Stream end check error: {e}")

    # ==============================================================================
    # REPORT
    # ==============================================================================
    def build_report(self):
        ended_at     = datetime.now(timezone.utc)
        duration_s   = int((ended_at - self.started_at).total_seconds())
        duration_fmt = f"{duration_s // 60}m {duration_s % 60}s"

        ev         = self._all_events
        viewer_ev  = [e for e in ev if e["event_type"] == "viewer_count"]
        chat_ev    = [e for e in ev if e["event_type"] == "chat_message"]

        peak_viewers = max((e.get("viewer_count", 0) for e in viewer_ev), default=0)
        avg_viewers  = int(sum(e.get("viewer_count", 0) for e in viewer_ev) / len(viewer_ev)) if viewer_ev else 0

        top_chatters    = Counter(e.get("username", "unknown") for e in chat_ev).most_common(5)
        recent_messages = chat_ev[-8:]

        viewer_sample = viewer_ev[::5] if len(viewer_ev) > 5 else viewer_ev
        chart_labels  = json.dumps([e["event_ts"][11:16] for e in viewer_sample])
        chart_data    = json.dumps([e.get("viewer_count", 0) for e in viewer_sample])

        stats = {
            "session_id":      self.session_id,
            "session_date":    self.started_at.strftime("%d %B %Y"),
            "session_start":   self.started_at.strftime("%H:%M UTC"),
            "session_end":     ended_at.strftime("%H:%M UTC"),
            "duration":        duration_fmt,
            "peak_viewers":    peak_viewers,
            "avg_viewers":     avg_viewers,
            "total_messages":  len(chat_ev),
            "total_published": self.total_published,
            "reconnections":   self.reconnections,
            "video_id":        self.video_id,
        }

        def top_rows(items):
            if not items:
                return "<tr><td colspan='2' style='color:#888;text-align:center'>No data</td></tr>"
            medals = ["1st", "2nd", "3rd"]
            return "".join(
                f"<tr><td>{medals[i] if i < 3 else ''} <strong>{name}</strong></td>"
                f"<td style='text-align:right;color:#ff0000'>{count}</td></tr>"
                for i, (name, count) in enumerate(items)
            )

        def message_cards():
            if not recent_messages:
                return "<p style='color:#888'>No messages recorded.</p>"
            return "".join(
                f"<div class='chat-card'>"
                f"<span class='chatter'>{e.get('username','unknown')}</span>"
                f"<span class='chat-time'>{e['event_ts'][11:16]}</span>"
                f"<p>{e.get('comment_text','')}</p>"
                f"</div>"
                for e in recent_messages
            )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Goshen Global - YouTube Live Session Report</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{ --red:#ff0000; --dark-red:#cc0000; --dark:#0d0d0d; --card:#1a1a1a; --border:#2a2a2a; --text:#e8e0d0; --muted:#888; }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--dark); color:var(--text); font-family:monospace; font-size:13px; line-height:1.7; }}
  .header {{ background:#0d0d0d; border-bottom:2px solid var(--red); padding:48px 40px 36px; }}
  .platform {{ font-size:11px; letter-spacing:.35em; text-transform:uppercase; color:var(--red); margin-bottom:8px; }}
  .report-title {{ font-size:38px; font-weight:300; line-height:1.1; margin-bottom:16px; }}
  .report-title strong {{ color:var(--red); }}
  .meta-row {{ display:flex; gap:32px; flex-wrap:wrap; color:var(--muted); font-size:11px; }}
  .meta-row span {{ color:var(--text); }}
  .container {{ max-width:900px; margin:0 auto; padding:40px; }}
  .section-title {{ font-size:11px; letter-spacing:.3em; text-transform:uppercase; color:var(--red); margin:40px 0 16px; }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1px; background:var(--border); border:1px solid var(--border); }}
  .kpi {{ background:var(--card); padding:24px 20px; }}
  .kpi.highlight {{ background:#1a0000; }}
  .kpi-label {{ font-size:9px; letter-spacing:.2em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }}
  .kpi-value {{ font-size:42px; font-weight:300; line-height:1; }}
  .kpi.highlight .kpi-value {{ color:var(--red); }}
  .kpi-sub {{ font-size:10px; color:var(--muted); margin-top:4px; }}
  .chart-wrap {{ background:var(--card); border:1px solid var(--border); padding:24px; }}
  .two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  .data-card {{ background:var(--card); border:1px solid var(--border); padding:20px; }}
  .data-card h4 {{ font-size:9px; letter-spacing:.2em; text-transform:uppercase; color:var(--muted); margin-bottom:14px; }}
  table {{ width:100%; border-collapse:collapse; }}
  td {{ padding:7px 0; border-bottom:1px solid var(--border); font-size:12px; }}
  tr:last-child td {{ border-bottom:none; }}
  .chat-card {{ background:var(--card); border-left:2px solid var(--red); padding:12px 16px; margin-bottom:8px; }}
  .chatter {{ font-size:11px; color:var(--red); font-weight:500; margin-right:12px; }}
  .chat-time {{ font-size:10px; color:var(--muted); }}
  .chat-card p {{ margin-top:4px; color:var(--text); font-size:12px; }}
  .yt-link {{ display:inline-block; margin-top:8px; padding:8px 16px; background:var(--red); color:#fff; text-decoration:none; font-size:11px; letter-spacing:.1em; }}
  .footer {{ margin-top:48px; padding-top:24px; border-top:1px solid var(--border); color:var(--muted); font-size:10px; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
  .footer strong {{ color:var(--red); }}
</style>
</head>
<body>
<div class="header">
  <div class="platform">Goshen Global Church - YouTube Live</div>
  <h1 class="report-title">YouTube Live<br><strong>Session Report</strong></h1>
  <div class="meta-row">
    <div>Date &nbsp;<span>{stats['session_date']}</span></div>
    <div>Start &nbsp;<span>{stats['session_start']}</span></div>
    <div>End &nbsp;<span>{stats['session_end']}</span></div>
    <div>Duration &nbsp;<span>{stats['duration']}</span></div>
  </div>
  <a class="yt-link" href="https://www.youtube.com/watch?v={stats['video_id']}" target="_blank">Watch Recording</a>
</div>
<div class="container">
  <div class="section-title">Reach and Engagement</div>
  <div class="kpi-grid">
    <div class="kpi highlight">
      <div class="kpi-label">Peak Viewers</div>
      <div class="kpi-value">{stats['peak_viewers']}</div>
      <div class="kpi-sub">avg {stats['avg_viewers']} throughout</div>
    </div>
    <div class="kpi"><div class="kpi-label">Chat Messages</div><div class="kpi-value">{stats['total_messages']}</div></div>
    <div class="kpi"><div class="kpi-label">Duration</div><div class="kpi-value" style="font-size:28px">{stats['duration']}</div></div>
    <div class="kpi"><div class="kpi-label">Reconnections</div><div class="kpi-value">{stats['reconnections']}</div><div class="kpi-sub">interruptions</div></div>
  </div>
  <div class="section-title">Viewer Curve</div>
  <div class="chart-wrap"><canvas id="viewerChart" height="80"></canvas></div>
  <div class="section-title">Top Chatters</div>
  <div class="two-col">
    <div class="data-card"><h4>Most Active in Chat</h4><table>{top_rows(top_chatters)}</table></div>
    <div class="data-card"><h4>Session Info</h4>
      <table>
        <tr><td>Video ID</td><td style='text-align:right;font-size:10px'>{stats['video_id']}</td></tr>
        <tr><td>Session ID</td><td style='text-align:right;font-size:10px'>{stats['session_id'][:12]}...</td></tr>
        <tr><td>Events Published</td><td style='text-align:right'>{stats['total_published']}</td></tr>
      </table>
    </div>
  </div>
  <div class="section-title">Recent Chat Messages</div>
  {message_cards()}
  <div class="footer">
    <div>Generated by <strong>Goshen Analytics Pipeline</strong> - Proud Ndlovu</div>
    <div>YouTube Live - Session {stats['session_id'][:8]}</div>
  </div>
</div>
<script>
new Chart(document.getElementById('viewerChart').getContext('2d'), {{
  type: 'line',
  data: {{
    labels: {chart_labels},
    datasets: [{{
      label: 'Viewers', data: {chart_data},
      borderColor: '#ff0000', backgroundColor: 'rgba(255,0,0,0.08)',
      borderWidth: 2, pointRadius: 3, pointBackgroundColor: '#ff0000',
      fill: true, tension: 0.4
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color:'#888' }}, grid: {{ color:'#222' }} }},
      y: {{ ticks: {{ color:'#888' }}, grid: {{ color:'#222' }}, beginAtZero: false }}
    }}
  }}
}});
</script>
</body>
</html>"""

        return stats, html

    async def save_and_email_report(self):
        log("Building YouTube live session report...")
        stats, html = self.build_report()

        report_path = self.folder / f"{self.session_id}_report.html"
        report_path.write_text(html, encoding="utf-8")
        log(f"Report saved -> {report_path}")

        subject = f"Goshen Global - YouTube Live - {stats['session_date']} - {stats['peak_viewers']} peak viewers"
        plain = (
            f"Goshen Global Church - YouTube Live Session Report\n"
            f"{'=' * 50}\n"
            f"Date         : {stats['session_date']}\n"
            f"Duration     : {stats['duration']}\n"
            f"Peak Viewers : {stats['peak_viewers']} (avg {stats['avg_viewers']})\n"
            f"Chat Messages: {stats['total_messages']}\n"
            f"Reconnections: {stats['reconnections']}\n"
            f"Video        : https://www.youtube.com/watch?v={stats['video_id']}\n"
            f"{'=' * 50}\n"
            f"Full report attached - open in any browser.\n"
            f"Session ID   : {stats['session_id']}\n"
            f"\n- Goshen Analytics Pipeline"
        )

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = ", ".join(EMAIL_RECIPIENTS)

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain, "plain"))
        alt.attach(MIMEText(html, "html"))
        msg.attach(alt)

        with open(report_path, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", f"attachment; filename={report_path.name}")
        msg.attach(part)

        try:
            context = ssl.create_default_context()
            await asyncio.get_running_loop().run_in_executor(
                None, lambda: self._send_email(context, msg)
            )
            log(f"Report emailed to: {', '.join(EMAIL_RECIPIENTS)}")
        except Exception as e:
            log(f"Email failed: {e}")
            log(f"Report available locally: {report_path}")

    def _send_email(self, context, msg):
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())


# ==============================================================================
# MAIN
# ==============================================================================
async def main():
    log("=" * 60)
    log("  GOSHEN ANALYTICS - YouTube Live Collector")
    log(f"  Channel    : {CHANNEL_ID}")
    log(f"  PRIMARY    : Pub/Sub -> {TOPIC_PATH}")
    log(f"  FALLBACK   : BQ load job -> {FULL_TABLE_ID}")
    log(f"  Flush      : every {FLUSH_INTERVAL_S}s  |  Poll: every {POLL_INTERVAL_S}s")
    log("=" * 60)

    ensure_fallback_table_exists()

    log("Authenticating with YouTube...")
    youtube = get_youtube_client()
    log("YouTube client ready")

    log(f"Monitoring channel {CHANNEL_ID} - checking every {POLL_INTERVAL_S}s until live...")

    # Poll until the channel goes live
    video_id, live_chat_id = None, None
    while not video_id:
        loop = asyncio.get_running_loop()
        video_id, live_chat_id = await loop.run_in_executor(
            None, lambda: get_live_stream(youtube)
        )
        if not video_id:
            jitter = POLL_INTERVAL_S + random.randint(0, 30)
            log(f"Not live - retrying in {jitter}s...")
            await asyncio.sleep(jitter)

    log(f"Stream is live! videoId: {video_id} | liveChatId: {live_chat_id}")

    session    = LiveSession(video_id, live_chat_id)
    stop       = asyncio.Event()

    log(f"Session started: {session.session_id}")
    log(f"Session folder : {session.folder}")

    await session.add("stream_start", video_id=video_id)

    # Start all background tasks concurrently
    await asyncio.gather(
        session.flush_loop(stop),
        session.poll_chat(youtube, stop),
        session.poll_viewers(youtube, stop),
        session.watch_for_stream_end(youtube, stop),
    )

    # Stream ended - final flush and report
    await session.flush(label="final-flush")
    await session.save_and_email_report()

    log("=" * 60)
    log(f"  SESSION COMPLETE")
    log(f"  ID              : {session.session_id}")
    log(f"  Folder          : {session.folder.resolve()}")
    log(f"  Pub/Sub msgs    : {session.total_published}")
    log(f"  Reconnections   : {session.reconnections}")
    log("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())