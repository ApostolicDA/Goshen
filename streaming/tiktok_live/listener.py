"""
Goshen Analytics — TikTok Live Collector
─────────────────────────────────────────────────────────────────────────────
Architecture:
  TikTok WebSocket → async event handlers → in-memory buffer
  → 30s flush → publish to Pub/Sub topic (tiktok-live-events)
  → GCP BigQuery subscription writes rows to BQ automatically (free-tier safe)
  → post-session: HTML report → email to leadership

Folder layout:
  tiktok_live_sessions/
  └── {session_id}/
      ├── {session_id}_20260527T190730Z.jsonl   ← local backup
      ├── {session_id}_final_20260527T190910Z.jsonl
      └── {session_id}_report.html

Environment variables (set in .env.local or your shell):
  GOOGLE_APPLICATION_CREDENTIALS  — path to GCP service account JSON
  GCP_PROJECT_ID                   — GCP project ID (goshen-analytics)
  PUBSUB_TOPIC_ID                  — Pub/Sub topic ID (tiktok-live-events)
  GMAIL_SENDER                     — sending Gmail address
  GMAIL_APP_PASSWORD               — Gmail App Password
  EMAIL_RECIPIENTS                 — comma-separated recipient addresses

Run:
  pip install TikTokLive google-cloud-pubsub python-dotenv
  python tiktok_live_collector.py

BQ subscription note:
  The BigQuery subscription (tiktok-live-bq-subscription) writes Pub/Sub
  messages to your BQ table. Without a topic schema, BQ stores messages
  using the default Pub/Sub subscription schema:
    subscription_name STRING
    message_id        STRING
    publish_time      TIMESTAMP
    attributes        STRING   (JSON key-value pairs)
    data              BYTES    (your event JSON, base64-encoded)

  Your dbt staging model decodes the data column like this:
    SAFE.JSON_VALUE(SAFE_CONVERT_BYTES_TO_STRING(data), '$.event_type')

  To write directly to typed columns instead, define an Avro/Proto schema
  on the topic in GCP and re-create the BQ subscription with schema enabled.
─────────────────────────────────────────────────────────────────────────────
"""

import asyncio
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
from TikTokLive import TikTokLiveClient
from TikTokLive.client.errors import UserOfflineError
from TikTokLive.events import (
    CommentEvent,
    ConnectEvent,
    DisconnectEvent,
    FollowEvent,
    GiftEvent,
    LikeEvent,
    LiveEndEvent,
    RoomUserSeqEvent,
    ShareEvent,
)

load_dotenv(".env.local")


# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
TARGET_USER       = "@goshenglobal"
POLL_INTERVAL_S   = 60
FLUSH_INTERVAL_S  = 30
BATCH_SIZE        = 200

GCP_PROJECT       = os.environ["GCP_PROJECT_ID"]
TOPIC_ID          = os.environ.get("PUBSUB_TOPIC_ID", "tiktok-live-events")
TOPIC_PATH        = f"projects/{GCP_PROJECT}/topics/{TOPIC_ID}"

# BigQuery fallback config
BQ_DATASET        = os.environ["BQ_DATASET"]
BQ_TABLE          = "raw_tiktok_live_events"
FULL_TABLE_ID     = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

SMTP_HOST         = "smtp.gmail.com"
SMTP_PORT         = 465
EMAIL_SENDER      = os.environ["GMAIL_SENDER"]
EMAIL_PASSWORD    = os.environ["GMAIL_APP_PASSWORD"]
EMAIL_RECIPIENTS  = [e.strip() for e in os.environ.get("EMAIL_RECIPIENTS", EMAIL_SENDER).split(",")]

SESSIONS_ROOT     = Path("./tiktok_live_sessions")
SESSIONS_ROOT.mkdir(exist_ok=True)

# ── Clients ───────────────────────────────────────────────────────────────────
publisher  = pubsub_v1.PublisherClient()
bq_client  = bigquery.Client(project=GCP_PROJECT)

# ══════════════════════════════════════════════════════════════════════════════
# BQ FALLBACK SCHEMA  —  only used if Pub/Sub publish fails
# ══════════════════════════════════════════════════════════════════════════════
SCHEMA = [
    bigquery.SchemaField("event_id",     "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("session_id",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_type",   "STRING",    mode="REQUIRED"),
    bigquery.SchemaField("event_ts",     "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at",  "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("username",     "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("user_id",      "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("comment_text", "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("like_count",   "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("viewer_count", "INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("gift_name",    "STRING",    mode="NULLABLE"),
    bigquery.SchemaField("diamond_count","INTEGER",   mode="NULLABLE"),
    bigquery.SchemaField("room_id",      "STRING",    mode="NULLABLE"),
]


def ensure_fallback_table_exists():
    """Create the BQ fallback table if it doesn't already exist."""
    try:
        bq_client.get_table(FULL_TABLE_ID)
        log(f"✅ BQ fallback table exists: {FULL_TABLE_ID}")
    except Exception:
        log(f"📦 Creating BQ fallback table: {FULL_TABLE_ID}")
        bq_client.create_table(bigquery.Table(FULL_TABLE_ID, schema=SCHEMA))
        log("✅ BQ fallback table created")


# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════
def log(msg: str):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ══════════════════════════════════════════════════════════════════════════════
# SESSION
# ══════════════════════════════════════════════════════════════════════════════
class LiveSession:
    """
    Owns all state for a single TikTok live stream connection.
    Publishes events to Pub/Sub; GCP's BigQuery subscription handles BQ writes.
    Local JSONL files are written as a backup before every publish attempt.
    """

    def __init__(self):
        self.session_id          = str(uuid.uuid4())
        self.started_at          = datetime.now(timezone.utc)
        self.folder              = SESSIONS_ROOT / self.session_id
        self.folder.mkdir(exist_ok=True)

        self._buffer: list[dict] = []
        self._lock               = asyncio.Lock()
        self._all_events: list[dict] = []
        self.total_published     = 0   # messages successfully published
        self.reconnections       = 0

    # ── Row factory ──────────────────────────────────────────────────────────
    def _make_row(self, event_type: str, **kwargs) -> dict:
        now = datetime.now(timezone.utc).isoformat()
        row = {
            "event_id":    str(uuid.uuid4()),
            "session_id":  self.session_id,
            "event_type":  event_type,
            "event_ts":    now,
            "ingested_at": now,
            **kwargs,
        }
        self._all_events.append(row)
        return row

    async def add(self, event_type: str, **kwargs):
        async with self._lock:
            self._buffer.append(self._make_row(event_type, **kwargs))
            if len(self._buffer) >= BATCH_SIZE:
                await self._flush(label="batch-size")

    # ── Flush ─────────────────────────────────────────────────────────────────
    async def flush(self, label: str = "scheduled"):
        async with self._lock:
            await self._flush(label=label)

    async def _flush(self, label: str):
        """
        Delivery order for each flush:
          1. Write JSONL to disk — always, before any network call.
          2. PRIMARY: publish to Pub/Sub → GCP BQ subscription writes to BQ.
          3. FALLBACK: if Pub/Sub fails, run a BQ load job from the JSONL file.
        Must be called with self._lock held.
        """
        if not self._buffer:
            return

        rows = self._buffer.copy()
        self._buffer.clear()

        is_final  = "final" in label
        flush_ts  = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        stem      = f"{self.session_id}_final_{flush_ts}" if is_final else f"{self.session_id}_{flush_ts}"
        file_path = self.folder / f"{stem}.jsonl"

        # ── 1. Local JSONL — always written first ─────────────────────────
        with open(file_path, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")

        log(f"[{label}] Flushing {len(rows)} events → {file_path.name}")

        # ── 2. PRIMARY: Pub/Sub ───────────────────────────────────────────
        pubsub_ok = await self._publish_to_pubsub(rows, label)

        # ── 3. FALLBACK: BQ load job (only if Pub/Sub failed) ────────────
        if not pubsub_ok:
            log(f"[{label}] 🔄 Falling back to BQ load job...")
            await self._load_to_bq(file_path, rows, label)

    async def _publish_to_pubsub(self, rows: list[dict], label: str) -> bool:
        """
        Publish rows to Pub/Sub topic. Returns True on success, False on failure.
        Each message carries event_type and session_id as Pub/Sub attributes
        so the BQ subscription (and any future subscribers) can filter cheaply.
        """
        futures = []
        try:
            for row in rows:
                data  = json.dumps(row).encode("utf-8")
                attrs = {
                    "event_type": row["event_type"],
                    "session_id": self.session_id,
                }
                futures.append(publisher.publish(TOPIC_PATH, data=data, **attrs))

            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._resolve_futures, futures)

            self.total_published += len(rows)
            log(f"[{label}] ✅ [PRIMARY]  Published {len(rows)} msgs to {TOPIC_ID} "
                f"(session total: {self.total_published})")
            return True

        except Exception as e:
            log(f"[{label}] ❌ [PRIMARY]  Pub/Sub failed: {type(e).__name__}: {e}")
            return False

    async def _load_to_bq(self, file_path: Path, rows: list[dict], label: str):
        """
        BQ load job from the already-written JSONL file.
        Mirrors the original batch logic exactly — free-tier safe, no streaming inserts.
        """
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=SCHEMA,
        )
        try:
            with open(file_path, "rb") as f:
                load_job = bq_client.load_table_from_file(
                    f, FULL_TABLE_ID, job_config=job_config
                )
            await asyncio.get_running_loop().run_in_executor(None, load_job.result)
            self.total_published += len(rows)
            log(f"[{label}] ✅ [FALLBACK] BQ load job: {len(rows)} rows → {FULL_TABLE_ID} "
                f"(session total: {self.total_published})")
        except Exception as e:
            log(f"[{label}] ❌ [FALLBACK] BQ load job also failed: {e}")
            log(f"[{label}]    ⚠️  Data is safe locally: {file_path}")
            log(f"[{label}]    Re-publish manually: scripts/replay_jsonl.py {file_path}")

    @staticmethod
    def _resolve_futures(futures):
        """Block on all publish futures; raises on first error."""
        for f in futures:
            f.result()   # raises google.api_core.exceptions.* on failure

    # ── Periodic flush task ───────────────────────────────────────────────────
    async def flush_loop(self, stop: asyncio.Event):
        while not stop.is_set():
            await asyncio.sleep(FLUSH_INTERVAL_S)
            if not stop.is_set():
                await self.flush(label="30s-flush")

    # ── Report ────────────────────────────────────────────────────────────────
    def build_report(self) -> tuple[dict, str]:
        ended_at     = datetime.now(timezone.utc)
        duration_s   = int((ended_at - self.started_at).total_seconds())
        duration_fmt = f"{duration_s // 60}m {duration_s % 60}s"

        ev = self._all_events
        viewer_ev  = [e for e in ev if e["event_type"] == "viewer_count"]
        like_ev    = [e for e in ev if e["event_type"] == "like"]
        comment_ev = [e for e in ev if e["event_type"] == "comment"]
        follow_ev  = [e for e in ev if e["event_type"] == "follow"]
        share_ev   = [e for e in ev if e["event_type"] == "share"]
        gift_ev    = [e for e in ev if e["event_type"] == "gift"]

        peak_viewers  = max((e.get("viewer_count", 0) for e in viewer_ev), default=0)
        avg_viewers   = (
            int(sum(e.get("viewer_count", 0) for e in viewer_ev) / len(viewer_ev))
            if viewer_ev else 0
        )
        diamond_total = sum(e.get("diamond_count", 0) or 0 for e in gift_ev)

        top_commenters  = Counter(e.get("username", "unknown") for e in comment_ev).most_common(5)
        top_likers      = Counter(e.get("username", "unknown") for e in like_ev).most_common(5)
        recent_comments = comment_ev[-8:]

        viewer_sample = viewer_ev[::5] if len(viewer_ev) > 5 else viewer_ev
        chart_labels  = json.dumps([e["event_ts"][11:16] for e in viewer_sample])
        chart_data    = json.dumps([e.get("viewer_count", 0) for e in viewer_sample])

        stats = {
            "session_id":     self.session_id,
            "session_date":   self.started_at.strftime("%d %B %Y"),
            "session_start":  self.started_at.strftime("%H:%M UTC"),
            "session_end":    ended_at.strftime("%H:%M UTC"),
            "duration":       duration_fmt,
            "peak_viewers":   peak_viewers,
            "avg_viewers":    avg_viewers,
            "total_likes":    len(like_ev),
            "total_comments": len(comment_ev),
            "total_follows":  len(follow_ev),
            "total_shares":   len(share_ev),
            "total_gifts":    len(gift_ev),
            "diamond_total":  diamond_total,
            "total_published": self.total_published,
            "reconnections":  self.reconnections,
        }

        def top_rows(items):
            if not items:
                return "<tr><td colspan='2' style='color:#888;text-align:center'>No data</td></tr>"
            medals = ["🥇", "🥈", "🥉"]
            return "".join(
                f"<tr><td>{medals[i] if i < 3 else ''} <strong>{name}</strong></td>"
                f"<td style='text-align:right;color:#f4a261'>{count}</td></tr>"
                for i, (name, count) in enumerate(items)
            )

        def comment_cards():
            if not recent_comments:
                return "<p style='color:#888'>No comments recorded.</p>"
            return "".join(
                f"""<div class='comment-card'>
                      <span class='commenter'>{e.get('username','unknown')}</span>
                      <span class='comment-time'>{e['event_ts'][11:16]}</span>
                      <p>{e.get('comment_text','')}</p>
                    </div>"""
                for e in recent_comments
            )

        reconnection_note = (
            f"<div style='background:#1a1208;border:1px solid #c9a84c;padding:12px 16px;"
            f"margin-bottom:16px;font-size:12px;color:#c9a84c;'>"
            f"⚠️  {stats['reconnections']} stream interruption(s) detected — "
            f"all data captured under this session ID.</div>"
            if stats["reconnections"] > 0 else ""
        )

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Goshen Global — Live Session Report</title>
<link href="https://fonts.googleapis.com/css2?family=Cormorant+Garamond:wght@300;400;600;700&family=DM+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
  :root {{
    --gold:#c9a84c; --gold2:#f4a261; --dark:#0d0d0d;
    --surface:#141414; --card:#1a1a1a; --border:#2a2a2a;
    --text:#e8e0d0; --muted:#888;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--dark); color:var(--text); font-family:'DM Mono',monospace; font-size:13px; line-height:1.7; }}
  .header {{ background:linear-gradient(135deg,#0d0d0d 0%,#1a1208 50%,#0d0d0d 100%); border-bottom:1px solid var(--gold); padding:48px 40px 36px; position:relative; overflow:hidden; }}
  .header::before {{ content:''; position:absolute; top:-60px; right:-60px; width:300px; height:300px; background:radial-gradient(circle,rgba(201,168,76,.08) 0%,transparent 70%); border-radius:50%; }}
  .church-name {{ font-family:'Cormorant Garamond',serif; font-size:11px; letter-spacing:.35em; text-transform:uppercase; color:var(--gold); margin-bottom:8px; }}
  .report-title {{ font-family:'Cormorant Garamond',serif; font-size:38px; font-weight:300; line-height:1.1; margin-bottom:16px; }}
  .report-title strong {{ color:var(--gold); font-weight:600; }}
  .meta-row {{ display:flex; gap:32px; flex-wrap:wrap; color:var(--muted); font-size:11px; letter-spacing:.05em; }}
  .meta-row span {{ color:var(--text); }}
  .container {{ max-width:900px; margin:0 auto; padding:40px; }}
  .section-title {{ font-family:'Cormorant Garamond',serif; font-size:11px; letter-spacing:.3em; text-transform:uppercase; color:var(--gold); margin:40px 0 16px; display:flex; align-items:center; gap:12px; }}
  .section-title::after {{ content:''; flex:1; height:1px; background:var(--border); }}
  .kpi-grid {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:1px; background:var(--border); border:1px solid var(--border); }}
  .kpi {{ background:var(--card); padding:24px 20px; }}
  .kpi.highlight {{ background:#1a1208; }}
  .kpi-label {{ font-size:9px; letter-spacing:.2em; text-transform:uppercase; color:var(--muted); margin-bottom:8px; }}
  .kpi-value {{ font-family:'Cormorant Garamond',serif; font-size:42px; font-weight:300; line-height:1; }}
  .kpi.highlight .kpi-value {{ color:var(--gold); }}
  .kpi-sub {{ font-size:10px; color:var(--muted); margin-top:4px; }}
  .chart-wrap {{ background:var(--card); border:1px solid var(--border); padding:24px; }}
  .two-col {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  @media(max-width:600px) {{ .two-col {{ grid-template-columns:1fr; }} }}
  .data-card {{ background:var(--card); border:1px solid var(--border); padding:20px; }}
  .data-card h4 {{ font-size:9px; letter-spacing:.2em; text-transform:uppercase; color:var(--muted); margin-bottom:14px; }}
  table {{ width:100%; border-collapse:collapse; }}
  td {{ padding:7px 0; border-bottom:1px solid var(--border); font-size:12px; }}
  tr:last-child td {{ border-bottom:none; }}
  .comment-card {{ background:var(--card); border-left:2px solid var(--gold); padding:12px 16px; margin-bottom:8px; }}
  .commenter {{ font-size:11px; color:var(--gold2); font-weight:500; margin-right:12px; }}
  .comment-time {{ font-size:10px; color:var(--muted); }}
  .comment-card p {{ margin-top:4px; color:var(--text); font-size:12px; }}
  .footer {{ margin-top:48px; padding-top:24px; border-top:1px solid var(--border); color:var(--muted); font-size:10px; letter-spacing:.05em; display:flex; justify-content:space-between; flex-wrap:wrap; gap:8px; }}
  .footer strong {{ color:var(--gold); }}
</style>
</head>
<body>
<div class="header">
  <div class="church-name">Goshen Global Church</div>
  <h1 class="report-title">TikTok Live<br><strong>Session Report</strong></h1>
  <div class="meta-row">
    <div>Date &nbsp;<span>{stats['session_date']}</span></div>
    <div>Start &nbsp;<span>{stats['session_start']}</span></div>
    <div>End &nbsp;<span>{stats['session_end']}</span></div>
    <div>Duration &nbsp;<span>{stats['duration']}</span></div>
    <div>Session &nbsp;<span style="font-size:10px;letter-spacing:0">{stats['session_id'][:12]}…</span></div>
  </div>
</div>
<div class="container">
  {reconnection_note}
  <div class="section-title">Reach &amp; Engagement</div>
  <div class="kpi-grid">
    <div class="kpi highlight">
      <div class="kpi-label">Peak Viewers</div>
      <div class="kpi-value">{stats['peak_viewers']}</div>
      <div class="kpi-sub">avg {stats['avg_viewers']} throughout</div>
    </div>
    <div class="kpi"><div class="kpi-label">Total Likes</div><div class="kpi-value">{stats['total_likes']}</div></div>
    <div class="kpi"><div class="kpi-label">Comments</div><div class="kpi-value">{stats['total_comments']}</div></div>
    <div class="kpi"><div class="kpi-label">New Follows</div><div class="kpi-value">{stats['total_follows']}</div></div>
    <div class="kpi"><div class="kpi-label">Shares</div><div class="kpi-value">{stats['total_shares']}</div></div>
    <div class="kpi">
      <div class="kpi-label">Gifts / Diamonds</div>
      <div class="kpi-value">{stats['total_gifts']}</div>
      <div class="kpi-sub">{stats['diamond_total']} diamonds</div>
    </div>
    <div class="kpi">
      <div class="kpi-label">Reconnections</div>
      <div class="kpi-value">{stats['reconnections']}</div>
      <div class="kpi-sub">stream interruptions</div>
    </div>
  </div>
  <div class="section-title">Viewer Curve</div>
  <div class="chart-wrap"><canvas id="viewerChart" height="80"></canvas></div>
  <div class="section-title">Top Engagers</div>
  <div class="two-col">
    <div class="data-card"><h4>Most Active Commenters</h4><table>{top_rows(top_commenters)}</table></div>
    <div class="data-card"><h4>Most Active Likers</h4><table>{top_rows(top_likers)}</table></div>
  </div>
  <div class="section-title">Recent Comments</div>
  {comment_cards()}
  <div class="footer">
    <div>Generated by <strong>Goshen Analytics Pipeline</strong> — Proud Ndlovu</div>
    <div>{stats['total_published']} events published via Pub/Sub · Session {stats['session_id'][:8]}</div>
  </div>
</div>
<script>
new Chart(document.getElementById('viewerChart').getContext('2d'), {{
  type: 'line',
  data: {{
    labels: {chart_labels},
    datasets: [{{
      label: 'Viewers', data: {chart_data},
      borderColor: '#c9a84c', backgroundColor: 'rgba(201,168,76,0.08)',
      borderWidth: 2, pointRadius: 3, pointBackgroundColor: '#c9a84c',
      fill: true, tension: 0.4,
    }}]
  }},
  options: {{
    responsive: true,
    plugins: {{ legend: {{ display: false }} }},
    scales: {{
      x: {{ ticks: {{ color:'#888', font:{{ family:'DM Mono', size:10 }} }}, grid: {{ color:'#222' }} }},
      y: {{ ticks: {{ color:'#888', font:{{ family:'DM Mono', size:10 }} }}, grid: {{ color:'#222' }}, beginAtZero: false }}
    }}
  }}
}});
</script>
</body>
</html>"""

        return stats, html

    async def save_and_email_report(self):
        log("📊 Building session report...")
        stats, html = self.build_report()

        report_path = self.folder / f"{self.session_id}_report.html"
        report_path.write_text(html, encoding="utf-8")
        log(f"💾 Report saved → {report_path}")

        subject = (
            f"Goshen Global — TikTok Live · "
            f"{stats['session_date']} · "
            f"{stats['peak_viewers']} peak viewers"
        )
        plain = (
            f"Goshen Global Church — TikTok Live Session Report\n"
            f"{'─' * 50}\n"
            f"Date         : {stats['session_date']}\n"
            f"Duration     : {stats['duration']}\n"
            f"Peak Viewers : {stats['peak_viewers']} (avg {stats['avg_viewers']})\n"
            f"Likes        : {stats['total_likes']}\n"
            f"Comments     : {stats['total_comments']}\n"
            f"New Follows  : {stats['total_follows']}\n"
            f"Shares       : {stats['total_shares']}\n"
            f"Gifts        : {stats['total_gifts']} ({stats['diamond_total']} diamonds)\n"
            f"Reconnections: {stats['reconnections']}\n"
            f"{'─' * 50}\n"
            f"Full report attached — open in any browser.\n"
            f"Session ID   : {stats['session_id']}\n"
            f"Pub/Sub msgs : {stats['total_published']}\n"
            f"\n— Goshen Analytics Pipeline"
        )

        msg = MIMEMultipart("mixed")
        msg["Subject"] = subject
        msg["From"]    = EMAIL_SENDER
        msg["To"]      = ", ".join(EMAIL_RECIPIENTS)

        alt = MIMEMultipart("alternative")
        alt.attach(MIMEText(plain, "plain"))
        alt.attach(MIMEText(html,  "html"))
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
            log(f"📧 Report emailed to: {', '.join(EMAIL_RECIPIENTS)}")
        except Exception as e:
            log(f"❌ Email failed: {e}")
            log(f"📂 Report available locally: {report_path}")

    def _send_email(self, context, msg):
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())


# ══════════════════════════════════════════════════════════════════════════════
# CLIENT FACTORY
# ══════════════════════════════════════════════════════════════════════════════
def build_client(session: LiveSession) -> TikTokLiveClient:
    client = TikTokLiveClient(unique_id=TARGET_USER)

    @client.on(ConnectEvent)
    async def on_connect(event: ConnectEvent):
        log(f"🟢 Connected to {TARGET_USER} | session: {session.session_id}")
        log(f"📁 Session folder: {session.folder}")
        await session.add("connect", room_id=str(event.room_id))

    @client.on(RoomUserSeqEvent)
    async def on_viewers(event: RoomUserSeqEvent):
        count = event.total_user
        log(f"👁️  Viewers: {count}")
        await session.add("viewer_count", viewer_count=count)

    @client.on(LikeEvent)
    async def on_like(event: LikeEvent):
        user = event.user.unique_id if event.user else "unknown"
        log(f"❤️  Like from: {event.user.nickname if event.user else user}")
        await session.add(
            "like",
            user_id=str(event.user.id) if event.user else None,
            username=user,
            like_count=getattr(event, "count", 1),
        )

    @client.on(CommentEvent)
    async def on_comment(event: CommentEvent):
        user    = event.user.unique_id if event.user else "unknown"
        comment = event.comment or ""
        log(f"💬 {event.user.nickname if event.user else user}: {comment}")
        await session.add(
            "comment",
            user_id=str(event.user.id) if event.user else None,
            username=user,
            comment_text=comment,
        )

    @client.on(FollowEvent)
    async def on_follow(event: FollowEvent):
        user = event.user.unique_id if event.user else "unknown"
        log(f"➕ Follow: {user}")
        await session.add(
            "follow",
            user_id=str(event.user.id) if event.user else None,
            username=user,
        )

    @client.on(ShareEvent)
    async def on_share(event: ShareEvent):
        user = event.user.unique_id if event.user else "unknown"
        log(f"🔗 Share: {user}")
        await session.add(
            "share",
            user_id=str(event.user.id) if event.user else None,
            username=user,
        )

    @client.on(GiftEvent)
    async def on_gift(event: GiftEvent):
        user     = event.user.unique_id if event.user else "unknown"
        gift     = event.gift.name if hasattr(event, "gift") else "unknown"
        diamonds = event.gift.diamond_count if hasattr(event, "gift") else 0
        repeats  = getattr(event.gift, "repeat_count", 1) if hasattr(event, "gift") else 1
        log(f"🎁 Gift from {user}: {gift} x{repeats} ({diamonds} 💎)")
        await session.add(
            "gift",
            user_id=str(event.user.id) if event.user else None,
            username=user,
            gift_name=gift,
            diamond_count=diamonds * repeats,
        )

    @client.on(LiveEndEvent)
    async def on_live_end(event: LiveEndEvent):
        log("🔴 Stream ended (LiveEndEvent)")
        await session.add("live_end")

    @client.on(DisconnectEvent)
    async def on_disconnect(event: DisconnectEvent):
        session.reconnections += 1
        log(f"🔌 Disconnected (reconnection #{session.reconnections})")

    return client


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════
async def main():
    log("═" * 60)
    log("  GOSHEN ANALYTICS — TikTok Live Collector")
    log(f"  Account    : {TARGET_USER}")
    log(f"  PRIMARY    : Pub/Sub → {TOPIC_PATH}")
    log(f"  FALLBACK   : BQ load job → {FULL_TABLE_ID}")
    log(f"  Flush      : every {FLUSH_INTERVAL_S}s  |  Poll: every {POLL_INTERVAL_S}s")
    log("═" * 60)

    ensure_fallback_table_exists()

    session    = LiveSession()
    stop_flush = asyncio.Event()
    flush_task = asyncio.create_task(session.flush_loop(stop_flush))

    log(f"👀 Monitoring {TARGET_USER} — checking every {POLL_INTERVAL_S}s until live...")

    try:
        while True:
            client = build_client(session)
            try:
                log(f"🔎 Connecting... (session: {session.session_id})")
                await client.connect()
                log("✅ Stream ended cleanly — wrapping up session.")
                break

            except UserOfflineError:
                log(f"Not live — retrying in {POLL_INTERVAL_S}s...")
                await asyncio.sleep(POLL_INTERVAL_S)

            except Exception as e:
                log(f"⚠️  Connection lost: {type(e).__name__}: {e} — reconnecting in {POLL_INTERVAL_S}s...")
                await asyncio.sleep(POLL_INTERVAL_S)

            finally:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    finally:
        stop_flush.set()
        await flush_task
        await session.flush(label="final-flush")
        await session.save_and_email_report()

        log("═" * 60)
        log(f"  SESSION COMPLETE")
        log(f"  ID              : {session.session_id}")
        log(f"  Folder          : {session.folder.resolve()}")
        log(f"  Pub/Sub msgs    : {session.total_published}")
        log(f"  Reconnections   : {session.reconnections}")
        log("═" * 60)


if __name__ == "__main__":
    asyncio.run(main())