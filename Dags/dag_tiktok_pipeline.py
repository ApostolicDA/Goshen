"""
DAG: dag_tiktok_pipeline
Description: Ingests TikTok data into BigQuery raw layer.
             Processes TikTok data export .txt files
             (live history, live comments, posts, watch history, followers).
Schedule: Daily at 23:00 UTC (01:00 SAST) — runs before master DAG
Owner: Proud Kudzai Ndlovu
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# ── Default Arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "proud_ndlovu",
    "depends_on_past": False,
    "email": ["fanisaproud@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_tiktok_pipeline",
    default_args=default_args,
    description="TikTok .txt export ingestion → BigQuery raw layer",
    schedule_interval="0 23 * * *",  # 23:00 UTC = 01:00 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "tiktok", "ingestion"],
) as dag:

    # ── Task 1: Check TikTok files exist ──────────────────────────────────
    # Files are mounted into the container via docker-compose volume
    check_files_exist = BashOperator(
        task_id="check_tiktok_files_exist",
        bash_command="""
            python -c "
import os

folder = os.getenv('TIKTOK_FOLDER', '/app/data/tiktok')
required = [
    'Go_LIVE_History.txt',
    'LiveStream_Comment.txt',
    'Posts.txt',
    'Watch_LIVE_History.txt',
    'Follower.txt',
]

missing = [f for f in required if not os.path.exists(os.path.join(folder, f))]
if missing:
    print(f'⚠️  Missing TikTok files: {missing}')
else:
    print(f'✅ All TikTok files found in {folder}')
"
        """,
    )

    # ── Task 2: Run TikTok ingestion ──────────────────────────────────────
    ingest_tiktok = BashOperator(
        task_id="ingest_tiktok",
        bash_command="cd /app && python ingestion/tiktok_ingestion.py",
    )

    # ── Task 3: Validate rows landed in BigQuery ──────────────────────────
    validate_bq_rows = BashOperator(
        task_id="validate_tiktok_bq_rows",
        bash_command="""
            python -c "
from google.cloud import bigquery
import os

project = os.getenv('GCP_PROJECT_ID', 'goshen-analytics')
dataset = os.getenv('BQ_DATASET', 'analytics')

client = bigquery.Client()

tables = [
    'tiktok_live_history',
    'tiktok_posts',
    'tiktok_followers',
]

for table in tables:
    full = f'{project}.{dataset}.{table}'
    result = client.query(f'SELECT COUNT(*) as cnt FROM \`{full}\`').result()
    count = list(result)[0].cnt
    assert count > 0, f'No rows in {full}'
    print(f'✅ {full}: {count} rows')
"
        """,
    )

    # ── Task 4: Log completion ─────────────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_tiktok_completion",
        bash_command='echo "✅ TikTok pipeline completed at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    check_files_exist >> ingest_tiktok >> validate_bq_rows >> log_completion
