"""
DAG: dag_youtube_pipeline
Description: Ingests YouTube Data API v3 data into Snowflake raw layer.
             Pulls channel stats, video metadata, and performance metrics daily.
Schedule: Daily at 23:00 UTC (01:00 SAST) — runs before master DAG
Owner: Proud Kudzai Ndlovu
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    dag_id="dag_youtube_pipeline",
    default_args=default_args,
    description="YouTube Data API v3 ingestion → Snowflake raw layer",
    schedule_interval="0 23 * * *",  # 23:00 UTC = 01:00 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "youtube", "ingestion"],
) as dag:

    # ── Task 1: Validate YouTube API credentials ───────────────────────────
    validate_credentials = BashOperator(
        task_id="validate_youtube_credentials",
        bash_command="""
            python -c "
import os
from dotenv import load_dotenv
load_dotenv()
api_key = os.getenv('YOUTUBE_API_KEY')
channel_id = os.getenv('YOUTUBE_CHANNEL_ID')
assert api_key, 'YOUTUBE_API_KEY not found in environment'
assert channel_id, 'YOUTUBE_CHANNEL_ID not found in environment'
print('YouTube credentials validated successfully')
"
        """,
    )

    # ── Task 2: Ingest YouTube channel stats ──────────────────────────────
    ingest_channel_stats = BashOperator(
        task_id="ingest_youtube_channel_stats",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.youtube_channel import run_youtube_channel_ingestion
run_youtube_channel_ingestion()
print('YouTube channel stats ingestion complete')
"
        """,
    )

    # ── Task 3: Ingest YouTube videos ─────────────────────────────────────
    ingest_videos = BashOperator(
        task_id="ingest_youtube_videos",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.youtube_videos import run_youtube_videos_ingestion
run_youtube_videos_ingestion()
print('YouTube videos ingestion complete')
"
        """,
    )

    # ── Task 4: Ingest YouTube comments ───────────────────────────────────
    ingest_comments = BashOperator(
        task_id="ingest_youtube_comments",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.youtube_comments import run_youtube_comments_ingestion
run_youtube_comments_ingestion()
print('YouTube comments ingestion complete')
"
        """,
    )

    # ── Task 5: Validate row counts ───────────────────────────────────────
    validate_row_counts = BashOperator(
        task_id="validate_youtube_row_counts",
        bash_command="""
            python -c "
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()

conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema='RAW_YOUTUBE'
)

cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM raw_youtube_videos')
count = cursor.fetchone()[0]
assert count > 0, f'No rows found in raw_youtube_videos — ingestion may have failed'
print(f'Validation passed: {count} rows in raw_youtube_videos')
conn.close()
"
        """,
    )

    # ── Task 6: Log completion ─────────────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_youtube_completion",
        bash_command='echo "YouTube pipeline completed successfully at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        validate_credentials
        >> ingest_channel_stats
        >> [ingest_videos, ingest_comments]
        >> validate_row_counts
        >> log_completion
    )
