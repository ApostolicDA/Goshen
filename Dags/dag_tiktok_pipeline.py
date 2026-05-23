"""
DAG: dag_tiktok_pipeline
Description: Ingests TikTok data into Snowflake raw layer.
             Processes weekly CSV exports (posts, live sessions, followers)
             and TikTok API data where available.
Schedule: Daily at 23:00 UTC (01:00 SAST) — runs before master DAG
Owner: Proud Kudzai Ndlovu

Note: TikTok does not provide a full real-time API for all metrics.
      Post and live session data is ingested via weekly CSV exports
      dropped into a designated folder, picked up by this DAG daily.
      Follower data is pulled via TikTok API where available.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
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
    description="TikTok CSV + API ingestion → Snowflake raw layer",
    schedule_interval="0 23 * * *",  # 23:00 UTC = 01:00 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "tiktok", "ingestion"],
) as dag:

    # ── Task 1: Check for new TikTok CSV exports ───────────────────────────
    # TikTok weekly CSVs are manually downloaded and dropped into /data/tiktok_exports/
    check_csv_exists = BashOperator(
        task_id="check_tiktok_csv_exists",
        bash_command="""
            python -c "
import os
import glob
csv_path = '/opt/airflow/goshen/data/tiktok_exports/'
files = glob.glob(csv_path + '*.csv')
if files:
    print(f'Found {len(files)} TikTok CSV file(s): {files}')
else:
    print('No new TikTok CSV files found — skipping CSV ingestion')
"
        """,
    )

    # ── Task 2: Ingest TikTok posts CSV ───────────────────────────────────
    ingest_tiktok_posts = BashOperator(
        task_id="ingest_tiktok_posts",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.tiktok_posts import run_tiktok_posts_ingestion
run_tiktok_posts_ingestion()
print('TikTok posts ingestion complete')
"
        """,
    )

    # ── Task 3: Ingest TikTok live sessions CSV ───────────────────────────
    ingest_tiktok_live = BashOperator(
        task_id="ingest_tiktok_live",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.tiktok_live import run_tiktok_live_ingestion
run_tiktok_live_ingestion()
print('TikTok live sessions ingestion complete')
"
        """,
    )

    # ── Task 4: Ingest TikTok followers ───────────────────────────────────
    ingest_tiktok_followers = BashOperator(
        task_id="ingest_tiktok_followers",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.tiktok_followers import run_tiktok_followers_ingestion
run_tiktok_followers_ingestion()
print('TikTok followers ingestion complete')
"
        """,
    )

    # ── Task 5: Archive processed CSVs ────────────────────────────────────
    archive_csvs = BashOperator(
        task_id="archive_tiktok_csvs",
        bash_command="""
            python -c "
import os
import glob
import shutil
from datetime import datetime

csv_path = '/opt/airflow/goshen/data/tiktok_exports/'
archive_path = '/opt/airflow/goshen/data/tiktok_archive/'
os.makedirs(archive_path, exist_ok=True)

files = glob.glob(csv_path + '*.csv')
for f in files:
    dest = archive_path + datetime.now().strftime('%Y%m%d_') + os.path.basename(f)
    shutil.move(f, dest)
    print(f'Archived: {f} → {dest}')

print(f'Archived {len(files)} CSV file(s)')
"
        """,
    )

    # ── Task 6: Validate row counts ───────────────────────────────────────
    validate_row_counts = BashOperator(
        task_id="validate_tiktok_row_counts",
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
    schema='RAW_TIKTOK'
)

cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM raw_tiktok_posts')
count = cursor.fetchone()[0]
assert count > 0, 'No rows in raw_tiktok_posts'
print(f'Validation passed: {count} rows in raw_tiktok_posts')
conn.close()
"
        """,
    )

    # ── Task 7: Log completion ─────────────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_tiktok_completion",
        bash_command='echo "TikTok pipeline completed successfully at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        check_csv_exists
        >> [ingest_tiktok_posts, ingest_tiktok_live, ingest_tiktok_followers]
        >> archive_csvs
        >> validate_row_counts
        >> log_completion
    )
