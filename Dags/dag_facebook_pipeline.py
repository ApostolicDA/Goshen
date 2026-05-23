"""
DAG: dag_facebook_pipeline
Description: Ingests Facebook Graph API data into Snowflake raw layer.
             Pulls page insights, post-level data, and metrics daily.
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
    dag_id="dag_facebook_pipeline",
    default_args=default_args,
    description="Facebook Graph API ingestion → Snowflake raw layer",
    schedule_interval="0 23 * * *",  # 23:00 UTC = 01:00 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "facebook", "ingestion"],
) as dag:

    # ── Task 1: Validate Facebook API credentials ──────────────────────────
    validate_credentials = BashOperator(
        task_id="validate_facebook_credentials",
        bash_command="""
            python -c "
import os
from dotenv import load_dotenv
load_dotenv()
token = os.getenv('FACEBOOK_ACCESS_TOKEN')
page_id = os.getenv('FACEBOOK_PAGE_ID')
assert token, 'FACEBOOK_ACCESS_TOKEN not found in environment'
assert page_id, 'FACEBOOK_PAGE_ID not found in environment'
print('Facebook credentials validated successfully')
"
        """,
    )

    # ── Task 2: Ingest Facebook page insights ─────────────────────────────
    ingest_facebook_insights = BashOperator(
        task_id="ingest_facebook_insights",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.facebook_insights import run_facebook_insights_ingestion
run_facebook_insights_ingestion()
print('Facebook insights ingestion complete')
"
        """,
    )

    # ── Task 3: Ingest Facebook posts ─────────────────────────────────────
    ingest_facebook_posts = BashOperator(
        task_id="ingest_facebook_posts",
        bash_command="""
            cd /opt/airflow/goshen && \
            python -c "
from ingestion.facebook_posts import run_facebook_posts_ingestion
run_facebook_posts_ingestion()
print('Facebook posts ingestion complete')
"
        """,
    )

    # ── Task 4: Validate row counts in Snowflake ──────────────────────────
    validate_row_counts = BashOperator(
        task_id="validate_facebook_row_counts",
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
    schema='RAW_FACEBOOK'
)

cursor = conn.cursor()
cursor.execute('SELECT COUNT(*) FROM raw_facebook_insights')
count = cursor.fetchone()[0]
assert count > 0, f'No rows found in raw_facebook_insights — ingestion may have failed'
print(f'Validation passed: {count} rows in raw_facebook_insights')
conn.close()
"
        """,
    )

    # ── Task 5: Log completion ─────────────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_facebook_completion",
        bash_command='echo "Facebook pipeline completed successfully at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        validate_credentials
        >> [ingest_facebook_insights, ingest_facebook_posts]
        >> validate_row_counts
        >> log_completion
    )
