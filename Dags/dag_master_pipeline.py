"""
DAG: dag_master_pipeline
Description: Master orchestration DAG for the Goshen Analytics Platform.
             Triggers all platform ingestion DAGs in parallel, then runs
             dbt transformations and tests once all ingestion is complete.
             Acts as the single source of truth for pipeline health.

Pipeline Architecture:
    ┌─────────────────────────────────────────────────┐
    │              dag_master_pipeline                │
    │                                                 │
    │   ┌─────────────┐  ┌──────────────┐            │
    │   │  Facebook   │  │   YouTube    │  ┌────────┐ │
    │   │  Ingestion  │  │  Ingestion   │  │TikTok  │ │
    │   └──────┬──────┘  └──────┬───────┘  │Ingest  │ │
    │          │                │          └───┬────┘ │
    │          └────────────────┼──────────────┘      │
    │                           ▼                     │
    │                  ┌────────────────┐             │
    │                  │   dbt run +    │             │
    │                  │   dbt test     │             │
    │                  └────────────────┘             │
    └─────────────────────────────────────────────────┘

Schedule: Daily at 00:00 UTC (02:00 SAST)
Owner: Proud Kudzai Ndlovu
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

# ── Default Arguments ──────────────────────────────────────────────────────
default_args = {
    "owner": "proud_ndlovu",
    "depends_on_past": False,
    "email": ["fanisaproud@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_master_pipeline",
    default_args=default_args,
    description="Master orchestration — triggers all ingestion DAGs then dbt",
    schedule_interval="0 0 * * *",  # 00:00 UTC = 02:00 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "master", "orchestration"],
) as dag:

    # ── Task 1: Pipeline health check ─────────────────────────────────────
    pipeline_health_check = BashOperator(
        task_id="pipeline_health_check",
        bash_command="""
            python -c "
import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()

print('Running pipeline health check...')

# Check Snowflake connection
conn = snowflake.connector.connect(
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
)
cursor = conn.cursor()
cursor.execute('SELECT CURRENT_TIMESTAMP()')
ts = cursor.fetchone()[0]
print(f'Snowflake connection OK — server time: {ts}')
conn.close()

print('Health check passed — starting pipeline')
"
        """,
    )

    # ── Task 2: Trigger Facebook ingestion DAG ─────────────────────────────
    trigger_facebook = TriggerDagRunOperator(
        task_id="trigger_facebook_pipeline",
        trigger_dag_id="dag_facebook_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # ── Task 3: Trigger YouTube ingestion DAG ──────────────────────────────
    trigger_youtube = TriggerDagRunOperator(
        task_id="trigger_youtube_pipeline",
        trigger_dag_id="dag_youtube_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # ── Task 4: Trigger TikTok ingestion DAG ───────────────────────────────
    trigger_tiktok = TriggerDagRunOperator(
        task_id="trigger_tiktok_pipeline",
        trigger_dag_id="dag_tiktok_pipeline",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # ── Task 5: Trigger dbt DAG ────────────────────────────────────────────
    trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_run",
        trigger_dag_id="dag_dbt_run",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "upstream_failed"],
    )

    # ── Task 6: Pipeline summary ───────────────────────────────────────────
    pipeline_summary = BashOperator(
        task_id="pipeline_summary",
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
    schema='ANALYTICS_MARTS'
)

cursor = conn.cursor()

# Print summary of key mart row counts
summary = {}
for mart in [
    'mart_facebook_insights',
    'mart_youtube_videos',
    'mart_tiktok_live_perfomance',
    'mart_posts_perfomance',
    'mart_social_overview'
]:
    cursor.execute(f'SELECT COUNT(*) FROM {mart}')
    summary[mart] = cursor.fetchone()[0]

print('=== GOSHEN PIPELINE SUMMARY ===')
for mart, count in summary.items():
    print(f'{mart}: {count} rows')
print('================================')
conn.close()
"
        """,
    )

    # ── Task 7: Log master completion ─────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_master_completion",
        bash_command='echo "Goshen master pipeline completed successfully at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        pipeline_health_check
        >> [trigger_facebook, trigger_youtube, trigger_tiktok]  # run in parallel
        >> trigger_dbt                                           # run after all ingestion
        >> pipeline_summary
        >> log_completion
    )
