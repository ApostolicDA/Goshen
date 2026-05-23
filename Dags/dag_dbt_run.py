"""
DAG: dag_dbt_run
Description: Runs dbt Core models and tests against Snowflake.
             Executes staging models first, then mart models.
             Runs dbt test after each layer to catch issues early.
Schedule: Daily at 00:30 UTC (02:30 SAST) — runs after all ingestion DAGs
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
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DBT_PROJECT_DIR = "/opt/airflow/goshen"
DBT_PROFILES_DIR = "/opt/airflow/goshen"

# ── DAG Definition ─────────────────────────────────────────────────────────
with DAG(
    dag_id="dag_dbt_run",
    default_args=default_args,
    description="dbt Core staging + mart models and tests",
    schedule_interval="30 0 * * *",  # 00:30 UTC = 02:30 SAST
    start_date=days_ago(1),
    catchup=False,
    tags=["goshen", "dbt", "transformation"],
) as dag:

    # ── Task 1: dbt debug — verify connection ──────────────────────────────
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt debug --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 2: dbt deps — install packages ───────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt deps --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 3: Run staging models ────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --select staging \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 4: Test staging models ───────────────────────────────────────
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --select staging \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 5: Run mart models ───────────────────────────────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt run --select marts \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 6: Test mart models ──────────────────────────────────────────
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt test --select marts \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 7: Generate dbt docs ─────────────────────────────────────────
    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"""
            cd {DBT_PROJECT_DIR} && \
            dbt docs generate \
            --profiles-dir {DBT_PROFILES_DIR}
        """,
    )

    # ── Task 8: Log completion ─────────────────────────────────────────────
    log_completion = BashOperator(
        task_id="log_dbt_completion",
        bash_command='echo "dbt run completed successfully at $(date)"',
    )

    # ── Dependencies ───────────────────────────────────────────────────────
    (
        dbt_debug
        >> dbt_deps
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
        >> dbt_docs_generate
        >> log_completion
    )
