from __future__ import annotations

from datetime import datetime, timedelta
import subprocess

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


PROJECT_DIR = "/opt/airflow/projects/salesforce-snowflake-revenue-pipeline"


def run_command(command: list[str]) -> None:
    subprocess.run(command, cwd=PROJECT_DIR, check=True)


def extract_salesforce() -> None:
    run_command(["python", "-m", "src.pipeline", "--mode", "salesforce", "--output", "data/out"])


def load_snowflake() -> None:
    run_command(["python", "-m", "src.snowflake_loader", "--input", "data/out"])


default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="salesforce_to_snowflake_revenue_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    default_args=default_args,
    tags=["salesforce", "snowflake", "elt"],
) as dag:
    extract = PythonOperator(task_id="extract_salesforce", python_callable=extract_salesforce)
    load = PythonOperator(task_id="load_raw_snowflake", python_callable=load_snowflake)

    stage = SQLExecuteQueryOperator(
        task_id="build_staging",
        conn_id="snowflake_default",
        sql=f"{PROJECT_DIR}/sql/10_staging.sql",
    )

    curate = SQLExecuteQueryOperator(
        task_id="build_customer360",
        conn_id="snowflake_default",
        sql=f"{PROJECT_DIR}/sql/20_customer360.sql",
    )

    quality = SQLExecuteQueryOperator(
        task_id="run_quality_checks",
        conn_id="snowflake_default",
        sql=f"{PROJECT_DIR}/sql/30_quality_checks.sql",
    )

    extract >> load >> stage >> curate >> quality
