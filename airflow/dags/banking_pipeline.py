"""Airflow DAG for the reproducible local portfolio pipeline."""
from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/projects/enterprise-banking-data-platform"

with DAG(
    dag_id="enterprise_banking_data_platform",
    description="Generate, standardize, reconcile, and publish synthetic banking data",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "data-engineering", "retries": 2},
    tags=["portfolio", "banking", "aml", "regulatory"],
) as dag:
    generate = BashOperator(
        task_id="generate_sources",
        bash_command=f"cd {PROJECT_DIR} && PYTHONPATH=src python -m banking_platform.generator --output data/raw",
    )
    transform = BashOperator(
        task_id="standardize_and_publish",
        bash_command=f"cd {PROJECT_DIR} && PYTHONPATH=src python -m banking_platform.local_pipeline --input data/raw --output data/processed",
    )
    assert_quality = BashOperator(
        task_id="assert_quality_gate",
        bash_command=(f"cd {PROJECT_DIR} && python -c \"import json; "
                      "r=json.load(open('data/processed/metrics/quality_report.json')); "
                      "assert r['status']=='PASS', r\""),
    )
    generate >> transform >> assert_quality
