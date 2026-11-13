"""Optional Airflow control-plane trigger for an existing Databricks bundle job.

Install: apache-airflow-providers-databricks. Supply connection and job ID
outside source control. Prevent parallel runs at the DAG level; the Databricks
job also enforces max_concurrent_runs=1.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

JOB_ID = os.environ.get("CAUSALMESH_DATABRICKS_JOB_ID")

with DAG(
    dag_id="causalmesh_cloud_lakehouse",
    start_date=datetime(2026, 11, 1, tzinfo=timezone.utc),
    schedule="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "data-platform", "retries": 2,
                  "retry_delay": timedelta(minutes=5)},
    tags=["causalmesh", "databricks", "settlements"],
    doc_md="Trigger the already deployed CausalMesh Databricks batch job."
) as dag:
    if JOB_ID:
        run = DatabricksRunNowOperator(
            task_id="run_settlement_reconciliation",
            databricks_conn_id=os.environ.get("CAUSALMESH_DATABRICKS_CONN_ID", "databricks_default"),
            job_id=int(JOB_ID),
            polling_period_seconds=30,
        )
    else:
        # Fail explicit config validation during DAG parsing instead of triggering
        # an accidental remote job in an unknown account.
        from airflow.operators.python import PythonOperator

        def missing_job_configuration():
            raise RuntimeError("CAUSALMESH_DATABRICKS_JOB_ID must be configured")

        run = PythonOperator(task_id="configure_job_id_before_enabling",
                             python_callable=missing_job_configuration)
