"""
dp1_ingest_bronze.py — DAG DP1: landing -> Bronze (Delta trên MinIO).

2 stage: ingest_bronze (Spark ingest raw -> Bronze) -> validate_bronze (data quality check).
Trigger job Spark qua docker exec vào spark-master (xem common.py).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import spark_submit

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,   # backoff khi retry
}

with DAG(
    dag_id="dp1_ingest_bronze",
    description="DP1: landing -> Bronze (Delta) + validate",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["toeic", "dp1", "bronze"],
) as dag:
    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command=spark_submit("ingest_bronze.py"),
    )
    validate = BashOperator(
        task_id="validate_bronze",
        bash_command=spark_submit("validate_bronze.py"),
    )

    ingest >> validate
