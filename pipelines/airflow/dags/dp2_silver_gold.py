"""
dp2_silver_gold.py — DAG DP2: Bronze -> Silver -> Gold (PostgreSQL).

2 stage:
  - ingest (TaskGroup): silver_transform -> gold_dim -> gold_fact
  - validate: validate_gold (referential + SCD2 integrity)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from common import spark_submit, validate_python

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="dp2_silver_gold",
    description="DP2: Bronze -> Silver -> Gold (dim SCD2 + fact + OBT) + validate",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["toeic", "dp2", "gold"],
) as dag:
    with TaskGroup(group_id="ingest") as ingest:
        silver = BashOperator(task_id="silver_transform", bash_command=spark_submit("silver_transform.py"))
        gold_dim = BashOperator(task_id="gold_dim", bash_command=spark_submit("gold_dim.py"))
        gold_fact = BashOperator(task_id="gold_fact", bash_command=spark_submit("gold_fact.py"))
        silver >> gold_dim >> gold_fact

    validate = BashOperator(
        task_id="validate_gold",
        bash_command=validate_python("validate_gold.py"),
    )

    ingest >> validate
