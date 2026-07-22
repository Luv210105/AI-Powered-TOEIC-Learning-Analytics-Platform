"""
dp3_feature_offline.py — DAG DP3: feature table offline feat_user_90d (point-in-time).

2 stage: feature_offline (Spark tính rolling 90d PIT) -> validate_features (check DP3).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from common import spark_submit, validate_python

default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
}

with DAG(
    dag_id="dp3_feature_offline",
    description="DP3: feature table feat_user_90d (point-in-time) + validate",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["toeic", "dp3", "feature"],
) as dag:
    ingest = BashOperator(
        task_id="feature_offline",
        bash_command=spark_submit("feature_offline.py"),
    )
    validate = BashOperator(
        task_id="validate_features",
        bash_command=validate_python("validate_features.py"),
    )

    ingest >> validate
