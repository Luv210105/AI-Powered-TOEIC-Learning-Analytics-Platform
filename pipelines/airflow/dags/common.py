"""
common.py — helper dùng chung cho DAG DP1/DP2/DP3.

Config lấy TẬP TRUNG từ Airflow (không hardcode trong DAG):
  - Variable   `spark_master`   : spark://spark-master:7077
  - Variable   `minio_endpoint` : http://minio:9000
  - Connection `postgres_gold`  : host/port/db/user/pass của Postgres Gold
  - MinIO credentials: biến môi trường MINIO_ROOT_USER/PASSWORD (đặt trong service airflow)

- spark_submit(job): lệnh `docker exec` vào spark-master chạy spark-submit 1 job Spark.
- validate_python(script): chạy validate script Python NGAY trong Airflow (nối postgres:5432).
"""

import os

from airflow.hooks.base import BaseHook
from airflow.models import Variable

SPARK_CONTAINER = "toeic-spark-master"
PACKAGES = ("io.delta:delta-spark_2.12:3.2.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "org.postgresql:postgresql:42.7.4")


def spark_submit(job: str) -> str:
    """Trả về lệnh bash submit 1 job Spark vào cluster (config từ Airflow Variable/Connection)."""
    spark_master = Variable.get("spark_master")
    minio_endpoint = Variable.get("minio_endpoint")
    pg = BaseHook.get_connection("postgres_gold")
    minio_user = os.environ["MINIO_ROOT_USER"]
    minio_pass = os.environ["MINIO_ROOT_PASSWORD"]
    return (
        f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
        f"--master {spark_master} "
        f"--packages {PACKAGES} "
        f"--conf spark.jars.ivy=/tmp/.ivy2 "
        f"--conf spark.hadoop.fs.s3a.endpoint={minio_endpoint} "
        f"--conf spark.hadoop.fs.s3a.access.key={minio_user} "
        f"--conf spark.hadoop.fs.s3a.secret.key={minio_pass} "
        f"--conf spark.hadoop.fs.s3a.path.style.access=true "
        f"--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
        f"--conf spark.hadoop.fs.s3a.aws.credentials.provider="
        f"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider "
        f"--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
        f"--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
        f"--conf spark.pg.url=jdbc:postgresql://{pg.host}:{pg.port}/{pg.schema} "
        f"--conf spark.pg.user={pg.login} "
        f"--conf spark.pg.password={pg.password} "
        f"/opt/spark-apps/{job}"
    )


def validate_python(script: str) -> str:
    """Trả về lệnh bash chạy validate script Python trong Airflow (nối postgres:5432)."""
    return f"python /opt/airflow/repo/processing/{script}"
