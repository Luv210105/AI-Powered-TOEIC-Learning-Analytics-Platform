#!/usr/bin/env bash
#
# submit.sh — wrapper submit Spark job vào cluster trong Docker.
#
# Gói sẵn: --packages (Delta + hadoop-aws) và toàn bộ --conf S3A (MinIO) + Delta,
# đọc credentials từ .env. Nhờ vậy chỉ cần gõ tên job, không phải nhớ đống flag.
#
# Cách dùng (chạy ở đâu cũng được, script tự về project root):
#   bash processing/spark/submit.sh smoke_test.py
#
# Job phải nằm trong processing/spark/ (mount vào container tại /opt/spark-apps/).

set -euo pipefail

# Về project root (2 cấp trên file này) để source .env + gọi docker compose
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

JOB="${1:?Usage: bash processing/spark/submit.sh <ten_job.py>}"

# Nạp biến từ .env (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD ...)
set -a
# shellcheck disable=SC1091
source .env
set +a

# Phiên bản tương thích Spark 3.5.3 (Scala 2.12, Hadoop 3.3.4)
DELTA_PKG="io.delta:delta-spark_2.12:3.2.0"
HADOOP_AWS_PKG="org.apache.hadoop:hadoop-aws:3.3.4"

docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "${DELTA_PKG},${HADOOP_AWS_PKG}" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
  --conf spark.hadoop.fs.s3a.access.key="${MINIO_ROOT_USER}" \
  --conf spark.hadoop.fs.s3a.secret.key="${MINIO_ROOT_PASSWORD}" \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
  "/opt/spark-apps/${JOB}"
