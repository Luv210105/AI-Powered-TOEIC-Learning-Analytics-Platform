"""
ingest_bronze.py — Phase 5.3 (DP1 ingest stage): landing Parquet -> Bronze Delta.

Đọc 7 bảng từ s3a://landing/, thêm cột metadata (ingest_ts, source, batch_id),
ghi ra Delta tại s3a://bronze/raw_<table>/. GIỮ NGUYÊN lỗi (KHÔNG dedup, KHÔNG sửa) — Bronze là raw.

Schema evolution: attempt_answers đọc bằng mergeSchema=true -> gộp 2 phiên bản schema; các
dòng partition cũ (trước schema_change_date) sẽ có difficulty/tag_grammar = NULL. Cả 2 phiên bản
được ingest vào cùng bảng Bronze mà không lỗi.

Submit: bash processing/spark/submit.sh ingest_bronze.py
"""

import uuid

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

LANDING = "s3a://landing"
BRONZE = "s3a://bronze"

# 7 bảng nguồn từ landing
TABLES = [
    "users", "questions", "vocabulary", "grammar_topics",
    "mock_tests", "test_attempts", "attempt_answers",
]

# Bảng có schema evolution -> cần mergeSchema khi đọc
MERGE_SCHEMA_TABLES = {"attempt_answers"}

# Bảng ghi Bronze có partition (giữ layout theo ngày như nguồn)
PARTITION_COL = {"attempt_answers": "attempt_date"}


def main() -> None:
    """Ingest 7 bảng landing -> Bronze Delta, thêm metadata, giữ nguyên lỗi."""
    spark = SparkSession.builder.appName("ingest_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    batch_id = uuid.uuid4().hex[:12]     # mã lần chạy ingest (idempotency/truy vết)
    print(f">>> batch_id = {batch_id}")

    for t in TABLES:
        reader = spark.read
        if t in MERGE_SCHEMA_TABLES:
            reader = reader.option("mergeSchema", "true")
        df = reader.parquet(f"{LANDING}/{t}/")

        # Bronze = raw + 3 cột metadata ingest
        df = (df
              .withColumn("ingest_ts", F.current_timestamp())
              .withColumn("source", F.lit("minio_landing"))
              .withColumn("batch_id", F.lit(batch_id)))

        target = f"{BRONZE}/raw_{t}/"
        writer = (df.write
                  .format("delta")
                  .mode("overwrite")               # idempotent: chạy lại thì ghi đè
                  .option("mergeSchema", "true"))
        if t in PARTITION_COL:
            writer = writer.partitionBy(PARTITION_COL[t])
        writer.save(target)

        print(f"  raw_{t:<16}: {df.count():>10,} dòng -> {target}")

    print(">>> INGEST BRONZE OK: 7 bảng đã vào Bronze (Delta).")
    spark.stop()


if __name__ == "__main__":
    main()
