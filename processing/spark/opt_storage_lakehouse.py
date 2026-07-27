"""
opt_storage_lakehouse.py — Phase 6.4: tối ưu lưu trữ lakehouse (Delta trên MinIO).

Chạy OPTIMIZE (compaction gom small files) + ZORDER BY question_id (data skipping) trên
stg_attempt_answers. Đo số file và thời gian query lọc theo question_id, TRƯỚC và SAU.

Submit: bash processing/spark/submit.sh opt_storage_lakehouse.py
"""

import time

from pyspark.sql import SparkSession

PATH = "s3a://silver/stg_attempt_answers"
TBL = f"delta.`{PATH}`"
Q_ID = 1234   # question_id dùng để test data skipping


def num_files(spark) -> int:
    """Số file dữ liệu hiện tại của bảng Delta (DESCRIBE DETAIL)."""
    return spark.sql(f"DESCRIBE DETAIL {TBL}").select("numFiles").collect()[0][0]


def timed_query(spark, label: str) -> float:
    """Chạy query lọc theo question_id và đo thời gian."""
    t0 = time.time()
    n = spark.sql(f"SELECT count(*) FROM {TBL} WHERE question_id = {Q_ID}").collect()[0][0]
    dt = time.time() - t0
    print(f">>> [{label}] query WHERE question_id={Q_ID}: {n:,} dòng, {dt:.2f}s")
    return dt


def main() -> None:
    """Đo baseline -> OPTIMIZE + ZORDER -> đo lại."""
    spark = SparkSession.builder.appName("opt_storage_lakehouse").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 64 + "\n BASELINE — trước OPTIMIZE\n" + "=" * 64)
    f0 = num_files(spark)
    print(f">>> Số file: {f0}")
    t_before = timed_query(spark, "BASELINE")

    print("=" * 64 + "\n OPTIMIZE + ZORDER BY (question_id)\n" + "=" * 64)
    spark.sql(f"OPTIMIZE {TBL} ZORDER BY (question_id)").show(truncate=False) #chạy OPTIMIZE + ZORDER BY

    print("=" * 64 + "\n SAU OPTIMIZE\n" + "=" * 64)
    f1 = num_files(spark)
    print(f">>> Số file: {f1}")
    t_after = timed_query(spark, "OPTIMIZED")

    print(f">>> KẾT QUẢ: số file {f0} -> {f1} | query {t_before:.2f}s -> {t_after:.2f}s")
    spark.stop()


if __name__ == "__main__":
    main()
