"""
opt_schema_evolution.py — Phase 5.4 demo lỗi SCHEMA EVOLUTION.

Partition cũ (trước schema_change_date 2025-09-01) thiếu 2 cột difficulty + tag_grammar.
Khi ingest vào Bronze bằng mergeSchema, các dòng đó có difficulty/tag_grammar = NULL.

  - BASELINE  : đọc thẳng Bronze -> đếm số dòng NULL ở 2 cột (thiệt hại do schema evolution).
  - OPTIMIZED : điền giá trị default (fillna) cho cột thiếu -> hết NULL.

Bằng chứng: số NULL trước/sau (in ra terminal, không cần Spark UI).
Submit: bash processing/spark/submit.sh opt_schema_evolution.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
DEFAULT_DIFFICULTY = -1.0        # giá trị sentinel cho difficulty thiếu
DEFAULT_TAG = "unknown"          # giá trị default cho tag_grammar thiếu


def count_nulls(df, label: str):
    """Đếm số dòng NULL ở difficulty và tag_grammar, in ra kèm nhãn."""
    row = df.select(
        F.sum(F.col("difficulty").isNull().cast("int")).alias("null_difficulty"),
        F.sum(F.col("tag_grammar").isNull().cast("int")).alias("null_tag_grammar"),
    ).collect()[0]
    total = df.count()
    print(f">>> [{label}] tổng {total:,} dòng | "
          f"NULL difficulty = {row['null_difficulty']:,} | NULL tag_grammar = {row['null_tag_grammar']:,}")
    return row["null_difficulty"], row["null_tag_grammar"]


def main() -> None:
    """Đếm NULL do schema evolution -> điền default -> đếm lại (0 NULL)."""
    spark = SparkSession.builder.appName("opt_schema_evolution").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(f"{BRONZE}/raw_attempt_answers/").cache()

    print("=" * 64 + "\n BASELINE — đọc thẳng Bronze (partition cũ NULL do schema evolution)\n" + "=" * 64)
    nd0, nt0 = count_nulls(df, "BASELINE raw")

    print("=" * 64 + "\n OPTIMIZED — điền default cho cột thiếu (fillna)\n" + "=" * 64)
    fixed = df.fillna({"difficulty": DEFAULT_DIFFICULTY, "tag_grammar": DEFAULT_TAG})
    nd1, nt1 = count_nulls(fixed, "OPTIMIZED filled")

    print(f">>> KẾT QUẢ: NULL difficulty {nd0:,} -> {nd1:,} | NULL tag_grammar {nt0:,} -> {nt1:,}")
    print(">>> Đã xử lý schema evolution: điền default, hết NULL.")
    spark.stop()


if __name__ == "__main__":
    main()
