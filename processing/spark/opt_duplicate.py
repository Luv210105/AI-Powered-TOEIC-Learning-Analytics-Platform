"""
opt_duplicate.py — Phase 5.4 demo lỗi DUPLICATE.

~2% dòng attempt_answers trùng business key (attempt_id, question_id) — cố tình cài ở Phase 2.
  - BASELINE  : đếm tổng dòng vs distinct (attempt_id, question_id) -> ra số dòng trùng.
  - OPTIMIZED : dropDuplicates theo business key -> loại trùng, còn 0.

Đây là DEDUP KEY dùng ở Silver (xem docs/schema_design.md §4). Bằng chứng: count trước/sau (terminal).
Submit: bash processing/spark/submit.sh opt_duplicate.py
"""

from pyspark.sql import SparkSession

BRONZE = "s3a://bronze"
DEDUP_KEYS = ["attempt_id", "question_id"]


def main() -> None:
    """Đếm số dòng trùng business key, dedup, rồi đếm lại (0 trùng)."""
    spark = SparkSession.builder.appName("opt_duplicate").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.format("delta").load(f"{BRONZE}/raw_attempt_answers/").cache()
    total = df.count()

    print("=" * 64 + "\n BASELINE — dữ liệu còn duplicate\n" + "=" * 64)
    distinct_keys = df.select(*DEDUP_KEYS).distinct().count()
    dup = total - distinct_keys
    print(f">>> [BASELINE] tổng {total:,} dòng | distinct (attempt_id, question_id) {distinct_keys:,} "
          f"| trùng {dup:,} ({dup / total:.2%})")

    print("=" * 64 + "\n OPTIMIZED — dropDuplicates theo business key\n" + "=" * 64)
    deduped = df.dropDuplicates(DEDUP_KEYS)
    after = deduped.count()
    dup_after = after - deduped.select(*DEDUP_KEYS).distinct().count()
    print(f">>> [OPTIMIZED] sau dedup {after:,} dòng | trùng còn {dup_after:,}")

    print(f">>> KẾT QUẢ: {total:,} -> {after:,} (loại {total - after:,} dòng trùng, còn {dup_after} trùng)")
    spark.stop()


if __name__ == "__main__":
    main()
