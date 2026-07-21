"""
validate_bronze.py — Phase 5.5 (DP1 validate stage): kiểm tra chất lượng Bronze.

Chạy các data quality check (schema / row count / null / uniqueness) trên bảng Bronze, in
PASS/FAIL từng check. Thoát với exit code != 0 nếu có FAIL -> để Airflow (Phase 9) bắt lỗi.

LƯU Ý: Bronze là RAW nên KHÔNG kiểm tra uniqueness business key (attempt_id, question_id) —
2% duplicate là cố ý, sẽ dedup ở Silver. Chỉ kiểm answer_id (khóa kỹ thuật) là duy nhất.

Submit: bash processing/spark/submit.sh validate_bronze.py
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
META_COLS = {"ingest_ts", "source", "batch_id"}

_results = []   # (tên check, passed, chi tiết)


def check(name: str, passed: bool, detail: str = "") -> None:
    """Ghi lại + in kết quả 1 check."""
    _results.append((name, passed, detail))
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}  {detail}")


def main() -> None:
    """Chạy toàn bộ data quality check trên Bronze."""
    spark = SparkSession.builder.appName("validate_bronze").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    aa = spark.read.format("delta").load(f"{BRONZE}/raw_attempt_answers/")
    users = spark.read.format("delta").load(f"{BRONZE}/raw_users/")
    questions = spark.read.format("delta").load(f"{BRONZE}/raw_questions/")

    print("=== 1. Schema check — có đủ metadata ingest ===")
    for name, df in [("raw_attempt_answers", aa), ("raw_users", users), ("raw_questions", questions)]:
        check(f"{name}: có ingest_ts/source/batch_id", META_COLS.issubset(set(df.columns)))

    print("=== 2. Row count check — không rỗng / đúng số ===")
    aa_count = aa.count()
    check("raw_attempt_answers không rỗng", aa_count > 0, f"({aa_count:,} dòng)")
    check("raw_users = 5,000", users.count() == 5000)
    check("raw_questions = 8,000", questions.count() == 8000)

    print("=== 3. Null check — key không NULL ===")
    check("raw_attempt_answers.answer_id không NULL",
          aa.filter(F.col("answer_id").isNull()).count() == 0)
    check("raw_attempt_answers.attempt_id không NULL",
          aa.filter(F.col("attempt_id").isNull()).count() == 0)
    check("raw_users.user_id không NULL",
          users.filter(F.col("user_id").isNull()).count() == 0)

    print("=== 4. Uniqueness check — khóa kỹ thuật duy nhất ===")
    check("raw_users.user_id duy nhất",
          users.select("user_id").distinct().count() == users.count())
    check("raw_questions.question_id duy nhất",
          questions.select("question_id").distinct().count() == questions.count())
    check("raw_attempt_answers.answer_id duy nhất",
          aa.select("answer_id").distinct().count() == aa_count)

    n_fail = sum(1 for _, p, _ in _results if not p)
    print(f"\n>>> TỔNG: {len(_results) - n_fail}/{len(_results)} PASS, {n_fail} FAIL")
    spark.stop()

    if n_fail > 0:
        sys.exit(1)     # báo FAIL cho Airflow
    print(">>> VALIDATE BRONZE OK: tất cả check PASS.")


if __name__ == "__main__":
    main()
