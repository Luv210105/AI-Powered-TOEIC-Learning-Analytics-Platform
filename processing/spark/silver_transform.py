"""
silver_transform.py — Phase 6.1 (DP2 part 1): Bronze -> Silver.

Làm sạch 7 bảng Bronze -> stg_* (Delta trên bucket silver). Bỏ metadata Bronze
(ingest_ts/source/batch_id), thêm silver_ts (mốc xử lý). Vẫn giữ grain chi tiết.

stg_attempt_answers (bảng chính, gánh 3 lỗi):
  - dedup theo business key (attempt_id, question_id)   -> bỏ ~46k dòng trùng
  - fillna difficulty=-1.0 / tag_grammar="unknown"       -> xử lý schema evolution
  - ép kiểu + loại dòng có key NULL

Submit: bash processing/spark/submit.sh silver_transform.py
"""

from __future__ import annotations   # cho phép cú pháp str|None trên Python 3.8/3.9 của Spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
SILVER = "s3a://silver"
META_COLS = ["ingest_ts", "source", "batch_id"]


def read_bronze(spark, name: str):
    """Đọc bảng Bronze raw_<name>, bỏ cột metadata ingest."""
    df = spark.read.format("delta").load(f"{BRONZE}/raw_{name}/")
    return df.drop(*[c for c in META_COLS if c in df.columns])


def write_silver(df, name: str, partition_col: str | None = None) -> None:
    """Thêm silver_ts và ghi ra stg_<name> (Delta)."""
    df = df.withColumn("silver_ts", F.current_timestamp())
    writer = (df.write.format("delta").mode("overwrite")
              .option("overwriteSchema", "true"))
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.save(f"{SILVER}/stg_{name}/")
    print(f"  stg_{name:<16}: {df.count():>10,} dòng")


def main() -> None:
    """Biến đổi 7 bảng Bronze -> Silver (stg_*)."""
    spark = SparkSession.builder.appName("silver_transform").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # --- attempt_answers: dedup + fillna + ép kiểu ---
    aa = (read_bronze(spark, "attempt_answers")
          .filter(F.col("answer_id").isNotNull()
                  & F.col("attempt_id").isNotNull()
                  & F.col("question_id").isNotNull())
          .dropDuplicates(["attempt_id", "question_id"])
          .fillna({"difficulty": -1.0, "tag_grammar": "unknown"})
          .withColumn("is_correct", F.col("is_correct").cast("boolean"))
          .withColumn("time_spent_sec", F.col("time_spent_sec").cast("int")))
    write_silver(aa, "attempt_answers", partition_col="attempt_date")

    # --- users ---
    users = (read_bronze(spark, "users")
             .filter(F.col("user_id").isNotNull())
             .dropDuplicates(["user_id"]))
    write_silver(users, "users")

    # --- questions ---
    questions = (read_bronze(spark, "questions")
                 .filter(F.col("question_id").isNotNull())
                 .dropDuplicates(["question_id"]))
    write_silver(questions, "questions")

    # --- test_attempts: dedup + kiểm điểm hợp lệ [0, 990] ---
    attempts = (read_bronze(spark, "test_attempts")
                .filter(F.col("attempt_id").isNotNull())
                .dropDuplicates(["attempt_id"])
                .filter(F.col("total_score").between(0, 990)))
    write_silver(attempts, "test_attempts")

    # --- bảng phụ: dedup theo khóa gốc ---
    for name, key in [("vocabulary", "word_id"), ("grammar_topics", "grammar_id"),
                      ("mock_tests", "test_id")]:
        df = (read_bronze(spark, name)
              .filter(F.col(key).isNotNull())
              .dropDuplicates([key]))
        write_silver(df, name)

    print(">>> SILVER OK: 7 bảng stg_* đã tạo.")
    spark.stop()


if __name__ == "__main__":
    main()
