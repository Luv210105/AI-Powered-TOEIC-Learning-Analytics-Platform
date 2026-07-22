"""
feature_offline.py — Phase 8 (DP3): tính feature table point-in-time feat_user_90d -> PostgreSQL.

Mỗi lần thi thử là 1 MỐC (reference point). Tại mỗi mốc, tính đặc trưng rolling 90 ngày TRƯỚC đó
(point-in-time: chỉ dùng answer có ans_date < ref_date, tránh data leakage).

Mỗi dòng feature có:
  - event_timestamp : mốc PIT (feature có hiệu lực tại thời điểm này)
  - created_ts      : lúc tính (dedup giữ mới nhất; khóa (user_id, event_timestamp) là duy nhất)

Submit: bash processing/spark/submit.sh feature_offline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "s3a://silver"
WINDOW_DAYS = 90


def main() -> None:
    """Tính feat_user_90d point-in-time và ghi PostgreSQL."""
    spark = SparkSession.builder.appName("feature_offline").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    answers = spark.read.format("delta").load(f"{SILVER}/stg_attempt_answers/")
    attempts = spark.read.format("delta").load(f"{SILVER}/stg_test_attempts/")

    # answer kèm user_id + ngày (ans_date)
    aw = (answers.select("attempt_id", "attempt_date", "part", "is_correct")
          .join(attempts.select("attempt_id", "user_id"), "attempt_id")
          .select("user_id",
                  F.col("attempt_id").alias("src_attempt_id"),
                  F.to_date("attempt_date").alias("ans_date"),
                  "part", "is_correct")
          .alias("a"))

    # reference point: mỗi lần thi = 1 mốc PIT
    refs = (attempts.select("user_id", "attempt_id",
                            F.col("start_ts").alias("event_timestamp"),
                            F.to_date("start_ts").alias("ref_date"))
            .alias("r"))

    # range join point-in-time: answer trong [ref_date - 90, ref_date) — TRƯỚC mốc, không gồm mốc
    joined = aw.join(
        refs,
        (F.col("a.user_id") == F.col("r.user_id"))
        & (F.col("a.ans_date") < F.col("r.ref_date"))
        & (F.col("a.ans_date") >= F.date_sub(F.col("r.ref_date"), WINDOW_DAYS)),
        "inner",
    )

    is_correct_int = F.col("a.is_correct").cast("int")
    feat = (joined.groupBy(
                F.col("r.user_id").alias("user_id"),
                F.col("r.attempt_id").alias("ref_attempt_id"),
                F.col("r.event_timestamp").alias("event_timestamp"))
            .agg(
                F.count("*").alias("f_questions_answered_90d"),
                F.countDistinct("a.src_attempt_id").alias("f_mock_test_count_90d"),
                F.round(F.avg(is_correct_int), 3).alias("f_accuracy_90d"),
                F.round(F.avg(F.when(F.col("a.part") <= 4, is_correct_int)), 3).alias("f_listening_acc_90d"),
                F.round(F.avg(F.when(F.col("a.part") >= 5, is_correct_int)), 3).alias("f_reading_acc_90d"),
            )
            .withColumn("f_listening_vs_reading_gap",
                        F.round(F.col("f_listening_acc_90d") - F.col("f_reading_acc_90d"), 3))
            .withColumn("created_ts", F.current_timestamp()))

    (feat.write.format("jdbc")
        .option("url", spark.conf.get("spark.pg.url"))
        .option("dbtable", "feat_user_90d")
        .option("user", spark.conf.get("spark.pg.user"))
        .option("password", spark.conf.get("spark.pg.password"))
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save())

    print(f">>> FEATURE OFFLINE OK: feat_user_90d -> PostgreSQL ({feat.count():,} dòng, "
          f"rolling {WINDOW_DAYS}d point-in-time).")
    spark.stop()


if __name__ == "__main__":
    main()
