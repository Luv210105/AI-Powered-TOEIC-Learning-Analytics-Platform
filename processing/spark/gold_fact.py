"""
gold_fact.py — Phase 6.3 (DP2): build Gold fact + OBT từ Silver + dim -> PostgreSQL.

- fact_question_attempt : grain 1 câu trả lời; FK user_sk/question_sk/date_sk; measure is_correct, time_spent_sec.
- fact_mock_test_result : grain 1 lượt thi; FK user_sk/test_sk/date_sk; measure điểm listening/reading/total.
- obt_student_learning_performance : 1 dòng/học viên, denormalized cho dashboard.

FK trỏ surrogate key lấy từ dim_user (is_current=true). Dim nhỏ -> broadcast join.
Submit: bash processing/spark/submit.sh gold_fact.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

SILVER = "s3a://silver"


def pg_opts(spark):
    """Option JDBC dùng chung."""
    return {
        "url": spark.conf.get("spark.pg.url"),
        "user": spark.conf.get("spark.pg.user"),
        "password": spark.conf.get("spark.pg.password"),
        "driver": "org.postgresql.Driver",
    }


def pg_read(spark, table):
    """Đọc bảng dim từ PostgreSQL."""
    return spark.read.format("jdbc").options(dbtable=table, **pg_opts(spark)).load()


def pg_write(df, table, spark):
    """Ghi đè bảng sang PostgreSQL."""
    df.write.format("jdbc").options(dbtable=table, **pg_opts(spark)).mode("overwrite").save()
    print(f"  {table:<32}: {df.count():>10,} dòng -> PostgreSQL")


def date_sk(col):
    """Chuyển cột ngày/timestamp -> date_sk dạng yyyyMMdd (int)."""
    return F.date_format(col.cast("date"), "yyyyMMdd").cast("int")


def main() -> None:
    """Build 2 fact + 1 OBT từ Silver và dim, ghi PostgreSQL."""
    spark = SparkSession.builder.appName("gold_fact").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Silver
    aa = spark.read.format("delta").load(f"{SILVER}/stg_attempt_answers/")
    attempts = spark.read.format("delta").load(f"{SILVER}/stg_test_attempts/")

    # Dim (surrogate key) — chỉ lấy dim_user hiện tại
    dim_user = pg_read(spark, "dim_user").filter(F.col("is_current")).select(
        "user_id", "user_sk", "university", "major", "target_score")
    dim_question = pg_read(spark, "dim_question").select("question_id", "question_sk")
    dim_test = pg_read(spark, "dim_test").select("test_id", "test_sk")

    attempts_user = attempts.select("attempt_id", "user_id", "test_id", "start_ts",
                                    "listening_score", "reading_score", "total_score")

    # --- fact_question_attempt (grain: 1 answer) ---
    fqa = (aa.join(attempts_user.select("attempt_id", "user_id"), "attempt_id")
             .join(F.broadcast(dim_user.select("user_id", "user_sk")), "user_id")
             .join(F.broadcast(dim_question), "question_id")
             .withColumn("date_sk", date_sk(F.col("attempt_date")))
             .select("answer_id", "attempt_id", "user_sk", "question_sk", "date_sk",
                     "part", "is_correct", "time_spent_sec"))
    pg_write(fqa, "fact_question_attempt", spark)

    # --- fact_mock_test_result (grain: 1 attempt) ---
    fmt = (attempts_user
           .join(F.broadcast(dim_user.select("user_id", "user_sk")), "user_id")
           .join(F.broadcast(dim_test), "test_id")
           .withColumn("date_sk", date_sk(F.col("start_ts")))
           .select("attempt_id", "user_sk", "test_sk", "date_sk",
                   "listening_score", "reading_score", "total_score"))
    pg_write(fmt, "fact_mock_test_result", spark)

    # --- obt_student_learning_performance (grain: 1 user) ---
    answers_user = aa.join(attempts_user.select("attempt_id", "user_id"), "attempt_id")
    ans_stats = answers_user.groupBy("user_id").agg(
        F.count("*").alias("questions_answered"),
        F.round(F.avg(F.col("is_correct").cast("int")), 3).alias("avg_accuracy"),
        F.round(F.avg(F.when(F.col("part") <= 4, F.col("is_correct").cast("int"))), 3).alias("listening_accuracy"),
        F.round(F.avg(F.when(F.col("part") >= 5, F.col("is_correct").cast("int"))), 3).alias("reading_accuracy"),
    )
    attempt_stats = attempts_user.groupBy("user_id").agg(
        F.countDistinct("attempt_id").alias("mock_test_count"),
        F.round(F.avg("total_score"), 1).alias("avg_total_score"),
    )
    obt = (dim_user.select("user_id", "university", "major", "target_score")
           .join(attempt_stats, "user_id", "left")
           .join(ans_stats, "user_id", "left"))
    pg_write(obt, "obt_student_learning_performance", spark)

    print(">>> GOLD FACT OK: 2 fact + OBT -> PostgreSQL.")
    spark.stop()


if __name__ == "__main__":
    main()
