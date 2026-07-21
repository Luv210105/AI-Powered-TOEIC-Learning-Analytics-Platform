"""
gold_dim.py — Phase 6.2a (DP2): build Gold dimension từ Silver -> PostgreSQL.

- dim_user     : SCD2 (user_sk, valid_from_ts, valid_to_ts, is_current). Build đầu tiên nên
                 mọi user is_current=true (demo thay đổi ở demo_scd2.py).
- dim_question : SCD1 (ghi đè).
- dim_test     : SCD1.
- dim_date     : sinh từ dải attempt_date.

Ghi PostgreSQL qua JDBC (mode overwrite). Thông tin kết nối lấy từ spark.pg.* (submit.sh truyền vào).
Submit: bash processing/spark/submit.sh gold_dim.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER = "s3a://silver"


def pg_write(df, table: str, spark) -> None:
    """Ghi 1 bảng sang PostgreSQL qua JDBC (overwrite)."""
    (df.write.format("jdbc")
       .option("url", spark.conf.get("spark.pg.url"))
       .option("dbtable", table)
       .option("user", spark.conf.get("spark.pg.user"))
       .option("password", spark.conf.get("spark.pg.password"))
       .option("driver", "org.postgresql.Driver")
       .mode("overwrite")
       .save())
    print(f"  {table:<16}: {df.count():>8,} dòng -> PostgreSQL")


def main() -> None:
    """Build 4 dimension từ Silver và ghi sang PostgreSQL."""
    spark = SparkSession.builder.appName("gold_dim").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    users = spark.read.format("delta").load(f"{SILVER}/stg_users/")
    questions = spark.read.format("delta").load(f"{SILVER}/stg_questions/")
    mock = spark.read.format("delta").load(f"{SILVER}/stg_mock_tests/")
    aa = spark.read.format("delta").load(f"{SILVER}/stg_attempt_answers/")

    # --- dim_user (SCD2, build đầu tiên: mọi dòng is_current=true) ---
    dim_user = (users
                .withColumn("user_sk", F.row_number().over(Window.orderBy("user_id")))
                .withColumn("valid_from_ts", F.col("signup_ts"))
                .withColumn("valid_to_ts", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .select("user_sk", "user_id", "age", "university", "major",
                        "target_score", "base_ability",
                        "valid_from_ts", "valid_to_ts", "is_current"))
    pg_write(dim_user, "dim_user", spark)

    # --- dim_question (SCD1) ---
    dim_question = (questions
                    .withColumn("question_sk", F.row_number().over(Window.orderBy("question_id")))
                    .select("question_sk", "question_id", "part", "skill",
                            "difficulty", "tag_grammar", "correct_answer"))
    pg_write(dim_question, "dim_question", spark)

    # --- dim_test (SCD1) ---
    dim_test = (mock
                .withColumn("test_sk", F.row_number().over(Window.orderBy("test_id")))
                .select("test_sk", "test_id", "test_name", "total_questions"))
    pg_write(dim_test, "dim_test", spark)

    # --- dim_date (sinh từ các attempt_date phân biệt) ---
    dim_date = (aa.select(F.col("attempt_date").cast("date").alias("date_value")).distinct()
                .withColumn("date_sk", F.date_format("date_value", "yyyyMMdd").cast("int"))
                .withColumn("year", F.year("date_value"))
                .withColumn("month", F.month("date_value"))
                .withColumn("day", F.dayofmonth("date_value"))
                .withColumn("weekday", F.dayofweek("date_value"))
                .withColumn("is_weekend", F.dayofweek("date_value").isin(1, 7))
                .select("date_sk", "date_value", "year", "month", "day", "weekday", "is_weekend"))
    pg_write(dim_date, "dim_date", spark)

    print(">>> GOLD DIM OK: dim_user (SCD2) + dim_question + dim_test + dim_date -> PostgreSQL.")
    spark.stop()


if __name__ == "__main__":
    main()
