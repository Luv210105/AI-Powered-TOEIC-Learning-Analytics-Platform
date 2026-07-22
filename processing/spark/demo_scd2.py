"""
demo_scd2.py — Phase 6.2b (DP2): chứng minh SCD2 trên dim_user hoạt động.

Giả lập vài học viên đổi target_score, rồi áp logic SCD2 trong Spark:
  - user đổi  -> ĐÓNG dòng cũ (valid_to_ts=now, is_current=false) + MỞ dòng mới (user_sk mới, is_current=true)
  - user không đổi -> giữ nguyên
Ghi lại dim_user -> các user đổi sẽ có 2 dòng (1 lịch sử + 1 hiện tại).

LƯU Ý: chạy gold_dim.py TRƯỚC (reset tất cả is_current=true) rồi chạy job này 1 lần.
Chạy lại nhiều lần sẽ cộng dồn version (đúng bản chất SCD2, nhưng demo nên chạy 1 lần).

Submit: bash processing/spark/submit.sh demo_scd2.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Giả lập thay đổi target_score cho vài học viên
CHANGES = [(1, 700), (2, 800), (3, 900)]   # (user_id, target_score mới)
CHANGED_IDS = [c[0] for c in CHANGES]


def pg_opts(spark):
    """Trả về dict option JDBC dùng chung cho read/write."""
    return {
        "url": spark.conf.get("spark.pg.url"),
        "user": spark.conf.get("spark.pg.user"),
        "password": spark.conf.get("spark.pg.password"),
        "driver": "org.postgresql.Driver",
    }


def pg_read(spark, table):
    """Đọc bảng từ PostgreSQL."""
    return spark.read.format("jdbc").options(dbtable=table, **pg_opts(spark)).load()


def pg_write(df, table, spark):
    """Ghi đè bảng sang PostgreSQL."""
    df.write.format("jdbc").options(dbtable=table, **pg_opts(spark)).mode("overwrite").save()


def main() -> None:
    """Áp SCD2 cho các học viên đổi target_score và ghi lại dim_user."""
    spark = SparkSession.builder.appName("demo_scd2").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # cache + materialize TRƯỚC khi ghi đè: tránh bẫy đọc-ghi cùng bảng (overwrite drop bảng
    # trước, nếu current còn lazy sẽ đọc lại bảng rỗng -> mất dữ liệu).
    current = pg_read(spark, "dim_user").cache()
    current.count()
    cols = current.columns
    changes = spark.createDataFrame(CHANGES, ["user_id", "new_target_score"])

    # Dòng current của các user đổi -> cần đóng + tạo bản mới
    to_change = current.filter(F.col("is_current") & F.col("user_id").isin(*CHANGED_IDS))

    # 1) Đóng dòng cũ
    closed = (to_change
              .withColumn("valid_to_ts", F.current_timestamp())
              .withColumn("is_current", F.lit(False)))

    # 2) Giữ nguyên mọi dòng còn lại
    unchanged = current.filter(~(F.col("is_current") & F.col("user_id").isin(*CHANGED_IDS)))

    # 3) Mở dòng mới với target_score mới + surrogate key mới
    max_sk = current.agg(F.max("user_sk")).collect()[0][0]
    new_rows = (to_change
                .drop("user_sk", "target_score", "valid_from_ts", "valid_to_ts", "is_current")
                .join(changes, "user_id")
                .withColumnRenamed("new_target_score", "target_score")
                .withColumn("valid_from_ts", F.current_timestamp())
                .withColumn("valid_to_ts", F.lit(None).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .withColumn("user_sk", F.lit(max_sk) + F.row_number().over(Window.orderBy("user_id"))))

    result = (unchanged.select(cols)
              .unionByName(closed.select(cols))
              .unionByName(new_rows.select(cols)))
    pg_write(result, "dim_user", spark)

    # --- Kiểm chứng ---
    after = pg_read(spark, "dim_user")
    print(f">>> Tổng dòng dim_user: {after.count():,} (trước 5,000 + {len(CHANGES)} version mới)")
    print(">>> Các học viên đổi target_score (mỗi người 2 dòng — lịch sử + hiện tại):")
    (after.filter(F.col("user_id").isin(*CHANGED_IDS))
          .select("user_id", "user_sk", "target_score", "is_current", "valid_from_ts", "valid_to_ts")
          .orderBy("user_id", "user_sk")
          .show(truncate=False))

    print(">>> DEMO SCD2 OK.")
    spark.stop()


if __name__ == "__main__":
    main()
