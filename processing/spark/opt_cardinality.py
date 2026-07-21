"""
opt_cardinality.py — Phase 5.4 demo lỗi HIGH CARDINALITY.

Cột answer_id có ~2M giá trị DUY NHẤT (cardinality rất cao). Đếm số distinct:
  - BASELINE  : exact countDistinct -> phải SHUFFLE toàn bộ ~2M khóa để khử trùng -> nặng.
  - OPTIMIZED : approx_count_distinct (HyperLogLog) -> mỗi partition dựng sketch nhỏ rồi gộp
                -> shuffle cực nhẹ, nhanh hơn, sai số ~1%.

So sánh thời gian + Shuffle Read/Write trên Spark UI. Giữ UI mở để chụp.
Submit: bash processing/spark/submit.sh opt_cardinality.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
PAUSE_SECONDS = 120


def run(df, agg_col, label: str):
    """Chạy 1 phép đếm distinct, gắn nhãn cho Spark UI, đo thời gian."""
    df.sparkSession.sparkContext.setJobDescription(label)
    t0 = time.time()
    result = df.select(agg_col.alias("distinct_count")).collect()[0]["distinct_count"]
    dt = time.time() - t0
    print(f">>> [{label}] distinct = {result:,} | thời gian: {dt:.1f}s")
    return dt, result


def main() -> None:
    """So sánh exact countDistinct vs approx_count_distinct trên answer_id."""
    spark = SparkSession.builder.appName("opt_cardinality").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.adaptive.enabled", "false")   # giữ shuffle thô để quan sát

    df = (spark.read.format("delta").load(f"{BRONZE}/raw_attempt_answers/")
          .select("answer_id")
          .cache())
    print(">>> Tổng số dòng:", df.count())

    print("=" * 64 + "\n BASELINE — exact countDistinct (shuffle toàn bộ khóa)\n" + "=" * 64)
    t_base, c_base = run(df, F.countDistinct("answer_id"), "BASELINE exact countDistinct")

    print("=" * 64 + "\n OPTIMIZED — approx_count_distinct (HyperLogLog)\n" + "=" * 64)
    t_opt, c_opt = run(df, F.approx_count_distinct("answer_id"), "OPTIMIZED approx_count_distinct")

    err = abs(c_base - c_opt) / c_base * 100
    speedup = t_base / max(t_opt, 0.1)
    print(f">>> KẾT QUẢ: exact {t_base:.1f}s vs approx {t_opt:.1f}s "
          f"(nhanh hơn ~{speedup:.1f}x, sai số ~{err:.2f}%)")

    print(f">>> Spark UI: http://localhost:4040 — CHỤP Stages (Shuffle Read/Write) trong {PAUSE_SECONDS}s...")
    time.sleep(PAUSE_SECONDS)
    spark.stop()


if __name__ == "__main__":
    main()
