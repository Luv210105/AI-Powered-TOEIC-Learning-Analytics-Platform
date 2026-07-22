"""
opt_skew.py — Phase 5.4 demo lỗi SKEW: baseline (sort-merge, lệch) vs optimized (broadcast join).

Bối cảnh: attempt_answers lệch nặng về Part 5 (~34% trong 2M dòng). Khi join với bảng chiều
nhỏ `part_dim` trên khóa `part`:
  - BASELINE  : ép sort-merge join + tắt AQE -> shuffle theo `part`, task cho part=5 ôm ~690k
                dòng trong khi các task khác nhẹ -> LỆCH (1 task chạy rất lâu, thấy rõ trên Spark UI).
  - OPTIMIZED : broadcast bảng nhỏ -> KHÔNG shuffle bảng lớn -> không còn lệch -> nhanh hơn.

Chạy job in ra thời gian 2 cách + giữ Spark UI (localhost:4040) mở để chụp Stages/SQL.
Submit: bash processing/spark/submit.sh opt_skew.py
"""

import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BRONZE = "s3a://bronze"
PAUSE_SECONDS = 120   # giữ Spark UI sống để kịp chụp màn hình


def run_join(fact, part_dim, label: str) -> float:
    """Join fact với part_dim, tổng hợp theo Part, in kết quả + đo thời gian (giây)."""
    # Gắn nhãn -> Spark UI (tab Jobs) hiện đúng "BASELINE..." / "OPTIMIZED..." ở cột Description
    fact.sparkSession.sparkContext.setJobDescription(label)
    t0 = time.time()
    joined = fact.join(part_dim, on="part", how="inner")
    (joined.groupBy("part_name")
           .agg(F.count("*").alias("n"), F.avg("difficulty").alias("avg_diff"))
           .orderBy("part_name")
           .show(truncate=False))
    dt = time.time() - t0
    print(f">>> [{label}] thời gian: {dt:.1f}s")
    return dt


def main() -> None:
    """Chạy baseline (skew) rồi optimized (broadcast) và so sánh thời gian."""
    spark = SparkSession.builder.appName("opt_skew").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Bảng lớn (fact) — cache để so sánh chỉ tính chi phí join, không tính chi phí đọc lại
    fact = (spark.read.format("delta").load(f"{BRONZE}/raw_attempt_answers/")
            .select("part", "difficulty")
            .cache())
    print(">>> Tổng số dòng fact:", fact.count())

    # Bảng chiều nhỏ (7 dòng) — mô tả từng Part
    part_dim = spark.createDataFrame(
        [(1, "Part1-Photographs"), (2, "Part2-Q&A"), (3, "Part3-Conversations"),
         (4, "Part4-Talks"), (5, "Part5-Incomplete-Sentences"),
         (6, "Part6-Text-Completion"), (7, "Part7-Reading")],
        ["part", "part_name"],
    )

    # ---- BASELINE: sort-merge join, tắt AQE + tắt broadcast -> skew lộ rõ ----
    print("=" * 64 + "\n BASELINE — sort-merge join (skew lộ rõ trên Spark UI)\n" + "=" * 64)
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    t_base = run_join(fact, part_dim, "BASELINE sort-merge")

    # ---- OPTIMIZED: broadcast bảng nhỏ -> không shuffle bảng lớn -> hết skew ----
    print("=" * 64 + "\n OPTIMIZED — broadcast join (khử skew)\n" + "=" * 64)
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    t_opt = run_join(fact, F.broadcast(part_dim), "OPTIMIZED broadcast")

    speedup = t_base / max(t_opt, 0.1)
    print(f">>> KẾT QUẢ: baseline {t_base:.1f}s vs optimized {t_opt:.1f}s (nhanh hơn ~{speedup:.1f}x)")

    print(f">>> Spark UI: http://localhost:4040 — CHỤP Stages/SQL trong {PAUSE_SECONDS}s...")
    time.sleep(PAUSE_SECONDS)
    spark.stop()


if __name__ == "__main__":
    main()
