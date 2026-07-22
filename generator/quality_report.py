"""
quality_report.py — Data Quality Report cho dữ liệu offline (sinh bởi offline_generator.py).

Đo & in BẰNG CHỨNG cho 4 lỗi cố tình cài (rubric Section 01). Chỉ ĐỌC dữ liệu, không sửa.
Dùng để chạy + chụp màn hình đưa vào docs/data_generator.md.
Chạy: uv run python generator/quality_report.py
"""

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import yaml

DATA = Path("data/offline")
CFG = yaml.safe_load(open(Path(__file__).parent / "config.yaml", encoding="utf-8"))
ANSWERS = DATA / "attempt_answers"


def section(title: str) -> None:
    """In tiêu đề một mục cho dễ đọc/chụp màn hình."""
    print("\n" + "=" * 64 + f"\n {title}\n" + "=" * 64)


def report_skew() -> None:
    """Lỗi 1 — Skew: mục tiêu dồn 600-700 (users) + câu dồn Part 5 (questions)."""
    section("1) SKEW")
    users = pd.read_parquet(DATA / "users", columns=["target_score"])
    in_band = users["target_score"].between(600, 700).mean()
    print(f"target_score trong band 600-700 : {in_band:.1%}  (cấu hình mong đợi ~{CFG['skew_target_score_band']:.0%})")
    print("Phân bố target_score:")
    print((users["target_score"].value_counts(normalize=True).sort_index() * 100).round(1).to_string())

    parts = pd.read_parquet(DATA / "questions", columns=["part"])["part"]
    dist = (parts.value_counts(normalize=True).sort_index() * 100).round(1)
    print("\nPhân bố câu hỏi theo Part (%):")
    print(dist.to_string())
    print(f"-> Part 5 chiếm {dist.get(5, 0):.1f}%  (cấu hình mong đợi ~{CFG['skew_part5_ratio']:.0%})")


def report_cardinality() -> None:
    """Lỗi 2 — High cardinality: các ID gần như duy nhất."""
    section("2) HIGH CARDINALITY")
    cols = pd.read_parquet(ANSWERS, columns=["answer_id", "attempt_id", "question_id"])
    total = len(cols)
    print(f"{'Cột':<14}{'distinct':>12}{'tổng':>14}{'tỷ lệ distinct/tổng':>22}")
    for c in ["answer_id", "attempt_id", "question_id"]:
        nunique = cols[c].nunique()
        print(f"{c:<14}{nunique:>12,}{total:>14,}{nunique / total:>21.2%}")
    users = pd.read_parquet(DATA / "users", columns=["user_id"])
    print(f"{'user_id':<14}{users['user_id'].nunique():>12,}{len(users):>14,}{1.0:>21.2%}")
    print("-> answer_id/user_id gần như duy nhất (cardinality cao); question_id lặp nhiều lần trong answers.")


def report_schema_evolution() -> None:
    """Lỗi 3 — Schema evolution: partition cũ thiếu 2 cột difficulty + tag_grammar."""
    section("3) SCHEMA EVOLUTION")
    change = CFG["schema_change_date"]
    with_cols = without_cols = rows_with = rows_without = 0
    for p in sorted(ANSWERS.glob("attempt_date=*")):
        f = p / "data.parquet"
        has = "difficulty" in pq.read_schema(f).names
        nrows = pq.read_metadata(f).num_rows
        if has:
            with_cols += 1
            rows_with += nrows
        else:
            without_cols += 1
            rows_without += nrows
    total_rows = rows_with + rows_without
    print(f"schema_change_date = {change}")
    print(f"Partition CÓ  difficulty+tag_grammar (>= ngày đổi): {with_cols:>4}  ({rows_with:,} dòng)")
    print(f"Partition THIẾU difficulty+tag_grammar (<  ngày đổi): {without_cols:>4}  ({rows_without:,} dòng)")
    print(f"-> Khi đọc gộp (mergeSchema), {rows_without / total_rows:.1%} số dòng sẽ NULL ở 2 cột này.")


def report_duplicate() -> None:
    """Lỗi 4 — Duplicate: dòng trùng business key (attempt_id, question_id)."""
    section("4) DUPLICATE")
    keys = pd.read_parquet(ANSWERS, columns=["attempt_id", "question_id"])
    total = len(keys)
    distinct = len(keys.drop_duplicates())
    dup = total - distinct
    print(f"Tổng dòng attempt_answers          : {total:,}")
    print(f"Distinct (attempt_id, question_id) : {distinct:,}")
    print(f"Số dòng trùng                      : {dup:,}  ({dup / total:.2%})")
    print(f"Sau khi dedup còn                  : {distinct:,}  (cấu hình cài ~{CFG['duplicate_rate_offline']:.0%})")


def main() -> None:
    """Chạy toàn bộ báo cáo chất lượng dữ liệu."""
    print("DATA QUALITY REPORT — dữ liệu offline TOEIC (seed =", CFG["random_seed"], ")")
    report_skew()
    report_cardinality()
    report_schema_evolution()
    report_duplicate()
    print("\nHoàn tất báo cáo.")


if __name__ == "__main__":
    main()
