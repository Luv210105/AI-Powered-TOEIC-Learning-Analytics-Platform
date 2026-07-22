"""
validate_gold.py — Phase 6.5 (DP2 validate stage): kiểm tra chất lượng Gold (PostgreSQL).

Chạy data quality check trên Gold (row count / referential / SCD2 / null-range / uniqueness),
in PASS/FAIL, thoát exit code != 0 nếu có FAIL -> để Airflow (Phase 9) bắt lỗi.

Chạy: uv run python processing/validate_gold.py
"""

import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

_results = []


def check(name: str, passed: bool, detail: str = "") -> None:
    """Ghi lại + in kết quả 1 check."""
    _results.append(passed)
    print(f"  [{'PASS' if passed else 'FAIL'}] {name}  {detail}")


def main() -> None:
    """Chạy toàn bộ data quality check trên Gold."""
    host = os.getenv("POSTGRES_HOST", "localhost")   # 'postgres' khi chạy trong Airflow
    port = os.getenv("POSTGRES_PORT", "5433")        # '5432' khi chạy nội bộ compose
    eng = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{host}:{port}/{os.environ['POSTGRES_DB']}"
    )
    with eng.connect() as c:
        def scalar(q: str):
            return c.execute(text(q)).scalar()

        print("=== 1. Row count ===")
        n_fqa = scalar("select count(*) from fact_question_attempt")
        check("dim_user không rỗng", scalar("select count(*) from dim_user") > 0)
        check("dim_question = 8000", scalar("select count(*) from dim_question") == 8000)
        check("fact_question_attempt không rỗng", n_fqa > 0, f"({n_fqa:,} dòng)")
        check("obt = 5000", scalar("select count(*) from obt_student_learning_performance") == 5000)

        print("=== 2. Referential integrity (fact -> dim) ===")
        check("fact_question_attempt.user_sk không mồ côi",
              scalar("select count(*) from fact_question_attempt f "
                     "left join dim_user u on f.user_sk=u.user_sk where u.user_sk is null") == 0)
        check("fact_question_attempt.question_sk không mồ côi",
              scalar("select count(*) from fact_question_attempt f "
                     "left join dim_question q on f.question_sk=q.question_sk where q.question_sk is null") == 0)
        check("fact_mock_test_result.user_sk không mồ côi",
              scalar("select count(*) from fact_mock_test_result f "
                     "left join dim_user u on f.user_sk=u.user_sk where u.user_sk is null") == 0)

        print("=== 3. SCD2 integrity ===")
        check("dim_user: mỗi user_id tối đa 1 is_current=true",
              scalar("select count(*) from (select user_id from dim_user where is_current "
                     "group by user_id having count(*) > 1) t") == 0)

        print("=== 4. Null & Range ===")
        check("dim_user.user_sk không NULL",
              scalar("select count(*) from dim_user where user_sk is null") == 0)
        check("fact_mock_test_result.total_score trong [0, 990]",
              scalar("select count(*) from fact_mock_test_result where total_score < 0 or total_score > 990") == 0)

        print("=== 5. Uniqueness ===")
        check("dim_user.user_sk duy nhất",
              scalar("select count(*) from dim_user") == scalar("select count(distinct user_sk) from dim_user"))

    n_fail = sum(1 for p in _results if not p)
    print(f"\n>>> TỔNG: {len(_results) - n_fail}/{len(_results)} PASS, {n_fail} FAIL")
    if n_fail > 0:
        sys.exit(1)
    print(">>> VALIDATE GOLD OK: tất cả check PASS.")


if __name__ == "__main__":
    main()
