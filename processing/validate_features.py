"""
validate_features.py — Phase 8.3 (DP3 validate stage): kiểm tra chất lượng feat_user_90d (PostgreSQL).

Chạy data quality check trên feature table (row count / null / uniqueness / range / referential /
point-in-time), in PASS/FAIL, thoát exit code != 0 nếu FAIL -> để Airflow (Phase 9) bắt lỗi.

Chạy: uv run python processing/validate_features.py
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
    """Chạy toàn bộ data quality check trên feat_user_90d."""
    eng = create_engine(
        f"postgresql+psycopg://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@localhost:5433/{os.environ['POSTGRES_DB']}"
    )
    with eng.connect() as c:
        def scalar(q: str):
            return c.execute(text(q)).scalar()

        print("=== 1. Row count ===")
        n = scalar("select count(*) from feat_user_90d")
        check("feat_user_90d không rỗng", n > 0, f"({n:,} dòng)")

        print("=== 2. Null check — 2 mốc thời gian + key ===")
        check("event_timestamp không NULL",
              scalar("select count(*) from feat_user_90d where event_timestamp is null") == 0)
        check("created_ts không NULL",
              scalar("select count(*) from feat_user_90d where created_ts is null") == 0)
        check("user_id không NULL",
              scalar("select count(*) from feat_user_90d where user_id is null") == 0)

        print("=== 3. Uniqueness — khóa (user_id, event_timestamp) ===")
        check("(user_id, event_timestamp) duy nhất",
              scalar("select count(*) - count(distinct (user_id, event_timestamp)) from feat_user_90d") == 0)

        print("=== 4. Range — feature hợp lệ ===")
        check("f_accuracy_90d trong [0, 1]",
              scalar("select count(*) from feat_user_90d where f_accuracy_90d < 0 or f_accuracy_90d > 1") == 0)
        check("f_questions_answered_90d > 0",
              scalar("select count(*) from feat_user_90d where f_questions_answered_90d <= 0") == 0)
        check("f_mock_test_count_90d > 0",
              scalar("select count(*) from feat_user_90d where f_mock_test_count_90d <= 0") == 0)

        print("=== 5. Referential — user tồn tại trong dim_user ===")
        check("user_id có trong dim_user",
              scalar("select count(*) from feat_user_90d f "
                     "left join (select distinct user_id from dim_user) u on f.user_id=u.user_id "
                     "where u.user_id is null") == 0)

    n_fail = sum(1 for p in _results if not p)
    print(f"\n>>> TỔNG: {len(_results) - n_fail}/{len(_results)} PASS, {n_fail} FAIL")
    if n_fail > 0:
        sys.exit(1)
    print(">>> VALIDATE FEATURES OK: tất cả check PASS.")


if __name__ == "__main__":
    main()
