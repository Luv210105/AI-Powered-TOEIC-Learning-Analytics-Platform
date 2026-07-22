"""
add_gold_constraints.py — thêm PK (dim surrogate key) + FK (fact -> dim) cho bảng Gold.

Mục đích: (1) referential integrity ở tầng DB, (2) DBeaver vẽ ERD có đường quan hệ dim–fact.
Idempotent: mỗi lệnh 1 transaction, bỏ qua nếu constraint đã tồn tại; FK chỉ thêm khi 0 orphan.

Chạy: uv run python processing/add_gold_constraints.py
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

# PK trên surrogate key của dim
PKS = [
    ("dim_user", "user_sk"),
    ("dim_question", "question_sk"),
    ("dim_test", "test_sk"),
    ("dim_date", "date_sk"),
]

# FK fact -> dim: (fact, cột, dim đích, cột đích)
FKS = [
    ("fact_question_attempt", "user_sk", "dim_user", "user_sk"),
    ("fact_question_attempt", "question_sk", "dim_question", "question_sk"),
    ("fact_question_attempt", "date_sk", "dim_date", "date_sk"),
    ("fact_mock_test_result", "user_sk", "dim_user", "user_sk"),
    ("fact_mock_test_result", "test_sk", "dim_test", "test_sk"),
    ("fact_mock_test_result", "date_sk", "dim_date", "date_sk"),
]


def run(eng, sql: str, label: str) -> None:
    """Chạy 1 câu lệnh trong transaction riêng, bỏ qua nếu lỗi (vd đã tồn tại)."""
    try:
        with eng.begin() as c:
            c.execute(text(sql))
        print(f"  [OK]   {label}")
    except Exception as e:
        print(f"  [skip] {label}  ({str(e).splitlines()[0][:60]})")


def main() -> None:
    """Thêm PK cho dim + FK cho fact (nếu referential integrity đạt)."""
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5433")
    eng = create_engine(
        f"postgresql+psycopg2://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@{host}:{port}/{os.environ['POSTGRES_DB']}"
    )

    print("=== PRIMARY KEY (dim surrogate key) ===")
    for tbl, col in PKS:
        run(eng, f"ALTER TABLE {tbl} ADD CONSTRAINT pk_{tbl} PRIMARY KEY ({col})",
            f"PK {tbl}({col})")

    print("=== FOREIGN KEY (fact -> dim) ===")
    for fact, col, dim, refcol in FKS:
        with eng.connect() as c:
            orphans = c.execute(text(
                f"SELECT count(*) FROM {fact} f LEFT JOIN {dim} d "
                f"ON f.{col} = d.{refcol} WHERE d.{refcol} IS NULL")).scalar()
        if orphans == 0:
            run(eng, f"ALTER TABLE {fact} ADD CONSTRAINT fk_{fact}_{col} "
                     f"FOREIGN KEY ({col}) REFERENCES {dim}({refcol})",
                f"FK {fact}.{col} -> {dim}.{refcol}")
        else:
            print(f"  [SKIP] FK {fact}.{col} -> {dim}.{refcol}  ({orphans} orphan)")

    print(">>> DONE.")


if __name__ == "__main__":
    main()
