"""
warehouse_index.py — Phase 6.4: tối ưu warehouse (PostgreSQL Gold).

Thêm index trên fact_question_attempt.user_sk, so EXPLAIN ANALYZE TRƯỚC (Seq Scan) và
SAU khi có index (Index Scan). Query mẫu: đếm câu trả lời của 1 học viên (lọc theo user_sk).

Chạy: uv run python processing/warehouse_index.py
"""

import os

from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

USER_SK = 100
QUERY = f"SELECT count(*) FROM fact_question_attempt WHERE user_sk = {USER_SK}"


def explain(conn, label: str) -> None:
    """In kế hoạch EXPLAIN ANALYZE của QUERY."""
    rows = conn.execute(text(f"EXPLAIN ANALYZE {QUERY}")).fetchall()
    print(f"\n--- {label} ---")
    for r in rows:
        print("  " + r[0])


def main() -> None:
    """So sánh kế hoạch thực thi trước/sau khi tạo index trên user_sk."""
    eng = create_engine(
        f"postgresql+psycopg://{os.environ['POSTGRES_USER']}:{os.environ['POSTGRES_PASSWORD']}"
        f"@localhost:5433/{os.environ['POSTGRES_DB']}"
    )
    with eng.begin() as c:
        # Đảm bảo bắt đầu từ trạng thái chưa có index
        c.execute(text("DROP INDEX IF EXISTS idx_fqa_user_sk"))
        c.execute(text("ANALYZE fact_question_attempt"))

        print("=" * 64 + "\n BASELINE — chưa có index (Seq Scan toàn bảng 2M dòng)\n" + "=" * 64)
        explain(c, "BASELINE")

        # Tạo index trên cột filter/join hay dùng
        c.execute(text("CREATE INDEX idx_fqa_user_sk ON fact_question_attempt(user_sk)"))
        c.execute(text("ANALYZE fact_question_attempt"))

        print("\n" + "=" * 64 + "\n OPTIMIZED — có index idx_fqa_user_sk (Index Scan)\n" + "=" * 64)
        explain(c, "OPTIMIZED")

    print("\n>>> WAREHOUSE INDEX OK: so sánh Seq Scan vs Index Scan ở trên.")


if __name__ == "__main__":
    main()
