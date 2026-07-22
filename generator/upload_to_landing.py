"""
upload_to_landing.py — Phase 3: đưa dữ liệu offline lên "nơi lưu nguồn" (landing).

- Upload toàn bộ Parquet trong data/offline/ lên MinIO bucket 'landing' (giữ nguyên cấu trúc partition).
- Nạp 2 bảng (users, questions) vào PostgreSQL như một 'source database' (mô phỏng department khác).

QUAN TRỌNG: GIỮ NGUYÊN dữ liệu lỗi — KHÔNG sửa gì ở đây (xử lý lỗi là việc của Spark ở Phase 5).

Thông tin bí mật (user/password) đọc từ .env; host/port để mặc định khớp docker-compose.
Chạy: uv run python generator/upload_to_landing.py
"""

import os
from pathlib import Path

import boto3
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()  # nạp biến từ file .env

DATA = Path("data/offline")
LANDING_BUCKET = "landing"

# --- Kết nối MinIO (S3-compatible) ---
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASS = os.environ["MINIO_ROOT_PASSWORD"]

# --- Kết nối PostgreSQL (port 5433 khớp mapping trong docker-compose) ---
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_USER = os.environ["POSTGRES_USER"]
PG_PASS = os.environ["POSTGRES_PASSWORD"]
PG_DB = os.environ["POSTGRES_DB"]

# 2 bảng seed vào Postgres để mô phỏng nguồn dạng database
SEED_TABLES = ["users", "questions"]


def upload_parquet_to_minio() -> None:
    """Upload mọi file .parquet trong data/offline/ lên bucket landing, giữ nguyên đường dẫn."""
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
        config=Config(signature_version="s3v4"),
    )
    files = sorted(DATA.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"Không thấy file Parquet nào trong {DATA}. Chạy offline_generator.py trước.")
    for f in files:
        key = str(f.relative_to(DATA))          # vd: users/signup_date=2025-06-01/data.parquet
        s3.upload_file(str(f), LANDING_BUCKET, key)
    print(f"[MinIO] Đã upload {len(files):,} file Parquet lên s3://{LANDING_BUCKET}/")


def seed_postgres() -> None:
    """Nạp users + questions vào Postgres (bảng src_*) như source DB mô phỏng department khác."""
    engine = create_engine(
        f"postgresql+psycopg://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    for name in SEED_TABLES:
        df = pd.read_parquet(DATA / name)        # đọc gộp mọi partition (nếu có)
        df.to_sql(f"src_{name}", engine, if_exists="replace", index=False)
        print(f"[Postgres] Đã nạp {len(df):,} dòng vào bảng src_{name}")


def main() -> None:
    """Chạy Phase 3: upload landing + seed source DB."""
    upload_parquet_to_minio()
    seed_postgres()
    print("Xong Phase 3: data đã ở landing (MinIO) + source DB (Postgres). Dữ liệu lỗi giữ nguyên.")


if __name__ == "__main__":
    main()
