# AI-Powered TOEIC Learning Analytics Platform

Data platform end-to-end cho domain luyện thi **TOEIC**, minh hoạ trọn vòng đời **Data Engineering**:
sinh dữ liệu (offline + streaming) → ingest & xử lý (Spark/Flink) → lakehouse **Bronze → Silver → Gold**
→ feature store → orchestration (Airflow) → governance (DataHub).

---

## Table of Contents

1. [Bối cảnh & Mục tiêu](#bối-cảnh--mục-tiêu)
2. [Kiến trúc](#kiến-trúc)
3. [Repo Structure](#repo-structure)
4. [Tech stack](#tech-stack)
5. [Data pipelines](#data-pipelines)
6. [Tài liệu chi tiết](#tài-liệu-chi-tiết-docs)
7. [Run instructions](#run-instructions)

---

## Bối cảnh & Mục tiêu

### Bài toán
Một nền tảng luyện thi **TOEIC** có hàng chục nghìn học viên làm bài luyện tập và thi thử mỗi ngày.
Dữ liệu học tập phát sinh từ **nhiều nguồn, nhiều dạng**:

- **Batch (offline):** kết quả bài làm, ngân hàng câu hỏi, hồ sơ học viên… được export định kỳ dạng file.
- **Streaming (realtime):** sự kiện tương tác của học viên (bắt đầu/nộp bài, trả lời từng câu…) bắn liên tục.
- **OLTP nguồn:** bảng người dùng/câu hỏi nằm ở database của một hệ thống khác (mô phỏng "department khác").

Dữ liệu thô này **bẩn và rời rạc** — lệch phân phối (skew), trùng lặp, schema thay đổi theo thời gian,
sự kiện đến trễ / sai thứ tự — nên **không thể dùng trực tiếp** cho phân tích hay huấn luyện mô hình.

### Mục tiêu
Xây một **data platform end-to-end** biến dữ liệu học tập thô thành dữ liệu **sạch, có cấu trúc,
sẵn sàng cho phân tích & ML**: dashboard theo dõi tiến bộ học viên và feature cho mô hình dự đoán
điểm / gợi ý luyện tập.

### Vì sao làm theo hướng này (trọng tâm Data Engineering)
Giá trị kỹ thuật của project **không nằm ở mô hình AI cuối**, mà ở việc dựng đúng **vòng đời dữ liệu**:

- Kiến trúc **medallion** Bronze → Silver → Gold: tách rõ dữ liệu raw / đã-làm-sạch / sẵn-sàng-dùng.
- **Cố tình tạo lỗi dữ liệu thật** (offline + streaming) rồi **xử lý có bằng chứng đo được** —
  đây là kỹ năng cốt lõi của Data Engineering, không phải "dữ liệu đẹp cho sẵn".
- Vận hành như production: batch (**Spark**) + streaming (**Flink**), điều phối (**Airflow**),
  tối ưu lưu trữ, và **governance** (lineage + data contract, **DataHub**).

> Domain TOEIC đóng vai trò "cái cớ" để có **dữ liệu thật với đặc tính đa dạng**. Phần mở rộng ML/LLM ở
> phase cuối chỉ để minh hoạ rằng dữ liệu tầng Gold **dùng được ngay** cho hạ nguồn.

---

## Kiến trúc

```
Generator ─┬─► MinIO (landing)  ─┐
           ├─► PostgreSQL (source)├─► Spark (batch) ─► Bronze→Silver→Gold (Delta/MinIO + Postgres)
           └─► Kafka (events) ───┴─► Flink (stream) ─► Bronze stream
                                          Airflow orchestrate DP1/DP2/DP3
                                          DataHub governance (lineage + contract)
```

Sơ đồ deployment đầy đủ (12 luồng dữ liệu đánh số, deployable units): **[docs/architecture.md](docs/architecture.md)**.

## Repo Structure

```
.
├── docker-compose.yml            # Main stack: minio, postgres, spark×2, kafka, flink×2, airflow
├── docker/                       # Dockerfile.flink, Dockerfile.airflow (custom images)
├── generator/                    # Section 01 — Data Generator
│   ├── config.yaml               #   mọi tham số + seed
│   ├── offline_generator.py      #   sinh 7 bảng Parquet + 4 lỗi offline
│   ├── stream_generator.py       #   bắn event Kafka + 3 lỗi streaming
│   ├── upload_to_landing.py      #   Parquet -> MinIO landing + seed Postgres
│   └── quality_report.py         #   đo 4 lỗi (Data Quality Report)
├── processing/
│   ├── spark/                    # Spark jobs: ingest_bronze, silver_transform, gold_dim/fact,
│   │                             #   feature_offline, opt_* (tối ưu), validate_bronze, submit.sh
│   ├── flink/                    # Flink jobs: flink_stream (windowing), flink_to_bronze
│   ├── warehouse_index.py        # tối ưu index Postgres
│   ├── validate_gold.py          # validate DP2
│   └── validate_features.py      # validate DP3
├── pipelines/airflow/dags/       # 3 DAG (dp1/dp2/dp3) + common.py (helper)
├── governance/datahub/           # DataHub: recipes ingest, lineage.yml, emit_assertion.py, start script
├── docs/                         # Tài liệu + ảnh bằng chứng (docs/images/phaseN/)
└── pyproject.toml / uv.lock      # môi trường Python (uv)
```

## Tech stack

| Layer | Công cụ |
|-------|---------|
| Storage | MinIO (S3), Delta Lake, PostgreSQL |
| Ingestion | Kafka (KRaft), Spark JDBC/S3A |
| Batch | Apache Spark (cluster) |
| Streaming | Apache Flink (cluster, PyFlink) |
| Orchestration | Apache Airflow |
| Governance | DataHub (lineage + data contract) |
| Env | Python + `uv`, Docker Compose |

## Data pipelines

| Pipeline | Luồng | Stage | Validate |
|----------|-------|-------|----------|
| **DP1** | landing → Bronze (Delta) | ingest → validate | 12/12 PASS |
| **DP2** | Bronze → Silver → Gold (dim SCD2 + fact + OBT) | ingest → validate | 11/11 PASS |
| **DP3** | feature `feat_user_90d` (point-in-time) | ingest → validate | 9/9 PASS |

3 lỗi dữ liệu **offline** (skew, high cardinality, schema evolution, duplicate) và **streaming**
(burst, late/out-of-order, duplicate) đều được cố tình tạo + xử lý có bằng chứng.

## Tài liệu chi tiết (`docs/`)

| Tài liệu | Nội dung |
|----------|----------|
| [architecture.md](docs/architecture.md) | Deployment diagram, luồng dữ liệu, deployable units |
| [data_generator.md](docs/data_generator.md) | Section 01: 7 bảng + 4 lỗi offline + Data Quality Report |
| [schema_design.md](docs/schema_design.md) | Section 02: Bronze/Silver/Gold, SCD2, ERD, naming, glossary |
| [spark_optimization.md](docs/spark_optimization.md) | 4 lỗi offline: baseline → optimize + Spark UI |
| [flink_streaming.md](docs/flink_streaming.md) | Streaming: windowing + watermark + dedup + Flink UI |
| [storage_optimization.md](docs/storage_optimization.md) | Compaction/Z-ORDER (lakehouse) + index (warehouse) |
| [orchestration.md](docs/orchestration.md) | Airflow: 3 DAG DP1/DP2/DP3, connections/variables |
| [governance.md](docs/governance.md) | DataHub: metadata + lineage + data contract |
| [novel_ideas.md](docs/novel_ideas.md) | Novel ideas + proof: `uv`/multistage (image −70%) + DataHub bypass |
| [rubric_coverage.md](docs/rubric_coverage.md) | Rubric Coverage Map (tự chấm 11 hạng mục) |

## Run instructions

```bash
# 1. Môi trường Python
uv sync

# 2. Dựng hạ tầng (MinIO, Postgres, Spark, Kafka, Flink, Airflow)
docker compose up -d

# 3. Sinh dữ liệu + đưa lên landing
uv run python generator/offline_generator.py
uv run python generator/upload_to_landing.py

# 4. Chạy pipeline (thủ công, hoặc trigger từ Airflow UI localhost:8083)
bash processing/spark/submit.sh ingest_bronze.py     # DP1
# ... hoặc trigger DAG dp1_ingest_bronze / dp2_silver_gold / dp3_feature_offline

# 5. Governance (DataHub — stack riêng)
bash governance/datahub/start_datahub.sh             # UI localhost:9002
```

> UI: MinIO `:9001` · Spark `:8080` · Flink `:8082` · Airflow `:8083` · DataHub `:9002`.
> Chi tiết Docker + tối ưu image (multistage, −70%): [docs/novel_ideas.md](docs/novel_ideas.md).
