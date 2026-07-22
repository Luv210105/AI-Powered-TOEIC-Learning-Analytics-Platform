# Rubric Coverage Map (tự chấm)

Đối chiếu từng hạng mục rubric mini-coursework với deliverable + bằng chứng đã có.
Tất cả ✅ = có code/doc/ảnh minh chứng.

| # | Hạng mục rubric | Điểm | Deliverable | Bằng chứng | ✅ |
|---|-----------------|:----:|-------------|-----------|:--:|
| 1 | README + deployment diagram | 10 | `README.md`, [architecture.md](architecture.md) | Repo structure + ToC + diagram 12 luồng | ✅ |
| 2 | Docker & Docker Compose + optimize | 2 | `docker-compose.yml`, [Dockerfile.app](../docker/Dockerfile.app) | multistage: 2.18GB→651MB ([novel_ideas](novel_ideas.md)) | ✅ |
| 3 | Generator offline (skew/card/schema-evo/dup + config + store) | ~12 | [`offline_generator.py`](../generator/offline_generator.py), [config.yaml](../generator/config.yaml) | [data_generator.md](data_generator.md) — DQ report đo 4 lỗi | ✅ |
| 4 | Generator streaming (burst/late/dup + config) | ~8 | [`stream_generator.py`](../generator/stream_generator.py) | event 2 timestamp + 3 lỗi; [flink_streaming.md](flink_streaming.md) | ✅ |
| 5 | Spark jobs (baseline→optimize + Airflow) | ~12 | `processing/spark/opt_*.py`, ingest/silver/gold | [spark_optimization.md](spark_optimization.md) — Spark UI before/after ×4 | ✅ |
| 6 | Flink jobs (burst/late/other + window) | ~10 | [`flink_stream.py`](../processing/flink/flink_stream.py) | [flink_streaming.md](flink_streaming.md) — windowing code + Flink UI | ✅ |
| 7 | Storage optimization (lakehouse + DWH) | 4 | `opt_storage_lakehouse.py`, `warehouse_index.py` | [storage_optimization.md](storage_optimization.md) — file 356→178, Seq→Index scan | ✅ |
| 8 | Orchestration Airflow (DP1/DP2/DP3, ingest+validate) | 12 | [`dags/`](../pipelines/airflow/dags/) | [orchestration.md](orchestration.md) — 3 DAG xanh + connections/variables | ✅ |
| 9 | Governance DataHub (lineage + data contract) | 12 | [`governance/datahub/`](../governance/datahub/) | [governance.md](governance.md) — 8 dataset + lineage + assertion PASS | ✅ |
| 10 | Documentation (SCD2, feat_, ERD, naming, 3 zone) | 8 | [schema_design.md](schema_design.md) | ERD (FK) + SCD2 + feat_ 2 timestamp + Bronze/Silver/Gold | ✅ |
| 11 | Novel ideas ×2 | 10 | [novel_ideas.md](novel_ideas.md) | uv/multistage (image −70%) + DataHub bypass | ✅ |
| | **Tổng** | **100** | | | |

## Điểm nhấn từng lỗi dữ liệu (đều đo được)

| Lỗi | Nơi cài | Nơi xử lý + bằng chứng |
|-----|---------|------------------------|
| Offline: skew | generator | Spark: broadcast join 7.7× |
| Offline: high cardinality | generator | Spark: approx_count_distinct, shuffle 10.5MiB→2.7KiB |
| Offline: schema evolution | partition cũ thiếu cột | Spark: fillna, 474k NULL→0 |
| Offline: duplicate | 2% answers | Spark: dropDuplicates 46k→0 |
| Streaming: burst | 19-22h spike | Flink: f_burst_flag |
| Streaming: late/out-of-order | ~12% event | Flink: event-time + watermark |
| Streaming: duplicate | 1.5% event | Flink: n_events vs distinct event_id |

## Checklist cuối
- [x] 3 data pipeline (DP1/DP2/DP3), mỗi cái ingest + validate
- [x] `dim_user` SCD2; `feat_user_90d` có event_timestamp + created_ts
- [x] ERD quan hệ dim–fact (FK thật); naming `raw_/stg_/dim_/fact_/obt_/feat_`
- [x] Bảng ở cả 3 zone (Bronze/Silver/Gold) xem được
- [x] `uv` (pyproject.toml + uv.lock) + Docker multistage đo image reduction
- [x] 2 novel ideas có proof
