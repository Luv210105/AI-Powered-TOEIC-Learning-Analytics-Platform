# AI-Powered TOEIC Learning Analytics Platform

> **Trạng thái:** Nháp (Phase 0) — README sẽ được hoàn thiện đầy đủ ở Phase 11 (diagram, repo structure, docstring...). Xem lộ trình chi tiết ở [process.md](process.md) và thiết kế tổng quan ở [project_proposal.md](project_proposal.md).

## Giới thiệu

Nhiều sinh viên cần chứng chỉ TOEIC để tốt nghiệp hoặc ứng tuyển việc làm, nhưng phần lớn nền tảng luyện thi hiện nay chỉ cung cấp tài liệu và đề luyện, chưa khai thác dữ liệu học tập để cá nhân hóa lộ trình.

Project này xây dựng một **Data Platform** cho domain TOEIC: sinh dữ liệu luyện thi (offline + streaming), ingest và xử lý qua Spark/Flink, lưu trữ theo mô hình lakehouse Bronze → Silver → Gold, orchestrate bằng Airflow, và quản trị dữ liệu bằng DataHub. Mini-coursework tập trung 100% vào Data Engineering (Section 01 + 02); final coursework sẽ mở rộng sang ML (dự đoán điểm TOEIC) và LLM (AI Tutor).

## Mục lục

- [project_proposal.md](project_proposal.md) — Project Proposal: bối cảnh, mục tiêu, tech stack, deployment diagram, repository structure, thiết kế schema, pipeline, rubric coverage map.
- [process.md](process.md) — Process Guide: lộ trình thực hiện chi tiết theo từng phase.
- `docs/` — tài liệu chi tiết cho từng phần (sẽ bổ sung dần qua các phase):
  - `architecture.md` — deployment diagram chi tiết
  - `data_generator.md` — Section 01 write-up
  - `schema_design.md` — Section 02: SCD2, ERD, naming convention
  - `spark_optimization.md` — baseline → optimize + Spark UI
  - `flink_streaming.md` — windowing + Flink UI
  - `storage_optimization.md` — compaction/Z-order/indexing
  - `governance.md` — DataHub lineage + data contract
  - `novel_ideas.md` — novel ideas + proof

## Run instructions (sẽ hoàn thiện ở Phase 12)

```bash
uv sync
docker compose up
```
