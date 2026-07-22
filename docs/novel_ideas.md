# Novel Ideas

Hai ý tưởng ngoài chương trình EDAI, có document + proof. Xem [project_proposal.md §15](../project_proposal.md).

---

## Idea 1 — `uv` + multistage Docker build (lean & reproducible images)

**Ý tưởng:** dùng **`uv`** (trình quản lý package Python thế hệ mới, viết bằng Rust) thay `pip`/`venv`
cho toàn bộ dependency management, kết hợp **multistage Docker build** + base image slim để tạo image
service Python **nhỏ + reproducible**.

### Kỹ thuật áp dụng
- **`uv sync --frozen --no-dev`**: cài đúng theo `uv.lock` (fail nếu lock lệch → **reproducible**), bỏ dev deps.
- **Multistage** ([`docker/Dockerfile.app`](../docker/Dockerfile.app)): stage `builder` cài deps vào `.venv`;
  stage runtime **chỉ copy `.venv` + code** (không mang theo `uv`, build layer, cache).
- **Base slim** (`python:3.14-slim`) thay vì full.
- **`.dockerignore`**: loại `data/`, `.venv`, `.git`, `docs/images/`, `.env` khỏi build context.

### Proof — đo image size (before → after)

| Image | Cách build | Size |
|-------|-----------|-----:|
| `toeic-app:naive` | single-stage, base `python:3.14` đầy đủ, có dev deps | **2.18 GB** |
| `toeic-app:optimized` | multistage, `python:3.14-slim`, `uv sync --frozen --no-dev` | **651 MB** |
| **Giảm** | | **~70% (−1.53 GB)** |

```bash
docker build -f docker/Dockerfile.app.naive -t toeic-app:naive .
docker build -f docker/Dockerfile.app       -t toeic-app:optimized .
docker images toeic-app     # naive 2.18GB vs optimized 651MB
```

### Vì sao "novel"
`uv` (2024+) chưa nằm trong chương trình EDAI (vốn dùng pip/venv); kết hợp multistage + `--frozen` cho
build **nhanh, nhỏ, reproducible** — thực hành DevOps/MLOps hiện đại.

---

## Idea 2 — DataHub deploy bypass (vượt lỗi quickstart do môi trường)

**Vấn đề gặp thực tế:** `datahub docker quickstart` (CLI chính thức) **KHÔNG chạy được** trên máy dev vì
**Docker Compose là v5.x** (quá mới) so với thứ quickstart CLI hỗ trợ (v2.x): CLI mới tự sinh compose có
bug broker DNS, CLI cũ không nhận ra compose v5.x. Không liên quan RAM/pipeline.

**Giải pháp (novel):** **bypass CLI hoàn toàn** — chạy **compose CHÍNH THỨC v1.4.0.3 trực tiếp** bằng
`docker compose` v2, kèm chuỗi fix môi trường:

| Vấn đề | Fix |
|--------|-----|
| CLI không hiểu Compose v5.x | Chạy `docker compose` v2 trực tiếp (bỏ CLI), loại `/snap/bin` (docker-compose v1 rởm) |
| `datahub-kafka-setup:<ver>` không publish | Đổi image sang tag `:head` |
| Confluent 8.x bỏ zookeeper | Ép Confluent **7.9.2** (còn zookeeper) |
| OpenSearch không hợp es-setup | Dùng **Elasticsearch 7.10.1** (default compose) |
| Port đụng main stack (8080/9092/8081) | Remap GMS 8085 / broker 9095 / schema 8086 |
| Setup jobs kẹt "Created" | Start thủ công + up lại |

Đóng gói thành script [`governance/datahub/start_datahub.sh`](../governance/datahub/start_datahub.sh).

### Proof
- DataHub UI chạy được (`localhost:9002`), GMS `localhost:8085` — xem [governance.md](governance.md) (metadata + lineage + data contract).
- DataHub là **stack docker riêng**, song song main stack (đúng chuẩn production tách governance).

### Vì sao "novel"
Đây là **problem-solving thực tế**: chẩn đoán tầng sâu (Compose version, image packaging, service naming,
port) và tự dựng lại quy trình deploy khi công cụ chính thức hỏng — kỹ năng vận hành ngoài chương trình EDAI.
