# Process Guide — AI-Powered TOEIC Learning Analytics Platform (Mini-Coursework)

> Tài liệu này là **lộ trình thực hiện chi tiết** cho phần mini-coursework (Section 01 + 02, 100% Data Engineering).
> Mục tiêu: một người mới (newbie) đọc và **tự làm theo từng bước để vừa hoàn thành coursework vừa hiểu kiến thức**.
> Tài liệu **không chứa code** — chỉ mô tả việc cần làm, khái niệm cần học, và bằng chứng cần nộp.
> Xem thiết kế tổng quan ở [project_proposal.md](project_proposal.md). Tham chiếu điểm số ở §17 của project_proposal.md.

---

## Cách đọc tài liệu này

Mỗi **Phase** có cấu trúc cố định:
- 🎯 **Mục tiêu** — làm xong phase này thì có gì.
- 📚 **Cần học trước** — khái niệm/công cụ phải nắm để không làm mò.
- 🔨 **Các bước làm** — trình tự thao tác cụ thể.
- ✅ **Bằng chứng (proof)** — thứ phải chụp/lưu để được chấm điểm.
- ⚠️ **Bẫy thường gặp** — lỗi newbie hay mắc.

Nguyên tắc vàng cho newbie: **làm từng phase một, mỗi phase phải "chạy được + có bằng chứng" rồi mới sang phase sau.** Đừng dựng hết mọi thứ cùng lúc.

---

## Bản đồ phụ thuộc (làm theo thứ tự này)

```
Phase 0  Chuẩn bị nền tảng (kiến thức + repo + uv + git)
   ↓
Phase 1  Dựng hạ tầng tối thiểu bằng Docker Compose (MinIO + PostgreSQL)
   ↓
Phase 2  Section 01 — Data Generator (offline Parquet)
   ↓
Phase 3  Lưu data nguồn vào MinIO / PostgreSQL (landing)
   ↓
Phase 4  Section 02 — Thiết kế schema Bronze/Silver/Gold trên giấy
   ↓
Phase 5  Spark job: ingest raw → Bronze + xử lý lỗi offline (DP1)
   ↓
Phase 6  Spark job: Bronze → Silver → Gold (DP2) + tối ưu storage
   ↓
Phase 7  Section 01 (streaming) + Flink job xử lý streaming
   ↓
Phase 8  Feature table offline (DP3)
   ↓
Phase 9  Orchestration: gói DP1/DP2/DP3 vào Airflow
   ↓
Phase 10 Governance: lineage + data contract trên DataHub
   ↓
Phase 11 Documentation + Schema visualization + Diagram + README
   ↓
Phase 12 Docker hoàn thiện + Novel ideas + Rà soát rubric
```

> Gợi ý thời lượng (tham khảo, ~4–6 tuần part-time): Phase 0–3 tuần 1, Phase 4–6 tuần 2, Phase 7–8 tuần 3, Phase 9–10 tuần 4, Phase 11–12 tuần 5.

---

## Phase 0 — Chuẩn bị nền tảng

🎯 **Mục tiêu:** có repo trống đúng cấu trúc, môi trường chạy được, và hiểu mình sắp xây cái gì.

📚 **Cần học trước:**
- **Data Lakehouse là gì** và mô hình **Bronze → Silver → Gold** (raw → sạch → sẵn sàng dùng). Đây là xương sống của cả project.
- **Batch vs Streaming** khác nhau thế nào.
- **Git cơ bản** (commit, branch), **Docker là gì** (container, image), **Docker Compose** (chạy nhiều container cùng lúc).
- **`uv`** quản lý môi trường Python (xem project_proposal.md §6, §13).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Fundamentals of Engineering – Linux* (Bash, Git nằm ngoài chương trình nhưng cùng nhóm setup môi trường), *Fundamentals of Engineering – Python* (`uv`, virtual environment).

🔨 **Các bước làm:**
1. Tạo repo Git theo đúng cấu trúc thư mục ở [project_proposal.md §6](project_proposal.md). Tạo trước các folder rỗng + file `README.md`, `docs/` (chưa cần nội dung).
2. Khởi tạo môi trường Python bằng `uv` (`uv init`), tạo `pyproject.toml`. Chưa cần thêm thư viện vội — thêm dần khi cần.
3. Cài Docker + Docker Compose trên máy. Chạy thử một container hello-world để chắc chắn Docker hoạt động.
4. Viết một `README.md` nháp gồm: tên project, 1 đoạn mô tả domain TOEIC, và mục lục (sẽ hoàn thiện ở Phase 11).
5. Đọc kỹ lại [project_proposal.md](project_proposal.md) một lượt để nắm bức tranh tổng thể.

✅ **Bằng chứng:** repo có cấu trúc thư mục đúng + commit đầu tiên.

⚠️ **Bẫy:** đừng cố hiểu hết mọi công cụ ngay. Hiểu *vai trò* từng công cụ trong sơ đồ ([project_proposal.md §4–5](project_proposal.md)) là đủ để bắt đầu.

---

## Phase 1 — Dựng hạ tầng tối thiểu bằng Docker Compose

🎯 **Mục tiêu:** có **MinIO** (object storage) và **PostgreSQL** chạy được bằng `docker compose up`.

📚 **Cần học trước:**
- **MinIO** = bản tự host của Amazon S3, lưu file dạng object trong "bucket". Ở đây dùng làm nơi chứa data lake (Parquet, Delta).
- **PostgreSQL** = CSDL quan hệ. Ở đây đóng 2 vai: (1) mô phỏng "dữ liệu ở department khác" mình cần kéo về, (2) kho Gold cho BI.
- Khái niệm **service / port / volume** trong Docker Compose.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Fundamentals of Engineering – Containerization & Orchestration* (Docker, Docker Compose, volume/network), *Fundamentals of Engineering – Database* (SQL/PostgreSQL cơ bản), *Data Engineering – Storage Layer* (MinIO nằm trong techstack của lesson này, dù dùng sớm hơn thứ tự curriculum).

🔨 **Các bước làm:**
1. Viết `docker-compose.yml` với 2 service đầu tiên: `minio` và `postgres`. Khai báo port, user/password (qua biến môi trường, để trong `.env`), và volume để dữ liệu không mất khi tắt container.
2. Chạy `docker compose up`, mở giao diện web của MinIO (console) để chắc chắn vào được.
3. Tạo sẵn vài bucket trong MinIO: ví dụ `landing`, `bronze`, `silver`, `gold`.
4. Kết nối thử vào PostgreSQL bằng **DBeaver** để chắc chắn truy cập được.

✅ **Bằng chứng:** ảnh chụp MinIO console (thấy các bucket) + DBeaver kết nối được Postgres.

⚠️ **Bẫy:** **không hardcode mật khẩu** trong file commit lên Git — để trong `.env` (và `.env` phải nằm trong `.gitignore`). Commit `.env.example` thôi.

> 💡 Các service nặng hơn (Kafka, Spark, Flink, Airflow, DataHub) sẽ **thêm dần ở các phase sau**, không dựng hết một lúc để tránh quá tải máy và khó debug.

---

## Phase 2 — Section 01: Data Generator (offline)

🎯 **Mục tiêu:** sinh được bộ dữ liệu TOEIC offline dạng Parquet, **có chủ đích cài sẵn các lỗi dữ liệu**.

📚 **Cần học trước:**
- **Parquet** = định dạng file dạng cột, nén tốt, chuẩn cho data lake.
- **Partition** = chia dữ liệu thành thư mục con theo một cột (ví dụ theo ngày) để query nhanh hơn.
- 4 loại lỗi offline phải mô phỏng: **skew, high cardinality, schema evolution, duplicate** (định nghĩa ở [project_proposal.md §7.2](project_proposal.md)).
- **Random seed** = số cố định để mỗi lần chạy ra cùng kết quả (reproducible).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Fundamentals of Engineering – Python* (file handling, argparse, data structures để viết generator), *Data Engineering – Storage Layer* (khái niệm Data Lake/Parquet được giới thiệu ở đây dù dùng sớm hơn).

🔨 **Các bước làm:**
1. Thiết kế **file config** cho generator (xem mẫu [project_proposal.md §7.5](project_proposal.md)): số học viên, số câu hỏi, số ngày lịch sử, các tỷ lệ skew/duplicate, ngày đổi schema, seed... Để mọi tham số ở đây, **không hardcode trong code**.
2. Thiết kế cơ chế sinh **bảng nền** trước: `users`, `questions`, `vocabulary`, `grammar_topics`, `mock_tests` ([project_proposal.md §7.1](project_proposal.md)).
3. Sinh **dữ liệu giao dịch**: `test_attempts` và `attempt_answers`. Dùng công thức điểm đơn giản hóa (`P(correct) = sigmoid(base_ability − difficulty + noise)`, [project_proposal.md §7.1](project_proposal.md)) — cho `base_ability` tăng dần theo thời gian để tạo "tín hiệu học tập".
4. **Cài lỗi có chủ đích** vào dữ liệu:
   - *Skew*: cho ~80% học viên rơi vào mục tiêu 600–700; câu Part 5 chiếm tỷ trọng lớn nhất.
   - *High cardinality*: các ID gần như duy nhất (tự nhiên đã vậy, chỉ cần kiểm chứng).
   - *Schema evolution*: với partition trước `schema_change_date`, **bỏ hẳn cột** `tag_grammar` và `difficulty`.
   - *Duplicate*: nhân đôi ~2% dòng trong `attempt_answers`.
5. Ghi ra Parquet, **partition theo `attempt_date` / `signup_date`**.

✅ **Bằng chứng (rất quan trọng — đây là phần nhiều điểm):** một **Data Quality Report** ([project_proposal.md §7.6](project_proposal.md)) gồm: phân bố skew (%), số lượng distinct theo ID, số null ở partition cũ (chứng minh schema evolution), tỷ lệ duplicate. Chụp màn hình từng cái kèm chú thích.

⚠️ **Bẫy:** đừng tạo dữ liệu "quá sạch". Lỗi là *yêu cầu*, không phải sự cố. Phải chứng minh được mình **cố tình** tạo ra chúng và **đo được** chúng.

---

## Phase 3 — Lưu data nguồn vào MinIO / PostgreSQL (landing)

🎯 **Mục tiêu:** mô phỏng "dữ liệu đang nằm ở nơi khác, mình cần kéo về" — bước chuẩn bị cho ingest Bronze.

📚 **Cần học trước:**
- Tại sao tách "nơi lưu nguồn" khỏi "Bronze": để mô phỏng thực tế dữ liệu đến từ hệ thống khác (department khác, app khác).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Ingestion Layer* (khái niệm "Source systems: Files, CDC, OLAP&OLTP, Logs" chính là lý do tách landing khỏi Bronze).

🔨 **Các bước làm:**
1. Upload các file Parquet đã sinh lên bucket `landing` của MinIO.
2. Đồng thời, nạp **một vài bảng** (ví dụ `users`, `questions`) vào **PostgreSQL** như một "source database" — để sau này thực hành kéo dữ liệu từ cả 2 loại nguồn (file + DB).

✅ **Bằng chứng:** ảnh chụp file trong bucket MinIO + bảng trong Postgres (DBeaver).

⚠️ **Bẫy:** giữ nguyên dữ liệu lỗi khi đưa lên landing — **không sửa lỗi ở đây**. Việc xử lý lỗi là nhiệm vụ của Spark/Flink ở các phase sau.

---

## Phase 4 — Section 02: Thiết kế schema Bronze/Silver/Gold (trên giấy)

🎯 **Mục tiêu:** có **bản thiết kế schema** rõ ràng trước khi code pipeline. Đây là bước "nghĩ trước, làm sau".

📚 **Cần học trước:**
- Ý nghĩa từng zone: **Bronze** = raw y nguyên + metadata ingest; **Silver** = đã làm sạch/dedup/chuẩn hóa; **Gold** = mô hình hóa cho phân tích (dim/fact/OBT/feature).
- **Dimension vs Fact** (mô hình sao — star schema): dim = mô tả thực thể, fact = sự kiện đo lường được.
- **SCD2** (Slowly Changing Dimension type 2): lưu lịch sử thay đổi bằng `valid_from_ts`, `valid_to_ts`, `is_current`.
- **Surrogate key (SK) vs Business key (BK)**.
- **Naming convention**: `raw_`, `stg_`, `dim_`, `fact_`, `obt_`, `feat_`.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Storage Layer* (Data warehouse & modeling, Lake House, Multi-hop architecture — chính là gốc của mô hình Bronze/Silver/Gold), *Data Engineering – Consumption Layer* (Dimensional modeling, SCD cho phần thiết kế dim/fact).

🔨 **Các bước làm:**
1. Liệt kê **input profile** (theo yêu cầu): dữ liệu nào từ Phase 2, key columns, volume ước lượng, các lỗi đã biết.
2. Thiết kế bảng từng zone:
   - *Bronze*: bản sao raw + cột metadata (`ingest_ts`, `source`, `batch_id`).
   - *Silver*: bảng đã sạch (`stg_attempt_answers`...), khóa dedup rõ ràng.
   - *Gold*: `dim_user` (SCD2), `dim_question`, `dim_date`...; `fact_question_attempt`, `fact_mock_test_result`; `obt_student_learning_performance` ([project_proposal.md §8](project_proposal.md)).
3. Thiết kế **feature table** `feat_user_90d` với **`event_timestamp` + `created_ts`** và quy tắc point-in-time ([project_proposal.md §8.4](project_proposal.md)).
4. Vẽ **ERD** (sơ đồ quan hệ dim–fact) — có thể vẽ tay rồi sau export lại bằng DBeaver.
5. Định nghĩa **data quality checks** cho từng bảng: uniqueness, referential, null.

✅ **Bằng chứng:** tài liệu `docs/schema_design.md` mô tả từng bảng + ERD + naming convention + chiến lược SCD2.

⚠️ **Bẫy:** đừng nhảy vào code pipeline khi chưa chốt schema. Sửa schema sau khi đã build pipeline rất tốn công.

---

## Phase 5 — Spark job: ingest raw → Bronze + xử lý lỗi offline

🎯 **Mục tiêu:** có Spark job kéo data từ landing vào **Bronze (Delta trên MinIO)**, rồi xử lý các lỗi offline với bằng chứng tối ưu.

📚 **Cần học trước:**
- **Apache Spark** là gì (xử lý dữ liệu lớn phân tán), khái niệm **DataFrame**, **transformation vs action**, **shuffle**.
- **Delta Lake**: bảng trên data lake có ACID, hỗ trợ `mergeSchema`, time travel.
- **Spark UI**: cách đọc stage/task để thấy bottleneck (đặc biệt task bị lệch do skew).
- Kỹ thuật xử lý skew: **salting, broadcast join, AQE (Adaptive Query Execution)**.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Transformation Layer (1)* (Spark architecture & execution, DataFrame/RDD, Catalyst Optimizer, AQE, expensive shuffle/join strategies/data skew, Spark UI analysis — đúng trọng tâm phase này).

🔨 **Các bước làm:**
1. Thêm service **Spark** vào `docker-compose.yml`.
2. Viết job **ingest**: đọc Parquet từ `landing` → ghi ra Bronze dưới dạng **Delta**, thêm cột metadata ingest. Đây là **DP1 (ingest stage)**.
3. Với mỗi lỗi offline, làm theo quy trình **baseline → quan sát → tối ưu → đo lại** ([project_proposal.md §9.1](project_proposal.md)):
   - Chạy **baseline** (cách ngây thơ, chưa tối ưu) → mở **Spark UI** chụp lại chỗ chậm/lệch.
   - Áp dụng kỹ thuật xử lý (vd skew → salting/broadcast) → chạy lại → chụp Spark UI cho thấy đã cải thiện.
   - Ghi lại: "lỗi thế này → thấy gì trên Spark UI → thử cách gì → kết quả nhanh hơn bao nhiêu so với baseline".
4. Xử lý: **skew**, **high cardinality** (repartition hợp lý), **schema evolution** (`mergeSchema` + điền default), **duplicate** (dedup theo (attempt_id, question_id)).
5. Thêm **validate stage** sau ingest: kiểm tra schema/null/uniqueness. Đây là phần **validate** của DP1.

✅ **Bằng chứng:** `docs/spark_optimization.md` gồm: từng lỗi + screenshot Spark UI baseline vs sau tối ưu + số liệu before/after + giải thích.

⚠️ **Bẫy:** phải có **baseline để so sánh**. Nếu chỉ trình bày "cách đúng" mà không cho thấy "cách chưa tối ưu chậm thế nào" thì mất điểm phần giải thích.

---

## Phase 6 — Spark job: Bronze → Silver → Gold + tối ưu storage

🎯 **Mục tiêu:** dựng bảng Gold (dim/fact/OBT) từ Silver, và tối ưu lưu trữ.

📚 **Cần học trước:**
- Cách **build dimension** (đặc biệt **SCD2**: phát hiện thay đổi, đóng dòng cũ, mở dòng mới) và **fact** (gắn khóa, tính measure).
- Kỹ thuật tối ưu lakehouse: **compaction** (gom file nhỏ), **Z-ORDER** (sắp xếp dữ liệu theo cột hay lọc), **partitioning**.
- Tối ưu warehouse: **index** trong PostgreSQL, đọc **EXPLAIN ANALYZE**.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Transformation Layer (1)* (Spark, tiếp tục dùng để build Silver/Gold), *Data Engineering – Storage Layer* (Lake House, multi-hop — cho phần compaction/Z-ORDER/partitioning), *Fundamentals of Engineering – Database* (index, EXPLAIN ANALYZE trong PostgreSQL cho phần tối ưu warehouse).

🔨 **Các bước làm:**
1. Viết job **Silver**: từ Bronze làm sạch, chuẩn hóa kiểu dữ liệu, dedup → ghi Delta Silver. Đây là một phần **DP2**.
2. Viết job **Gold**: build `dim_*` (có SCD2 cho `dim_user`), `fact_*`, `obt_*` → ghi sang **PostgreSQL** (Gold warehouse). Phần còn lại của **DP2**.
3. **Tối ưu storage** ([project_proposal.md §10](project_proposal.md)):
   - Lakehouse: chạy compaction/Z-ORDER/partition trên bảng Delta lớn → đo số file & thời gian query trước/sau.
   - Warehouse: thêm index trên cột hay filter/join trong Postgres → so sánh runtime bằng EXPLAIN ANALYZE.
4. Thêm **validate stage** cho DP2.

✅ **Bằng chứng:** `docs/storage_optimization.md` theo format *Workload → Bottleneck → Optimization → Result (before/after) → Trade-off* ([project_proposal.md §10](project_proposal.md)).

⚠️ **Bẫy:** SCD2 dễ làm sai. Kiểm tra kỹ: một học viên đổi `target_score` phải tạo **dòng mới** với `is_current=true` và **đóng** dòng cũ (`valid_to_ts`, `is_current=false`).

---

## Phase 7 — Section 01 (streaming) + Flink job xử lý streaming

🎯 **Mục tiêu:** có luồng sự kiện qua Kafka và Flink job xử lý lỗi streaming + windowing.

📚 **Cần học trước:**
- **Kafka**: topic, producer, consumer — hàng đợi message cho streaming.
- **Event time vs processing time**, **watermark**, **allowed lateness** — cách hệ thống streaming xử lý dữ liệu đến muộn/sai thứ tự.
- **Windowing** (tumbling/sliding window) — gom sự kiện theo cửa sổ thời gian để tính toán.
- 3 lỗi streaming: **burst, late arrival/out-of-order, duplicate** ([project_proposal.md §7.4](project_proposal.md)).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Ingestion Layer* (The Kafka Ecosystem, producer/consumer internals, partitioning, topic configuration — cho phần bắn/nhận event), *Data Engineering – Transformation Layer (2)* (Flink APIs, execution modes & notions of time, windows strategies, late data handling, watermark, state management, backpressure — đúng trọng tâm phase này).

🔨 **Các bước làm:**
1. Thêm **Kafka** và **Flink** vào `docker-compose.yml`.
2. Hoàn thiện phần **stream generator** ([project_proposal.md §7.3](project_proposal.md)): bắn event vào topic `toeic.events`, có phân biệt **`event_timestamp` vs `created_ts`**, và cài sẵn lỗi: burst (19:00–22:00 + 2 tuần trước kỳ thi), late arrival ~12%, duplicate ~1.5%.
3. Viết Flink job (baseline → tối ưu, dùng **Flink UI** làm bằng chứng — [project_proposal.md §9.2](project_proposal.md)):
   - **Burst**: quan sát backpressure trên Flink UI → chỉnh parallelism.
   - **Late/out-of-order**: cấu hình event-time + **watermark** + allowed lateness → đo số bản ghi muộn.
   - **Duplicate**: dedup theo `event_id` trong window.
   - **Window processing**: tính ví dụ `f_correct_rate_60m`, cờ burst bằng tumbling/sliding window.
4. Ghi kết quả stream đã làm sạch vào Bronze (streaming).

✅ **Bằng chứng:** `docs/flink_streaming.md` + screenshot Flink UI + **đoạn code thể hiện window** (rubric yêu cầu chụp phần windowing).

⚠️ **Bẫy:** đừng nhầm `event_timestamp` (lúc sự kiện xảy ra) với `created_ts` (lúc bản ghi được tạo/đến hệ thống). Watermark phải dựa trên **event time**.

---

## Phase 8 — Feature table offline (DP3)

🎯 **Mục tiêu:** có pipeline tính bảng feature offline với point-in-time đúng.

📚 **Cần học trước:**
- **Feature store / feature table** là gì, vì sao cần `event_timestamp` (point-in-time join) và `created_ts` (dedup).
- **Point-in-time correctness**: không được dùng feature có timestamp muộn hơn thời điểm cần dự đoán (tránh data leakage).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Consumption Layer* (Feature store: Feature core, Feature view design, Feature transformation, ML integration — dùng công cụ Feast), kết hợp lại với *Transformation Layer (1)* (Spark) để viết job tính feature.

🔨 **Các bước làm:**
1. Viết Spark job tính `feat_user_90d` ([project_proposal.md §8.4](project_proposal.md)): `f_accuracy_by_part_90d`, `f_listening_vs_reading_gap`, `f_questions_answered_90d`, `f_mock_test_count_90d`.
2. Đảm bảo mỗi dòng feature có `event_timestamp` + `created_ts`, dedup giữ `created_ts` mới nhất.
3. Thêm **validate stage**. Đây là **DP3**.

✅ **Bằng chứng:** bảng `feat_user_90d` xem được trong DBeaver, có đủ 2 cột timestamp.

⚠️ **Bẫy:** rolling window 90 ngày phải tính **tại từng mốc thời gian**, không phải tính một lần cho toàn bộ lịch sử.

---

## Phase 9 — Orchestration: gói DP1/DP2/DP3 vào Airflow

🎯 **Mục tiêu:** 3 pipeline chạy tự động, có thứ tự, theo lịch — quản lý tập trung.

📚 **Cần học trước:**
- **Airflow**: **DAG**, **task**, **dependency**, **schedule**, **retry**.
- **Connections & Variables** trong Airflow — để cấu hình (host/credential) tập trung, tái dùng giữa các pipeline.
- Khái niệm **idempotency** (chạy lại không tạo dữ liệu trùng) và **retry/backoff**.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Orchestration Layer* (Pipeline orchestration benefits, idempotency/backfill/dynamic pipelines, Airflow architecture/operators/data passing, administration & scaling — đúng trọng tâm phase này).

🔨 **Các bước làm:**
1. Thêm **Airflow** vào `docker-compose.yml`.
2. Đưa **tất cả connection & variable** (MinIO, Postgres, Kafka...) vào Airflow, **không hardcode trong DAG**.
3. Tạo 3 DAG ([project_proposal.md §11](project_proposal.md)):
   - **DP1**: ingest raw → Bronze (ingest stage → validate stage).
   - **DP2**: Bronze → Silver → Gold (ingest → validate).
   - **DP3**: tính feature offline (ingest → validate).
4. Mỗi DAG: cấu hình retry + backoff, ghi run metadata (run_id, số dòng in/out, trạng thái).
5. Chạy thử từng DAG trên Airflow UI, xem các stage chạy đúng thứ tự.

✅ **Bằng chứng:** screenshot Airflow UI thể hiện các stage và thứ tự của **từng** DP1/DP2/DP3.

⚠️ **Bẫy:** mỗi DAG phải có **2 stage rõ ràng (ingest + validate)** — rubric chấm riêng từng stage.

---

## Phase 10 — Governance: lineage + data contract trên DataHub

🎯 **Mục tiêu:** nhìn được dữ liệu chảy từ đâu đến đâu (lineage) và ràng buộc chất lượng (data contract).

📚 **Cần học trước:**
- **Data lineage**: bản đồ "bảng nào sinh ra từ bảng nào, qua pipeline nào".
- **Data contract**: cam kết về schema + chất lượng (uniqueness, null, freshness) của một dataset.
- **Metadata ingestion**: cách DataHub thu thập thông tin từ các nguồn.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Data Engineering – Orchestration Layer* (phần "Fundamentals of data validation: circuit breaker pattern, validation frameworks and data contracts" và techstack DataHub nằm ngay trong lesson này, cùng nhóm với Phase 9).

🔨 **Các bước làm:**
1. Thêm **DataHub** vào `docker-compose.yml`.
2. Cấu hình ingest metadata từ Postgres/MinIO vào DataHub.
3. Với mỗi DP1/DP2/DP3, thiết lập **lineage** giữa pipeline và các bảng liên quan (Source → Bronze → Silver → Gold → Feature).
4. Gắn **data validation + data contract** cho các bảng chính.

✅ **Bằng chứng:** screenshot DataHub UI thể hiện lineage + validation + data contract cho từng DP. Tổng hợp ở `docs/governance.md`.

⚠️ **Bẫy:** DataHub khá nặng và nhiều thành phần — dựng riêng, test riêng, đừng để chung lần đầu với mọi service khác kẻo khó tìm lỗi.

---

## Phase 11 — Documentation + Schema visualization + Diagram + README

🎯 **Mục tiêu:** đóng gói toàn bộ thành tài liệu mạch lạc cho người chấm.

📚 **Cần học trước:**
- Cách viết README tốt: **Repo Structure + Table of Contents**, README chỉ **summarize** và link tới `docs/`.
- Quy tắc vẽ diagram của coursework (xem đầu PDF rubric / [project_proposal.md §5](project_proposal.md)): mỗi node là **deployable unit**, mũi tên đi theo **luồng dữ liệu**, có **đánh số + mô tả**, hạn chế nét đứt.

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** không có lesson riêng — đây là bước tổng hợp lại kiến thức của *Storage Layer* (zone Bronze/Silver/Gold), *Consumption Layer* (dim/fact/SCD2), dùng **DBeaver** (công cụ đã dùng từ *Fundamentals of Engineering – Database*) để export ERD.

🔨 **Các bước làm:**
1. Hoàn thiện **deployment diagram** chi tiết trong `docs/architecture.md` (mở rộng từ sơ đồ [project_proposal.md §5](project_proposal.md)): tách màu cho data flow vs developer flow, đánh số từng luồng.
2. Hoàn thiện **README.md**: mô tả domain, Repo Structure, Table of Contents, nhúng diagram, link tới mọi file trong `docs/`.
3. **Schema visualization** ([project_proposal.md §14](project_proposal.md)): export ERD quan hệ dim–fact từ **DBeaver**; chụp bảng `dim_user` có SCD2; chụp bảng `feat_` có `event_timestamp` + `created_ts`; chụp **bảng ở cả 3 zone** (Bronze/Silver/Gold).
4. Đảm bảo **mọi hàm/class có docstring**, **mỗi file có mô tả đầu file** (rubric chấm).
5. Mỗi screenshot trong docs phải có **chú thích mục đích**, không để ảnh rời rạc.

✅ **Bằng chứng:** README hoàn chỉnh + thư mục `docs/` đầy đủ + các ảnh ERD/zone/SCD2.

⚠️ **Bẫy:** README **không** nhồi mọi chi tiết — chi tiết để trong `docs/`, README chỉ tóm tắt và dẫn link.

---

## Phase 12 — Docker hoàn thiện + Novel ideas + Rà soát rubric

🎯 **Mục tiêu:** toàn hệ thống chạy bằng một lệnh, có điểm sáng tạo, và đối chiếu đủ rubric.

📚 **Cần học trước:**
- **Multistage Docker build** và cách giảm kích thước image (xem [project_proposal.md §13](project_proposal.md)).
- Công cụ cho novel idea: ví dụ **Airbyte** (ingestion), **Great Expectations** (data quality), hoặc dùng **`uv`** làm điểm tối ưu build ([project_proposal.md §15](project_proposal.md)).

📖 **Bài học liên quan (EDAI-1 curriculum, xlsx):** *Fundamentals of Engineering – Containerization & Orchestration* (multistage build, tối ưu image — có trong "A comprehensive guideline to build, run, optimize and manage containers"), *Data Engineering – Orchestration Layer* (validation frameworks, cùng nhóm với Great Expectations/data contract cho novel ideas).

🔨 **Các bước làm:**
1. Viết **Dockerfile multistage** cho các service Python tự viết, cài deps bằng `uv sync --frozen --no-dev`. **Đo và ghi lại** image giảm từ X MB → Y MB.
2. Hoàn thiện `docker-compose.yml` để `docker compose up` dựng được toàn bộ stack.
3. Triển khai **2 novel ideas** ([project_proposal.md §15](project_proposal.md)) — chọn cái bạn thực sự dựng được + có proof chạy thật (vd Airbyte cho ingestion, Great Expectations cho validation).
4. **Rà soát Rubric Coverage Map** ([project_proposal.md §17](project_proposal.md)): đi từng dòng, đánh dấu đã có bằng chứng chưa. Phần nào thiếu → bổ sung.
5. Viết **run instructions** ngắn gọn trong README để reviewer tự reproduce.

✅ **Bằng chứng:** `docker compose up` chạy được; `docs/novel_ideas.md` có 2 ý tưởng + proof; bảng rubric tự chấm.

⚠️ **Bẫy:** đừng để novel idea đến phút chót. Nếu một idea quá khó dựng, chuyển sớm sang phương án dễ hơn (Soda Core, dbt test... — [project_proposal.md §15](project_proposal.md)).

---

## Checklist tổng (tự chấm trước khi nộp)

**Data Generator**
- [ ] Offline: skew, high cardinality, schema evolution, duplicate — đều đo được
- [ ] Streaming: burst, late arrival, duplicate — đều đo được
- [ ] Config file điều khiển mọi tham số + random seed
- [ ] Data Quality Report có screenshot

**Processing**
- [ ] Spark: baseline → tối ưu từng lỗi + Spark UI
- [ ] Flink: burst/late/duplicate + window + Flink UI
- [ ] Storage optimization: lakehouse (compaction/Z-order/partition) + DWH (index), có before/after

**Pipeline & Governance**
- [ ] DP1/DP2/DP3 trên Airflow, mỗi cái có ingest + validate stage
- [ ] Connections & variables nằm trong Airflow
- [ ] DataHub: lineage + data contract cho từng DP

**Schema & Docs**
- [ ] dim_user có SCD2; feat_ có event_timestamp + created_ts
- [ ] ERD quan hệ dim–fact; naming convention đúng
- [ ] Bảng ở cả 3 zone xem được
- [ ] README có Repo Structure + ToC + diagram; docstring đầy đủ

**Engineering & Novel**
- [ ] Docker Compose chạy toàn stack; Dockerfile multistage + đo image reduction
- [ ] Dùng `uv` quản môi trường (pyproject.toml + uv.lock)
- [ ] 2 novel ideas có proof
- [ ] Đối chiếu xong Rubric Coverage Map ([project_proposal.md §17](project_proposal.md))

---

## Lời khuyên học tập cho newbie

1. **Chạy được trước, đẹp sau.** Mỗi phase ưu tiên "có kết quả + bằng chứng", tối ưu sau.
2. **Ghi chép khi làm**, không để cuối mới viết docs — screenshot ngay lúc thấy vấn đề trên Spark/Flink UI.
3. **Hiểu *tại sao*, không chỉ *làm gì*.** Mỗi lỗi dữ liệu và mỗi kỹ thuật xử lý đều phản ánh một vấn đề thật trong data engineering — đó mới là thứ đáng mang vào CV/phỏng vấn.
4. **Commit nhỏ, thường xuyên.** Dễ quay lui khi hỏng.
5. **Đừng dựng hết hạ tầng cùng lúc.** Thêm service theo phase để dễ cô lập lỗi.
