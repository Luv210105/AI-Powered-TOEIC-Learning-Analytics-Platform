#!/usr/bin/env bash
#
# start_datahub.sh — dựng DataHub (stack RIÊNG, chạy song song main stack).
#
# LƯU Ý: DataHub quickstart CLI KHÔNG chạy được ở môi trường này (Docker Compose v5.x quá mới +
# bug broker của bản mới). Cách chạy được là BYPASS CLI: chạy compose CHÍNH THỨC v1.4.0.3 trực
# tiếp bằng `docker compose` v2, với các fix bên dưới. Xem docs/governance.md.
#
# Nếu container DataHub CÒN TỒN TẠI (chỉ bị stop): dùng `docker compose -p datahub start` cho nhanh.
# Script này để dựng LẠI từ đầu khi container đã bị xóa.

set -euo pipefail
QS=~/.datahub/quickstart

# 1) Đảm bảo có compose chính thức v1.4.0.3 (CLI fetch về rồi fail ở bước detect — kệ, ta lấy file)
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin"   # loại /snap/bin (docker-compose v1 rởm)
datahub docker quickstart --version v1.4.0 >/dev/null 2>&1 || true

cd "$QS"

# 2) Vá: kafka-setup chỉ có tag :head (bản version không publish)
sed -i 's|\${DATAHUB_KAFKA_SETUP_IMAGE:-acryldata/datahub-kafka-setup}:\${DATAHUB_VERSION:-head}|acryldata/datahub-kafka-setup:head|' docker-compose.yml || true

# 3) Env: image v1.4.0.3, Elasticsearch 7.10.1 (KHÔNG OpenSearch), Confluent 7.9.2 (còn zookeeper),
#    remap port tránh main stack (spark-master 8080, kafka 9092, spark-worker 8081)
export DATAHUB_VERSION=v1.4.0.3 \
       DATAHUB_MAPPED_GMS_PORT=8085 \
       DATAHUB_MAPPED_KAFKA_BROKER_PORT=9095 \
       DATAHUB_MAPPED_SCHEMA_REGISTRY_PORT=8086
unset DATAHUB_SEARCH_IMAGE DATAHUB_SEARCH_TAG DATAHUB_CONFLUENT_VERSION 2>/dev/null || true
set -a; source .local-secrets.env; set +a

# 4) Dựng. Setup jobs (mysql/es/kafka-setup) có thể kẹt "Created" -> start tay rồi up lại.
docker compose -p datahub -f docker-compose.yml up -d
sleep 10
for job in datahub-mysql-setup-1 datahub-elasticsearch-setup-1 datahub-kafka-setup-1; do
  docker start "$job" >/dev/null 2>&1 || true
done
sleep 20
docker compose -p datahub -f docker-compose.yml up -d

echo ">>> DataHub UI: http://localhost:9002 (login datahub/datahub) | GMS: http://localhost:8085"
