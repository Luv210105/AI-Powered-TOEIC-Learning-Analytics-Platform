# Dockerfile.app — image cho service Python tự viết (generator + processing).
# TỐI ƯU: multistage + base slim + uv sync --frozen --no-dev (đúng lock, bỏ dev deps).
# So sánh với Dockerfile.app.naive (single-stage, base đầy đủ) để đo image reduction.

# ---- Stage 1: builder — cài deps bằng uv vào .venv ----
FROM python:3.14-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
WORKDIR /app
COPY pyproject.toml uv.lock ./
# cài đúng uv.lock, bỏ dev deps, fail nếu lock lệch -> reproducible
RUN uv sync --frozen --no-dev --no-install-project

# ---- Stage 2: runtime — chỉ copy .venv + code (không có uv, build tools) ----
FROM python:3.14-slim
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH"
COPY generator/ ./generator/
COPY processing/ ./processing/
COPY main.py ./
CMD ["python", "main.py"]
