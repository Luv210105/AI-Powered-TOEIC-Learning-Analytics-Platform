"""
flink_to_bronze.py — Phase 7.4: ghi stream RAW vào Bronze (MinIO).

- Đọc toeic.events từ Kafka.
- Ghi RAW (giữ nguyên lỗi, kể cả duplicate) ra s3a://bronze/stream_events/ (JSON, partition theo ngày).

LƯU Ý kiến trúc: Bronze = raw nên KHÔNG dedup ở đây (giống offline: Bronze giữ dup, Silver mới dedup).
Việc dedup theo event_id đã được DEMO ở flink_stream.py (window đếm n_duplicate) và sẽ áp ở streaming Silver.

Cần checkpointing để filesystem sink commit file (chuyển in-progress -> finished).
Submit: docker compose exec flink-jobmanager flink run -d -py /opt/flink/jobs/flink_to_bronze.py
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = "kafka:29092"


def main() -> None:
    """Kafka -> dedup theo event_id -> ghi Bronze (MinIO) dạng JSON partition theo ngày."""
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    # checkpoint 30s để filesystem sink finalize file trên MinIO
    t_env.get_config().set("execution.checkpointing.interval", "30 s")
    t_env.get_config().set("parallelism.default", "2")

    # --- Source: Kafka + event-time (dùng cho dedup keep-first) ---
    t_env.execute_sql(f"""
        CREATE TABLE events (
            event_id STRING,
            event_type STRING,
            event_timestamp TIMESTAMP(3),
            created_ts TIMESTAMP(3),
            user_id INT,
            question_id INT,
            part INT,
            is_correct BOOLEAN
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'toeic.events',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink-bronze',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # --- Sink: Bronze trên MinIO (filesystem s3a, JSON, partition theo ngày) ---
    t_env.execute_sql("""
        CREATE TABLE bronze_stream_events (
            event_id STRING,
            event_type STRING,
            event_timestamp TIMESTAMP(3),
            created_ts TIMESTAMP(3),
            user_id INT,
            question_id INT,
            part INT,
            is_correct BOOLEAN,
            dt STRING
        ) PARTITIONED BY (dt) WITH (
            'connector' = 'filesystem',
            'path' = 's3a://bronze/stream_events/',
            'format' = 'json',
            'sink.rolling-policy.rollover-interval' = '30 s',
            'sink.rolling-policy.check-interval' = '10 s'
        )
    """)

    # --- Ghi RAW event vào Bronze (append-only, giữ nguyên lỗi) ---
    t_env.execute_sql("""
        INSERT INTO bronze_stream_events
        SELECT event_id, event_type, event_timestamp, created_ts, user_id, question_id, part, is_correct,
               DATE_FORMAT(event_timestamp, 'yyyy-MM-dd') AS dt
        FROM events
    """)


if __name__ == "__main__":
    main()
