"""
flink_stream.py — Phase 7.3: Flink job xử lý streaming từ Kafka topic toeic.events.

Xử lý 3 lỗi streaming:
  - Late / out-of-order : dùng EVENT-TIME + WATERMARK (event_timestamp - 5s). Event tới muộn quá
                          watermark bị coi là late (Flink UI: numLateRecordsDropped).
  - Duplicate           : trong mỗi window, so COUNT(*) với COUNT(DISTINCT event_id) -> số trùng.
  - Burst               : cờ f_burst_flag khi số event trong window vượt ngưỡng.

WINDOWING: TUMBLE 30s trên event-time, tính f_correct_rate (tỷ lệ trả lời đúng) — đại diện cho
f_correct_rate_60m (rút ngắn để demo). Kết quả in ra print sink (log TaskManager).

Submit: docker compose exec flink-jobmanager flink run -py /opt/flink/jobs/flink_stream.py
Xem: Flink UI http://localhost:8082
"""

from pyflink.table import EnvironmentSettings, TableEnvironment

BOOTSTRAP = "kafka:29092"   # listener nội bộ (Flink trong cùng mạng compose)
BURST_THRESHOLD = 1500      # số event/window coi là burst (minh họa)


def main() -> None:
    """Dựng pipeline Kafka -> window aggregation -> print."""
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "2")

    # --- Source: Kafka + event-time + watermark ---
    t_env.execute_sql(f"""
        CREATE TABLE events (
            event_id STRING,
            event_type STRING,
            event_timestamp TIMESTAMP(3),
            created_ts TIMESTAMP(3),
            user_id INT,
            question_id INT,
            part INT,
            is_correct BOOLEAN,
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'toeic.events',
            'properties.bootstrap.servers' = '{BOOTSTRAP}',
            'properties.group.id' = 'flink-toeic',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.timestamp-format.standard' = 'ISO-8601',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    # --- Sink: in kết quả window ra log ---
    t_env.execute_sql("""
        CREATE TABLE window_stats (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            n_events BIGINT,
            n_unique_events BIGINT,
            n_duplicate BIGINT,
            f_correct_rate DOUBLE,
            f_burst_flag BOOLEAN
        ) WITH ('connector' = 'print')
    """)

    # --- Windowing: TUMBLE 30s trên event-time ---
    t_env.execute_sql(f"""
        INSERT INTO window_stats
        SELECT
            window_start,
            window_end,
            COUNT(*)                                   AS n_events,
            COUNT(DISTINCT event_id)                   AS n_unique_events,
            COUNT(*) - COUNT(DISTINCT event_id)        AS n_duplicate,
            ROUND(
                SUM(CASE WHEN event_type = 'question_answered' AND is_correct THEN 1 ELSE 0 END) * 1.0
                / NULLIF(SUM(CASE WHEN event_type = 'question_answered' THEN 1 ELSE 0 END), 0), 3
            )                                          AS f_correct_rate,
            COUNT(*) > {BURST_THRESHOLD}               AS f_burst_flag
        FROM TABLE(TUMBLE(TABLE events, DESCRIPTOR(event_timestamp), INTERVAL '30' SECOND))
        GROUP BY window_start, window_end
    """)


if __name__ == "__main__":
    main()
