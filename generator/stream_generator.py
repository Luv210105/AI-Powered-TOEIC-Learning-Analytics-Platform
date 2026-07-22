"""
stream_generator.py — Section 01 (streaming): bắn event TOEIC vào Kafka topic `toeic.events`.

Mỗi event có 2 mốc thời gian: event_timestamp (lúc sự kiện xảy ra) vs created_ts (lúc bản ghi
được tạo/gửi). CỐ TÌNH cài 3 lỗi streaming:
  - Burst        : xen kẽ khung tốc độ cao (mô phỏng spike 19-22h / trước kỳ thi).
  - Late arrival : ~12% event có event_timestamp SỚM hơn created_ts (mobile offline sync) -> out-of-order/late.
  - Duplicate    : ~1.5% event gửi lại với cùng event_id.

Tham số lỗi lấy từ generator/config.yaml; tốc độ bắn điều khiển bằng CLI (để demo nhanh).
Chạy: uv run python generator/stream_generator.py --events 5000
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml
from confluent_kafka import Producer

CONFIG = yaml.safe_load(open(Path(__file__).parent / "config.yaml", encoding="utf-8"))
TOPIC = "toeic.events"
BOOTSTRAP = "localhost:9092"

EVENT_TYPES = ["login", "lesson_opened", "lesson_completed", "question_answered",
               "flashcard_reviewed", "audio_replay", "mock_test_started", "mock_test_submitted"]


def make_event(rng: random.Random) -> dict:
    """Sinh 1 event; ~late_arrival_rate event có event_timestamp sớm hơn (late/out-of-order)."""
    # naive UTC, độ chính xác mili-giây -> ISO-8601 sạch cho Flink TIMESTAMP(3)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    if rng.random() < CONFIG["late_arrival_rate"]:
        event_ts = now - timedelta(seconds=rng.randint(60, 1800))   # xảy ra sớm 1-30 phút, tới muộn
    else:
        event_ts = now

    etype = rng.choice(EVENT_TYPES)
    ev = {
        "event_id": str(uuid.uuid4()),
        "event_type": etype,
        "event_timestamp": event_ts.isoformat(timespec="milliseconds"),
        "created_ts": now.isoformat(timespec="milliseconds"),
        "user_id": rng.randint(1, CONFIG["n_users"]),
        "session_id": f"s{rng.randint(1, CONFIG['n_users'] * 3)}",
        "device_type": rng.choice(["android", "ios", "web"]),
    }
    if etype == "question_answered":
        ev["question_id"] = rng.randint(1, CONFIG["n_questions"])
        ev["part"] = rng.randint(1, 7)
        ev["is_correct"] = rng.random() < 0.5
        ev["time_spent_sec"] = rng.randint(5, 120)
    if etype in ("mock_test_started", "mock_test_submitted"):
        ev["test_id"] = rng.randint(1, CONFIG["n_mock_tests"])
    return ev


def main() -> None:
    """Bắn event vào Kafka theo tốc độ có xen kẽ burst, kèm late + duplicate."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--events", type=int, default=5000, help="tổng số event bắn")
    ap.add_argument("--normal-eps", type=float, default=40, help="event/giây lúc bình thường")
    ap.add_argument("--burst-eps", type=float, default=400, help="event/giây lúc burst")
    args = ap.parse_args()

    rng = random.Random(CONFIG["random_seed"])
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50})
    recent: list[dict] = []   # buffer để tạo duplicate
    sent = 0

    print(f">>> Bắn {args.events} event vào {TOPIC} (normal {args.normal_eps} eps / burst {args.burst_eps} eps)")
    while sent < args.events:
        # burst 5 giây mỗi 20 giây (mô phỏng spike theo giờ)
        in_burst = int(time.time()) % 20 < 5

        ev = make_event(rng)
        producer.produce(TOPIC, key=str(ev["user_id"]), value=json.dumps(ev))
        recent.append(ev)
        recent = recent[-300:]
        sent += 1

        # duplicate: gửi lại 1 event gần đây với cùng event_id
        if rng.random() < CONFIG["duplicate_rate_stream"] and recent:
            producer.produce(TOPIC, key=str(ev["user_id"]), value=json.dumps(rng.choice(recent)))
            sent += 1

        if sent % 500 == 0:
            producer.poll(0)
            print(f"  đã bắn {sent:,} event{'  [BURST]' if in_burst else ''}")

        time.sleep(1.0 / (args.burst_eps if in_burst else args.normal_eps))

    producer.flush()
    print(f">>> XONG: đã bắn {sent:,} event (có late ~{CONFIG['late_arrival_rate']:.0%}, "
          f"duplicate ~{CONFIG['duplicate_rate_stream']:.1%}).")


if __name__ == "__main__":
    main()
