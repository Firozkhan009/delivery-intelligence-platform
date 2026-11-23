import argparse
import json
import time
import uuid
from collections import Counter

from confluent_kafka import Consumer

from dip.config import load_config


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max", type=int, default=100000)
    parser.add_argument("--timeout", type=int, default=15)
    args = parser.parse_args()

    cfg = load_config()
    consumer = Consumer({
        **cfg.kafka.client_config(),
        "group.id": f"check-{uuid.uuid4().hex[:8]}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([cfg.kafka.topic])

    types, ids = Counter(), Counter()
    total = bad = null_values = 0
    deadline = time.time() + args.timeout
    try:
        while total < args.max and time.time() < deadline:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print("consumer error:", msg.error())
                continue
            total += 1
            try:
                event = json.loads(msg.value())
                types[event["event_type"]] += 1
                ids[event["event_id"]] += 1
                if event.get("order_value") is None and event["event_type"] == "order_created":
                    null_values += 1
            except (ValueError, KeyError):
                bad += 1
    finally:
        consumer.close()

    duplicates = sum(v - 1 for v in ids.values() if v > 1)
    print(f"messages={total} unparseable={bad} duplicates={duplicates} null_order_value={null_values}")
    for name, count in sorted(types.items()):
        print(f"  {name}: {count}")


if __name__ == "__main__":
    main()