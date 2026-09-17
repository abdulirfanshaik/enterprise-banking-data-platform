"""Consume payment events from Kafka into an append-only Bronze JSONL file."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaConsumer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "banking.payments.v1"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--output", default="data/streaming_bronze/payment_events.jsonl")
    parser.add_argument("--max-records", type=int, default=0, help="0 means run continuously")
    args = parser.parse_args()

    target = Path(args.output)
    target.parent.mkdir(parents=True, exist_ok=True)
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id="banking-bronze-v1",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    count = 0
    try:
        with target.open("a", encoding="utf-8") as handle:
            for message in consumer:
                envelope = {
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                    "payload": message.value,
                }
                handle.write(json.dumps(envelope, sort_keys=True) + "\n")
                handle.flush()
                consumer.commit()
                count += 1
                if args.max_records and count >= args.max_records:
                    break
    finally:
        consumer.close()
    print(f"Consumed {count} events into {target}")


if __name__ == "__main__":
    main()
