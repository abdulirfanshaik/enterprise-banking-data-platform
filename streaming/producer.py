"""Publish generated UPI/card events to Kafka.

Install kafka-python and start the Docker services before running:
PYTHONPATH=src python streaming/producer.py --source data/raw/upi/upi_transactions.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True)
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "banking.payments.v1"))
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--delay-ms", type=int, default=10)
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
        enable_idempotence=True,
    )
    published = 0
    try:
        with Path(args.source).open(encoding="utf-8") as handle:
            for line in handle:
                event = json.loads(line)
                payment_id = str(event.get("upi_transaction_id") or event.get("authorization_id") or "unknown")
                producer.send(args.topic, key=payment_id, value=event)
                published += 1
                if args.delay_ms:
                    time.sleep(args.delay_ms / 1000)
        producer.flush()
    finally:
        producer.close()
    print(f"Published {published} events to {args.topic}")


if __name__ == "__main__":
    main()
