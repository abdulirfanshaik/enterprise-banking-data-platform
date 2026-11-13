"""Optional Kafka producer for accepted-event JSONL envelopes.

Requires `pip install -e '.[stream]'`; use the local Redpanda compose profile.
"""
import argparse
import json
from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--bootstrap", default="localhost:19092")
    parser.add_argument("--topic", default="causalmesh.accepted.v1")
    args = parser.parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda data: json.dumps(data, sort_keys=True).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all", retries=5,
    )
    with open(args.input, encoding="utf-8") as source:
        for line in source:
            if line.strip():
                doc = json.loads(line)
                producer.send(args.topic, key=doc["event_id"], value=doc).get(timeout=20)
    producer.flush()
    producer.close()


if __name__ == "__main__":
    main()
