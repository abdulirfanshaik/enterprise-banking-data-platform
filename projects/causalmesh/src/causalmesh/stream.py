"""Optional Redpanda/Kafka adapter: at-least-once transport + idempotent DB effects."""
from __future__ import annotations

import json
import os
from typing import Any

from causalmesh.engine import Engine

TOPIC = "causalmesh.ledger.v1"


def producer(bootstrap: str, events: list[dict[str, Any]]) -> int:
    from kafka import KafkaProducer

    client = KafkaProducer(
        bootstrap_servers=bootstrap, key_serializer=lambda x: x.encode(),
        value_serializer=lambda x: json.dumps(x).encode(), acks="all", retries=3,
    )
    try:
        for event in events:
            client.send(TOPIC, key=event.get("transfer_id", "unknown"), value=event)
        client.flush(timeout=60)
    finally:
        client.close()
    return len(events)


def consume(engine: Engine, bootstrap: str, group: str = "causalmesh-projection") -> None:
    from kafka import KafkaConsumer

    client = KafkaConsumer(
        TOPIC, bootstrap_servers=bootstrap, group_id=group, enable_auto_commit=False,
        auto_offset_reset="earliest", value_deserializer=lambda b: json.loads(b.decode()),
        consumer_timeout_ms=1000,
    )
    try:
        while True:
            for message in client:
                result = engine.ingest(message.value)
                # Quarantine is an explicit durable outcome; no poison-message infinite loop.
                with engine.db.transaction() as conn:
                    conn.execute(
                        """INSERT INTO consumer_offsets(topic,partition_no,last_offset)
                           VALUES(?,?,?) ON CONFLICT(topic,partition_no)
                           DO UPDATE SET last_offset=max(last_offset,excluded.last_offset)""",
                        (message.topic, message.partition, message.offset),
                    )
                client.commit()
                print(json.dumps({"offset": message.offset, "result": result["status"]}), flush=True)
            if os.environ.get("CAUSALMESH_ONCE") == "1":
                break
    finally:
        client.close()
