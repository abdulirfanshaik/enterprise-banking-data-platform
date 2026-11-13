"""Canonical event contract shared by batch, streaming, and the independent tests.

No Spark dependency on import. The schema is constructed only when Spark is installed.
"""
from __future__ import annotations

from dataclasses import dataclass

PAYLOAD_FIELDS = (
    "event_id", "leg_id", "transfer_id", "revision", "account_id",
    "counterparty_id", "side", "currency", "amount_minor",
    "effective_at", "schema_version",
)


@dataclass(frozen=True)
class StoragePaths:
    root: str

    def dataset(self, name: str) -> str:
        if name not in {"bronze", "silver", "gold_transfers", "gold_balances", "quarantine"}:
            raise ValueError("unknown lakehouse dataset")
        return self.root.rstrip("/") + "/" + name


def event_schema():
    from pyspark.sql.types import (
        IntegerType, LongType, StringType, StructField, StructType,
    )

    return StructType([
        StructField("seq", LongType()),
        StructField("recorded_at", StringType()),
        StructField("event_hash", StringType()),
        StructField("prior_hash", StringType()),
        StructField("event_id", StringType()),
        StructField("leg_id", StringType()),
        StructField("transfer_id", StringType()),
        StructField("revision", IntegerType()),
        StructField("account_id", StringType()),
        StructField("counterparty_id", StringType()),
        StructField("side", StringType()),
        StructField("currency", StringType()),
        StructField("amount_minor", LongType()),
        StructField("effective_at", StringType()),
        StructField("schema_version", IntegerType()),
    ])
