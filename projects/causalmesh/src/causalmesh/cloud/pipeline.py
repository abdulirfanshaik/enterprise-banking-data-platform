"""Spark DataFrame transformations, intentionally separate from I/O and credentials.

The source is an immutable, audit-verified accepted-event export. This module
also validates independently because other producers may write the landing zone.
All monetary calculations use integral minor units; no currency conversion is implied.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F
from causalmesh.cloud.contracts import PAYLOAD_FIELDS


def validate_and_dedupe(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Return accepted Bronze rows and non-replay contract violations.

    Replayed identical event IDs collapse to one row. Conflicting payloads using
    the same event ID quarantine *all* variants, rather than selecting arbitrarily.
    """
    payload = [F.coalesce(F.col(k).cast("string"), F.lit("<NULL>")) for k in PAYLOAD_FIELDS]
    src = (raw
        .withColumn("recorded_ts", F.try_to_timestamp("recorded_at"))
        .withColumn("effective_ts", F.try_to_timestamp("effective_at"))
        .withColumn("payload_sha", F.sha2(F.concat_ws("|", *payload), 256)))
    reason = (F.when(F.col("event_id").isNull() | (F.length("event_id") == 0), "MISSING_EVENT_ID")
        .when(F.col("seq").isNull() | (F.col("seq") <= 0), "BAD_SEQUENCE")
        .when(F.col("recorded_ts").isNull() | F.col("effective_ts").isNull(), "BAD_TIMESTAMP")
        .when(F.col("schema_version") != 1, "UNSUPPORTED_SCHEMA_VERSION")
        .when(F.col("schema_version").isNull(), "UNSUPPORTED_SCHEMA_VERSION")
        .when(F.col("revision").isNull() | (F.col("revision") < 1), "BAD_REVISION")
        .when(F.col("amount_minor").isNull() | (F.col("amount_minor") <= 0), "BAD_AMOUNT")
        .when(F.col("side").isNull() | (~F.col("side").isin("DEBIT", "CREDIT")), "BAD_SIDE")
        .when(F.col("currency").isNull() | (~F.col("currency").rlike("^[A-Z]{3}$")), "BAD_CURRENCY")
        .when(
            F.coalesce(
                F.col("leg_id").isNull() | (F.length("leg_id") == 0)
                | F.col("transfer_id").isNull() | (F.length("transfer_id") == 0)
                | F.col("account_id").isNull() | (F.length("account_id") == 0)
                | F.col("counterparty_id").isNull()
                | (F.length("counterparty_id") == 0), F.lit(True)
            ), "BAD_IDENTITY"
        ))
    checked = src.withColumn("reject_reason", reason)
    valid = checked.filter(F.col("reject_reason").isNull())
    variants = valid.groupBy("event_id").agg(
        F.countDistinct("payload_sha").alias("variant_count")
    )
    identified = valid.join(variants, "event_id", "left")
    conflict = identified.filter(F.col("variant_count") > 1).withColumn(
        "reject_reason", F.lit("EVENT_ID_PAYLOAD_COLLISION")
    )
    safe = identified.filter(F.col("variant_count") == 1)
    order = Window.partitionBy("event_id").orderBy("recorded_ts", "seq")
    bronze = (safe.withColumn("_rank", F.row_number().over(order))
        .filter(F.col("_rank") == 1)
        .drop("_rank", "variant_count", "reject_reason"))
    quarantine = checked.filter(F.col("reject_reason").isNotNull()).unionByName(
        conflict.drop("variant_count"), allowMissingColumns=True
    )
    return bronze, quarantine


def build_silver(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Construct bitemporal revisions. Fail closed on broken revision sequences.

    This is a deterministic snapshot computation, not a mutable SCD2 update.
    """
    w = Window.partitionBy("leg_id").orderBy("recorded_ts", "seq", "event_id")
    full = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    numbered = (bronze
        .withColumn("expected_revision", F.row_number().over(w))
        .withColumn("original_transfer", F.first("transfer_id").over(full))
        .withColumn("original_account", F.first("account_id").over(full))
        .withColumn("original_counterparty", F.first("counterparty_id").over(full))
        .withColumn("original_side", F.first("side").over(full))
        .withColumn("original_currency", F.first("currency").over(full))
    )
    good = (
        (F.col("revision") == F.col("expected_revision"))
        & (F.col("transfer_id") == F.col("original_transfer"))
        & (F.col("account_id") == F.col("original_account"))
        & (F.col("counterparty_id") == F.col("original_counterparty"))
        & (F.col("side") == F.col("original_side"))
        & (F.col("currency") == F.col("original_currency"))
    )
    intermediate = ["expected_revision", "original_transfer", "original_account",
                    "original_counterparty", "original_side", "original_currency"]
    issues = (numbered.filter(~good).drop(*intermediate)
        .withColumn("reject_reason", F.lit("INVALID_REVISION_OR_IDENTITY")))
    accepted = numbered.filter(good).drop(*intermediate)
    # Re-window after removing bad rows, so a quarantined revision does not
    # artificially close the previous valid system-time interval.
    silver = (accepted.withColumn("sys_from", F.col("recorded_ts"))
        .withColumn("sys_to", F.lead("recorded_ts").over(w)))
    return silver, issues


def as_of(silver: DataFrame, business_at: str, knowledge_at: str) -> DataFrame:
    """Point-in-time ledger: state known at knowledge_at, effective by business_at."""
    b, k = F.to_timestamp(F.lit(business_at)), F.to_timestamp(F.lit(knowledge_at))
    return silver.filter(
        (F.col("effective_ts") <= b) & (F.col("sys_from") <= k)
        & (F.col("sys_to").isNull() | (F.col("sys_to") > k))
    )


def build_gold(silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Reconcile mirrored settlement legs and publish account balances.

    Gold contains the latest *known* revisions, not a historical as-of snapshot.
    """
    current = silver.filter(F.col("sys_to").isNull())

    def side_summary(side: str, prefix: str) -> DataFrame:
        return current.filter(F.col("side") == side).groupBy("transfer_id").agg(
            F.count("*").alias(f"{prefix}_count"),
            F.max("amount_minor").alias(f"{prefix}_amount"),
            F.max("currency").alias(f"{prefix}_currency"),
            F.max("account_id").alias(f"{prefix}_account"),
            F.max("counterparty_id").alias(f"{prefix}_counterparty"),
            F.max("event_id").alias(f"{prefix}_event_id"),
        )

    debit = side_summary("DEBIT", "d")
    credit = side_summary("CREDIT", "c")
    joined = debit.join(credit, "transfer_id", "full_outer").fillna(0, ["d_count", "c_count"])
    balanced = (
        (F.col("d_count") == 1) & (F.col("c_count") == 1)
        & (F.col("d_amount") == F.col("c_amount"))
        & (F.col("d_currency") == F.col("c_currency"))
        & (F.col("d_account") == F.col("c_counterparty"))
        & (F.col("c_account") == F.col("d_counterparty"))
    )
    transfers = (joined.withColumn("status", F.when(balanced, "SETTLED")
        .when((F.col("d_count") == 0) | (F.col("c_count") == 0), "PENDING")
        .otherwise("MISMATCH"))
        .withColumn("difference_minor", F.when(
            (F.col("d_count") == 1) & (F.col("c_count") == 1),
            F.col("d_amount") - F.col("c_amount")
        )))

    signed = current.withColumn(
        "signed_minor", F.when(F.col("side") == "DEBIT", -F.col("amount_minor"))
        .otherwise(F.col("amount_minor"))
    )
    balances = signed.groupBy("account_id", "currency").agg(
        F.sum("signed_minor").alias("balance_minor"),
        F.count("*").alias("active_leg_count"),
        F.max("recorded_ts").alias("last_known_at"),
    )
    return transfers, balances
