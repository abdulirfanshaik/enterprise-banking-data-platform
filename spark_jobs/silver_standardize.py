"""Silver standardization using explicit source mappings and quality rejects."""
from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession, functions as F


def canonical(frame: DataFrame, source: str) -> DataFrame:
    mappings = {
        "upi": ("upi_transaction_id", "payer_account", "payee_account", "amount_inr", "transaction_timestamp", "transaction_status"),
        "neft": ("utr_number", "sender_account", "beneficiary_account", "amount", "transaction_date", "status"),
        "rtgs": ("utr_number", "sender_account", "beneficiary_account", "amount", "value_date", "status"),
        "cards": ("authorization_id", "account_number_token", "merchant_account", "authorized_amount", "authorization_time", "response_code"),
    }
    payment_id, source_account, destination_account, amount, event_time, status = mappings[source]
    status_expr = F.when(F.col(status) == "00", "SUCCESS").otherwise("FAILED") if source == "cards" else F.upper(F.col(status))
    return frame.select(
        F.col(payment_id).cast("string").alias("payment_id"),
        F.lit(f"SIMULATED_{source.upper()}").alias("source_system"),
        F.col(source_account).cast("string").alias("source_account"),
        F.col(destination_account).cast("string").alias("destination_account"),
        F.lit(source.upper()).alias("payment_type"),
        F.col(amount).cast("decimal(18,2)").alias("amount"),
        F.lit("INR").alias("currency"),
        F.to_timestamp(F.col(event_time)).alias("event_timestamp"),
        status_expr.alias("status"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingestion_timestamp"),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze", default="lake/bronze")
    parser.add_argument("--output", default="lake/silver")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("banking-silver-standardize").getOrCreate()
    frames = [canonical(spark.read.parquet(f"{args.bronze}/{source}"), source) for source in ("upi", "neft", "rtgs", "cards")]
    payments = frames[0]
    for frame in frames[1:]:
        payments = payments.unionByName(frame, allowMissingColumns=True)
    valid = (F.col("payment_id").isNotNull() & F.col("source_account").isNotNull()
             & F.col("destination_account").isNotNull() & F.col("amount").isNotNull()
             & (F.col("amount") > 0) & F.col("event_timestamp").isNotNull())
    payments.filter(~valid).write.mode("overwrite").parquet(f"{args.output}/rejects")
    (payments.filter(valid)
             .dropDuplicates(["source_system", "payment_id"])
             .write.mode("overwrite").partitionBy("payment_type").parquet(f"{args.output}/payments"))
    spark.stop()


if __name__ == "__main__":
    main()
