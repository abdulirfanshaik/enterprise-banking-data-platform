"""Gold marts for daily regulatory totals and rule-based alert candidates."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver", default="lake/silver")
    parser.add_argument("--output", default="lake/gold")
    parser.add_argument("--high-value", type=float, default=200000)
    args = parser.parse_args()
    spark = SparkSession.builder.appName("banking-gold-models").getOrCreate()
    payments = spark.read.parquet(f"{args.silver}/payments")
    regulatory = (payments.withColumn("event_date", F.to_date("event_timestamp"))
        .groupBy("event_date", "payment_type")
        .agg(F.count("*").alias("transaction_count"),
             F.sum("amount").alias("transaction_value"),
             F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
             F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_count")))
    alerts = (payments.filter(F.col("amount") >= F.lit(args.high_value))
        .withColumn("alert_id", F.concat(F.lit("ALERT-"), F.col("payment_id")))
        .withColumn("rule_name", F.lit("HIGH_VALUE"))
        .withColumn("risk_score", F.lit(30)))
    regulatory.write.mode("overwrite").partitionBy("event_date").parquet(f"{args.output}/regulatory_daily")
    alerts.write.mode("overwrite").parquet(f"{args.output}/aml_fraud_alerts")
    spark.stop()


if __name__ == "__main__":
    main()
