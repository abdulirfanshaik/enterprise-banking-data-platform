"""Bronze ingestion: preserve source values and append audit metadata."""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="lake/bronze")
    args = parser.parse_args()
    spark = SparkSession.builder.appName("banking-bronze-ingest").getOrCreate()
    sources = {
        "upi": ("json", f"{args.input}/upi/*.jsonl"),
        "neft": ("csv", f"{args.input}/neft/*.csv"),
        "rtgs": ("csv", f"{args.input}/rtgs/*.csv"),
        "cards": ("json", f"{args.input}/cards/*.jsonl"),
    }
    for source_name, (file_type, path) in sources.items():
        reader = spark.read.option("header", True) if file_type == "csv" else spark.read
        frame = reader.format(file_type).load(path)
        frame = (frame.withColumn("_source_system", F.lit(source_name.upper()))
                      .withColumn("_source_file", F.input_file_name())
                      .withColumn("_ingested_at", F.current_timestamp()))
        frame.write.format("parquet").mode("overwrite").save(f"{args.output}/{source_name}")
    spark.stop()


if __name__ == "__main__":
    main()
