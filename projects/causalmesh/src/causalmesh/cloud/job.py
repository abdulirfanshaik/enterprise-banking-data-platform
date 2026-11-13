"""Batch Parquet/Delta and streaming Delta lakehouse job.

Databricks supplies Delta's runtime classes. Plain Spark can run portable Parquet
batch locally. Each output is a separate ACID table when using Delta; a full
multi-table atomic snapshot is NOT claimed.
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING

from causalmesh.cloud.contracts import StoragePaths, event_schema

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass(frozen=True)
class JobConfig:
    input_path: str
    output_path: str
    storage_format: str = "parquet"
    mode: str = "batch"
    checkpoint_path: str | None = None
    source: str = "files"
    kafka_bootstrap: str | None = None
    kafka_topic: str | None = None

    def __post_init__(self) -> None:
        if self.storage_format not in {"parquet", "delta"}:
            raise ValueError("storage_format must be parquet or delta")
        if self.mode not in {"batch", "stream"}:
            raise ValueError("mode must be batch or stream")
        if self.source not in {"files", "kafka"}:
            raise ValueError("source must be files or kafka")
        if self.source == "kafka" and (
            self.mode != "stream" or not self.kafka_bootstrap or not self.kafka_topic
        ):
            raise ValueError("Kafka source requires streaming, bootstrap and topic")
        if self.mode == "stream" and (self.storage_format != "delta" or not self.checkpoint_path):
            raise ValueError("stream mode requires Delta and a durable checkpoint path")
        if (self.source == "files" and not self.input_path) or not self.output_path or (
            self.source == "files" and self.input_path == self.output_path
        ):
            raise ValueError("input and output must be distinct and nonempty")


def _write(frame: DataFrame, path: str, fmt: str) -> None:
    # Table overwrite is a repeatable snapshot. Only Delta gives table-level
    # atomic visibility. These writes are not atomic across different tables.
    frame.write.format(fmt).mode("overwrite").option("overwriteSchema", "true").save(path)


def run_snapshot(raw: DataFrame, paths: StoragePaths, fmt: str = "parquet") -> dict[str, int]:
    from causalmesh.cloud.pipeline import validate_and_dedupe, build_silver, build_gold

    bronze, invalid = validate_and_dedupe(raw)
    silver, broken = build_silver(bronze)
    transfers, balances = build_gold(silver)
    quarantine = invalid.unionByName(broken, allowMissingColumns=True)
    _write(bronze, paths.dataset("bronze"), fmt)
    _write(silver, paths.dataset("silver"), fmt)
    _write(transfers, paths.dataset("gold_transfers"), fmt)
    _write(balances, paths.dataset("gold_balances"), fmt)
    _write(quarantine, paths.dataset("quarantine"), fmt)
    return {"accepted": bronze.count(), "quarantined": quarantine.count(),
            "transfers": transfers.count(), "accounts": balances.count()}


def _merge_new(delta, dataframe: DataFrame, path: str, keys: str) -> None:
    from delta.tables import DeltaTable

    spark = dataframe.sparkSession
    if DeltaTable.isDeltaTable(spark, path):
        delta.forPath(spark, path).alias("existing").merge(
            dataframe.alias("incoming"), keys
        ).whenNotMatchedInsertAll().execute()
    else:
        # Only one writer may initialize the table/checkpoint.
        dataframe.write.format("delta").mode("errorifexists").save(path)


def stream_forever(spark: SparkSession, config: JobConfig) -> None:
    """Microbatch: idempotent Bronze MERGE, then deterministic Gold projection.

    Files added to an immutable landing prefix are processed with a durable
    checkpoint. Same event IDs in later microbatches are no-ops (or quarantined
    if their payload differs). Run one active writer per output/checkpoint.
    """
    from pyspark.sql import functions as F
    from causalmesh.cloud.pipeline import validate_and_dedupe, build_silver, build_gold
    from delta.tables import DeltaTable

    p = StoragePaths(config.output_path)
    if config.source == "kafka":
        source = (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", config.kafka_bootstrap)
            .option("subscribe", config.kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
            .select(F.from_json(F.col("value").cast("string"), event_schema()).alias("event"))
            .select("event.*"))
    else:
        source = spark.readStream.schema(event_schema()).option(
            "maxFilesPerTrigger", 1
        ).json(config.input_path)

    def process_batch(batch: DataFrame, _batch_id: int) -> None:
        # Retried foreachBatch invocations are safe because MERGE identities are stable.
        if batch.isEmpty():
            return
        accepted, invalid = validate_and_dedupe(batch)
        bronze_path = p.dataset("bronze")
        if DeltaTable.isDeltaTable(spark, bronze_path):
            prior = spark.read.format("delta").load(bronze_path).select("event_id", "payload_sha")
            collisions = (accepted.alias("a").join(prior.alias("p"), "event_id", "inner")
                .where(F.col("a.payload_sha") != F.col("p.payload_sha"))
                .select("a.*").withColumn("reject_reason", F.lit("CROSS_BATCH_ID_COLLISION")))
            invalid = invalid.unionByName(collisions, allowMissingColumns=True)
        if not accepted.isEmpty():
            _merge_new(DeltaTable, accepted, bronze_path,
                       "existing.event_id = incoming.event_id")
        if not DeltaTable.isDeltaTable(spark, bronze_path):
            if not invalid.isEmpty():
                q = invalid.withColumn("quarantine_key", F.sha2(F.concat_ws(
                    "|", F.coalesce(F.col("event_id"), F.lit("")),
                    F.coalesce(F.col("payload_sha"), F.lit("")),
                    F.col("reject_reason")
                ), 256)).dropDuplicates(["quarantine_key"])
                _merge_new(DeltaTable, q, p.dataset("quarantine"),
                           "existing.quarantine_key = incoming.quarantine_key")
            return
        bronze_all = spark.read.format("delta").load(bronze_path)
        silver, broken = build_silver(bronze_all)
        transfers, balances = build_gold(silver)
        _write(silver, p.dataset("silver"), "delta")
        _write(transfers, p.dataset("gold_transfers"), "delta")
        _write(balances, p.dataset("gold_balances"), "delta")
        quarantine = invalid.unionByName(broken, allowMissingColumns=True)
        if not quarantine.isEmpty():
            q = quarantine.withColumn("quarantine_key", F.sha2(F.concat_ws(
                "|", F.coalesce(F.col("event_id"), F.lit("")),
                F.coalesce(F.col("payload_sha"), F.lit("")),
                F.col("reject_reason")
            ), 256)).dropDuplicates(["quarantine_key"])
            _merge_new(DeltaTable, q, p.dataset("quarantine"),
                       "existing.quarantine_key = incoming.quarantine_key")

    query = (source.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", config.checkpoint_path)
        .queryName("causalmesh_bronze_ingest")
        .start())
    query.awaitTermination()


def register_unity_catalog(spark: SparkSession, output_uri: str, catalog: str,
                           schema: str) -> None:
    """Register materialized Delta locations in an *existing* governed UC namespace.

    Requires an admin-created Unity Catalog external location covering output_uri.
    Names are strictly identifier validated to avoid dynamic SQL injection.
    """
    import re
    for name in (catalog, schema):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
            raise ValueError("invalid Unity Catalog identifier")
    if not output_uri.startswith("s3://") or "'" in output_uri:
        raise ValueError("Unity Catalog registration requires a safe S3 URI")
    for table in ("bronze", "silver", "gold_transfers", "gold_balances", "quarantine"):
        uri = StoragePaths(output_uri).dataset(table)
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS `{catalog}`.`{schema}`.`causalmesh_{table}` "
            f"USING DELTA LOCATION '{uri}'"
        )


def run(config: JobConfig, catalog: str | None = None,
        schema: str | None = None) -> dict[str, object]:
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.appName("CausalMeshLakehouse")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate())
    try:
        if config.mode == "stream":
            stream_forever(spark, config)
            return {"mode": "stream", "status": "terminated"}
        raw = spark.read.schema(event_schema()).json(config.input_path)
        report = run_snapshot(raw, StoragePaths(config.output_path), config.storage_format)
        if catalog or schema:
            if not (catalog and schema and config.storage_format == "delta"):
                raise ValueError("Unity Catalog registration requires --catalog, --schema and Delta")
            register_unity_catalog(spark, config.output_path, catalog, schema)
        return {"mode": "batch", **report}
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="CausalMesh distributed lakehouse job")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--format", choices=("parquet", "delta"), default="parquet")
    parser.add_argument("--mode", choices=("batch", "stream"), default="batch")
    parser.add_argument("--checkpoint")
    parser.add_argument("--source", choices=("files", "kafka"), default="files")
    parser.add_argument("--kafka-bootstrap")
    parser.add_argument("--kafka-topic")
    parser.add_argument("--catalog")
    parser.add_argument("--schema")
    args = parser.parse_args()
    cfg = JobConfig(args.input, args.output, args.format, args.mode, args.checkpoint,
                    args.source, args.kafka_bootstrap, args.kafka_topic)
    print(json.dumps(run(cfg, args.catalog, args.schema), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
