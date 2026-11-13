# Architecture interview walkthrough: CausalMesh Cloud Lakehouse

**What did you build?** A demonstrable integration of an event-sourced settlement ledger
with a Spark/Delta analytical lakehouse. First the SQLite core accepts synthetic
transaction legs and hash-chains them. The exporter refuses to publish if audit
verification fails. Immutable JSONL lands on object storage; PySpark checks schema,
validates monetary types, deduplicates by event_id, and quarantines conflicts.

**Why bitemporal?** A correction can become known after the time it takes financial effect.
Both `effective_at` and the independent `recorded_at/sys_from/sys_to` knowledge interval
are preserved so a historical compliance question can be answered without silently
rewriting earlier beliefs.

**How do you ensure reliability?** The source ledger has atomic local DB effects. Spark
Bronze uses event_id-keyed Delta MERGE for retry-safe outcomes. ForeachBatch plus durable
checkpoints handles at-least-once source delivery, but I do not claim globally exactly-once
processing or cross-table atomicity. The system quarantines conflicts rather than guessing.

**How does it detect broken settlements?** Gold joins current debit/credit legs. A transfer
is SETTLED only when there is exactly one of each, matching minor-unit amounts/currency,
and mirrored counterparties. PENDING handles missing legs; MISMATCH covers inconsistent
paired legs. Account balances group signed minor units by currency without floating point.

**Why Databricks and AWS?** The same pure PySpark transformation core runs as local
Parquet batch or Delta-backed Databricks jobs. AWS Terraform documents S3 and IAM
boundaries. Unity Catalog external locations govern Delta paths; registration only
happens when an authorized environment is configured.

**What remains to productionize?** Integration testing of the managed cloud services;
external table setup, RBAC, KMS / audit log delivery, alerting, encrypted state backend,
Kafka TLS/auth, Delta optimization, incremental materialized aggregations, multi-table
publish manifests, and actual cluster load/failure benchmarks.

**How should you describe it on a resume?**

> Built a reproducible, synthetic settlement-intelligence reference platform integrating
> bitemporal event sourcing with Spark transformations, Delta lakehouse modeling,
> event-id deduplication, quarantined data contracts, and explainable reconciliation;
> added an AWS S3/Terraform deployment blueprint, Databricks job bundle, CI Spark tests,
> and optional Airflow/Snowflake integrations.

Do not describe cloud infrastructure or Databricks as deployed or used in production
unless you personally deploy and verify them.
