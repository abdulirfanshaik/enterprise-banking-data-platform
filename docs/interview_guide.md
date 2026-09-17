# Interview guide

## Truthful 60–90 second walkthrough

I built a portfolio implementation of an enterprise banking data platform using
fully synthetic customer, account, UPI, NEFT, RTGS, card, AML, and settlement
data. I deliberately modeled each payment source with a different schema so the
project required real source-to-canonical mapping rather than loading one clean
CSV repeatedly.

The runnable local pipeline preserves immutable Bronze copies, validates and
standardizes payments into a Silver contract, removes duplicate source events,
maps accounts to customers, and quarantines malformed rows. It then performs
transaction-level settlement reconciliation and publishes Gold outputs for
rule-based AML review, Customer 360, and daily regulatory-style totals. I added
quality thresholds so the workflow can fail when reject, duplicate, or missing-
settlement rates exceed expectations.

For an enterprise deployment path, the repository also includes Kafka producer
and consumer examples, PySpark Bronze/Silver/Gold jobs, an Airflow DAG, dbt
models, SQL controls, Docker services, Terraform, and CI pipelines. The local
path is intentionally zero-dependency so reviewers can run it quickly, while
the other components demonstrate how I would scale and operationalize the same
design. I keep this portfolio work separate from employer systems and never use
real bank or customer data.

## Strong follow-up answers

**Why synthetic data?**  Real bank transactions and internal schemas are
confidential. Synthetic sources let me demonstrate engineering patterns,
failure modes, and controls without exposing customer information.

**Why not send every source through Kafka?**  Source cadence should determine
the ingestion pattern. UPI and card authorizations fit event processing, while
core master data and some settlement feeds naturally fit batch extracts.

**How is the pipeline idempotent?**  Silver deduplication uses the source system
and payment ID. A production design would combine that with replay-safe writes,
checkpointing, and atomic/versioned publication.

**How do you handle bad data?**  Bronze keeps the original payload. Silver
validates required IDs, positive amounts, and timestamps, then sends invalid
records to a reject dataset with source file, row number, and reason.

**What does reconciliation prove?**  It compares successful canonical payments
with an independently generated settlement feed at transaction level, exposing
missing settlements and amount mismatches before downstream publication.

**Is the AML output a fraud model?**  No. It is a transparent rule-based review
queue using synthetic labels and amount rules. It demonstrates feature and
alert pipelines, not ownership of an ML detection model.

**What would you change for production?**  I would use governed object storage,
schema registry/contracts, secure Kafka, Spark/Databricks, Snowflake or a SQL
warehouse, centralized observability, RBAC, encryption, lineage, retention,
and environment-specific CI/CD approvals.

## Demo metrics versus resume metrics

Use `quality_report.json` to discuss what this implementation actually produced:
input, canonical, rejected, duplicate, and settlement-exception counts. Do not
combine those demo numbers with employment metrics. Resume outcomes remain
employment claims; repository outputs are reproducible portfolio results.
