# Interview Guide — Salesforce to Snowflake Revenue Pipeline

## 60–90 second project explanation

“One project I can walk through is a Salesforce-to-Snowflake revenue and Customer 360 pipeline that I built as a sanitized portfolio implementation. The business problem is common in enterprise environments: CRM data such as Accounts, Contacts, and Opportunities needs to be available in Snowflake for analytics, forecasting, and downstream reporting, but simply copying the records is not enough.

I designed the ingestion layer in Python with support for Salesforce authentication, incremental extraction using SystemModstamp, pagination, retries, and audit metadata. The raw Salesforce-aligned data is loaded into Snowflake, and I use SQL for the ELT portion: deterministic deduplication with ROW_NUMBER and QUALIFY, incremental MERGE logic for the Account dimension, an Opportunity fact model, and a Customer 360 view.

I also added source-to-target reconciliation, referential-integrity checks, monetary-value validation, Airflow orchestration, retries, and an operational runbook. The design is replay-safe because the raw layer is preserved and canonical models are deterministic.

The project uses synthetic data by default. It demonstrates the end-to-end ownership pattern I would use in production: understand the business requirement, build the integration, validate completeness, secure credentials, monitor SLAs, and define recovery procedures.”

## Likely follow-up: Why SystemModstamp?

Salesforce `SystemModstamp` is useful for incremental extraction because it tracks system-level changes and is generally more reliable for replication-style use cases than relying only on user-facing modification fields. In production I would still validate the exact object semantics and use a controlled overlap window to protect against boundary conditions.

## How do you handle API limits?

Use incremental loads, bulk endpoints where appropriate, pagination, bounded retries with exponential backoff, and checkpoints. Monitor throttling rate and distinguish retryable failures from permanent authentication or validation errors.

## How do you prevent duplicates?

Preserve every source-aligned delivery in RAW, then use Salesforce Id as the natural source key and select the latest `SystemModstamp` deterministically. Reruns therefore converge to the same canonical result.

## What if Salesforce has 1.2M rows but Snowflake has 1.15M?

First confirm the same population and time scope. Then compare extraction counts, pagination, watermarks, rejected rows, load counts, deduplication, deleted-record behavior, and transformation filters. Fix the cause, replay the affected interval, and add a source-to-target control.

## How would you productionize further?

- Salesforce Bulk API 2.0 for larger extracts
- OAuth/JWT service principal instead of username/password
- secret manager integration
- schema registry or explicit data contracts
- Snowflake Streams/Tasks where appropriate
- dbt tests/documentation
- centralized observability and alerting
- CDC for use cases requiring lower latency
- cost/resource monitors
- environment-specific CI/CD

## Claim boundary

Do not say this repository contains employer Salesforce data or production GX2 logic. It is a portfolio implementation using synthetic data and production-oriented patterns.
