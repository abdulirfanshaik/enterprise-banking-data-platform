# Salesforce → Snowflake Revenue & Customer 360 Pipeline

A portfolio-grade, end-to-end Data Engineering project that demonstrates **SQL, Python, Snowflake, Salesforce integration, ETL/ELT pipelines, data quality, orchestration, monitoring, and pipeline ownership**.

> **Claim boundary:** This project uses synthetic Salesforce-style records and optional developer-org connectivity. It contains no employer, customer, or confidential production data.

## Business problem

Sales, finance, and operations teams often need a trusted view of Salesforce Accounts, Contacts, and Opportunities in Snowflake for revenue reporting, customer analytics, forecasting, and downstream data products. The difficult part is not simply copying rows: the pipeline must handle incremental extraction, API pagination and limits, duplicates, schema drift, retries, secure credentials, data quality, reconciliation, and safe reprocessing.

## Architecture

```text
Salesforce REST/Bulk API or synthetic JSON
              |
              v
     Python extraction layer
 pagination | retries | watermark | audit manifest
              |
              v
      Snowflake RAW_SALESFORCE
              |
              v
      SQL staging / deduplication
         QUALIFY + ROW_NUMBER
              |
              v
     Curated Revenue / Customer 360
              |
              v
 Quality controls + reconciliation + BI/analytics

        Apache Airflow orchestrates the flow
```

## What this module demonstrates

- Salesforce Account, Contact, and Opportunity extraction
- Mock mode for a fully runnable portfolio demo
- Optional live Salesforce mode using `simple-salesforce`
- Incremental extraction using `SystemModstamp`
- Pagination, retry/backoff, audit metadata, and checkpoint concepts
- Python normalization and deterministic business keys
- Snowflake RAW → STAGING → CURATED ELT design
- SQL deduplication with `ROW_NUMBER()` and `QUALIFY`
- Incremental `MERGE` patterns
- Customer 360 and revenue pipeline modeling
- Source-to-target reconciliation and quality checks
- Airflow orchestration, retries, dependency management, and quality gating
- End-to-end ownership: requirements → design → build → test → monitor → recover
- Unit tests and an interview walkthrough

## Repository structure

```text
salesforce-snowflake-revenue-pipeline/
├── README.md
├── .env.example
├── requirements.txt
├── src/
│   ├── config.py
│   ├── salesforce_client.py
│   ├── pipeline.py
│   └── snowflake_loader.py
├── sql/
│   ├── 00_setup.sql
│   ├── 10_staging.sql
│   ├── 20_customer360.sql
│   └── 30_quality_checks.sql
├── airflow/dags/
│   └── salesforce_snowflake_pipeline.py
├── data/mock/
│   ├── accounts.json
│   ├── contacts.json
│   └── opportunities.json
├── tests/
│   └── test_pipeline.py
└── docs/
    ├── ARCHITECTURE.md
    ├── INTERVIEW_GUIDE.md
    └── RUNBOOK.md
```

## Local demo

Requirements: Python 3.10+

```bash
cd projects/salesforce-snowflake-revenue-pipeline
pip install -r requirements.txt
python -m src.pipeline --mode mock --output data/out
python -m unittest discover -s tests -v
```

The mock path does **not** require Salesforce or Snowflake credentials. It produces normalized JSONL files plus a control manifest that can be inspected locally.

## Live Salesforce mode

Copy `.env.example` to `.env` and configure a Salesforce developer/sandbox account.

```bash
python -m src.pipeline --mode salesforce --output data/out
```

The client uses `SystemModstamp` as the incremental watermark and is designed so the extraction logic can be scheduled safely through Airflow.

## Snowflake deployment

Run the SQL in order:

1. `sql/00_setup.sql`
2. Load normalized extraction files into the RAW tables using `src/snowflake_loader.py`
3. `sql/10_staging.sql`
4. `sql/20_customer360.sql`
5. `sql/30_quality_checks.sql`

## Operational KPIs

A production implementation should track:

- extraction duration and API latency
- records extracted by object
- source-to-target count variance
- duplicate rate
- rejected-record rate
- pipeline success/failure rate
- retry rate
- data freshness
- Snowflake query duration and scanned bytes
- SLA compliance
- mean time to recovery

## Interview positioning

A truthful 60–90 second walkthrough is in [docs/INTERVIEW_GUIDE.md](docs/INTERVIEW_GUIDE.md).

The strongest senior-level message is: **a successful API call or DAG run is not enough; the pipeline must prove completeness, correctness, recoverability, security, and operational ownership.**
