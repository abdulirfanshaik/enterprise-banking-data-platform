# Enterprise Banking Data Platform

A runnable portfolio project that simulates an enterprise payments, AML/fraud,
Customer 360, and regulatory-reporting platform. It uses only synthetic data in
the default demo; it does **not** contain or claim access to JPMorgan Chase,
Axis Bank, Finacle, RBI, NPCI, or customer-confidential data.

## Featured project: Salesforce → Snowflake Revenue Pipeline

For Senior Data Engineer interviews requiring **SQL, Python, Snowflake,
Salesforce integration, ETL/ELT, APIs, data quality, Airflow, and end-to-end
pipeline ownership**, see:

[projects/salesforce-snowflake-revenue-pipeline/](projects/salesforce-snowflake-revenue-pipeline/)

That module includes a synthetic Salesforce-style Account/Contact/Opportunity
source, Python incremental extraction, Snowflake RAW/STAGING/CURATED SQL,
deduplication, MERGE logic, Customer 360, reconciliation controls, an Airflow
DAG, tests, an operations runbook, and an interview walkthrough.

## What this project demonstrates

- Independent core-banking, UPI, NEFT, RTGS, and card source schemas
- Deterministic synthetic data with duplicates, malformed rows, failed
  settlements, suspicious patterns, and late/missing settlement records
- Batch ingestion and canonical payment standardization
- Data quality, idempotent deduplication, and source-to-settlement reconciliation
- Gold outputs for AML/fraud alerts, Customer 360, and regulatory reporting
- A zero-dependency local implementation backed by CSV/JSONL and SQLite
- Production-oriented examples for Kafka, PySpark, Airflow, dbt, Docker, and SQL
- Unit tests, data dictionary, architecture notes, and an interview walkthrough

## Fastest way to run

Requirements: Python 3.10+ and GNU Make. The local demo uses the Python standard
library only.

```bash
make demo
make test
```

Or run the commands directly:

```bash
PYTHONPATH=src python -m banking_platform.generator \
  --output data/raw --customers 250 --accounts 400 --transactions 3000

PYTHONPATH=src python -m banking_platform.local_pipeline \
  --input data/raw --output data/processed

PYTHONPATH=src python -m unittest discover -s tests -v
```

The pipeline creates:

```text
data/processed/
├── bronze/
├── silver/
│   ├── payments.csv
│   └── rejected_records.csv
├── gold/
│   ├── aml_fraud_alerts.csv
│   ├── customer_360.csv
│   ├── regulatory_daily.csv
│   └── settlement_reconciliation.csv
├── metrics/quality_report.json
└── banking_analytics.db
```

## Useful SQLite queries

```bash
sqlite3 data/processed/banking_analytics.db \
  "select payment_type, count(*), round(sum(amount),2) from payments group by 1;"

sqlite3 data/processed/banking_analytics.db \
  "select rule_name, count(*) from aml_fraud_alerts group by 1 order by 2 desc;"
```

## Two execution paths

1. **Local proof path:** `generator.py` + `local_pipeline.py`; runs immediately
   without cloud accounts or heavy dependencies and is fully tested.
2. **Enterprise pattern path:** Kafka producer/consumer, PySpark Bronze/Silver/Gold
   jobs, an Airflow DAG, dbt models, SQL controls, and Docker infrastructure.
   These files model how the same logic would be deployed at larger scale.

See [docs/architecture.md](docs/architecture.md) for the system design and
[docs/interview_guide.md](docs/interview_guide.md) for a truthful project story.

## Optional public reference sources

The project is intentionally self-contained. For additional context, manually
download official aggregate statistics and place normalized copies under
`data/reference/`:

- RBI Database on Indian Economy / Payment System Indicators
- NPCI UPI Product Statistics
- IBM AMLSim or IBM AML-Data synthetic datasets

These are external reference or synthetic inputs. Never present aggregate RBI
or NPCI data as customer-level bank transactions. See
[`data/reference/README.md`](data/reference/README.md).

## Scope and claim boundary

This repository is a new portfolio implementation inspired by common banking
data-engineering patterns and the technologies listed on Irfan Shaik's resume.
The metrics produced by this demo are demo metrics. They are not employment
outcomes and must not be presented as confidential production results.

## License

MIT. See [LICENSE](LICENSE).
