# Data dictionary

## Silver: payments

| Column | Type | Meaning |
|---|---|---|
| payment_id | string | Source transaction or authorization identifier |
| source_system | string | Explicit simulated source name |
| source_account | string | Synthetic originating account |
| destination_account | string | Synthetic beneficiary or merchant account |
| customer_id | string | Customer mapped from the core account master |
| payment_type | string | UPI, NEFT, RTGS, or CARD |
| amount | decimal(18,2) | Positive transaction amount |
| currency | string | INR in the default generator |
| event_timestamp | timestamp | Source event time in UTC |
| status | string | SUCCESS, FAILED, or PENDING |
| merchant_category | string | Present when supplied by source |
| device_id | string | Present for digital/card sources |
| ingestion_timestamp | timestamp | Pipeline standardization time |
| source_file | string | Input file used for traceability |

## Gold: aml_fraud_alerts

Rule-based **review candidates**, not fraud verdicts. `risk_score` is a transparent
demo score and must not be described as a trained production model.

| Column | Meaning |
|---|---|
| alert_id | Deterministic alert key |
| payment_id | Related payment |
| customer_id | Related customer |
| rule_name | One or more triggered demo rules |
| risk_score | Capped additive demo score |
| disposition | REVIEW until investigated |

## Gold: customer_360

One record per synthetic customer containing KYC/risk attributes, account count,
combined balance, outbound payment activity, alert count, and last activity.

## Gold: regulatory_daily

Daily channel-level transaction counts, values, and status counts. It resembles
a regulatory control mart but is not an actual RBI filing schema.

## Gold: settlement_reconciliation

| Status | Meaning |
|---|---|
| MATCHED | Successful payment has equal settlement amount |
| MISSING_SETTLEMENT | Successful payment lacks a settlement record |
| AMOUNT_MISMATCH | Payment and settlement amounts differ |
| NOT_EXPECTED | Failed/pending payment is not expected to settle |
