# Operations Runbook

## Pipeline SLA
Example portfolio target: hourly extraction completes within 20 minutes and curated data is no more than 90 minutes behind Salesforce.

## First checks when the DAG fails

1. Identify the failed task and blast radius.
2. Confirm whether Salesforce is reachable and credentials are valid.
3. Check API throttling, timeout, and authentication errors.
4. Check the extraction manifest for record counts and max watermarks.
5. Confirm raw Snowflake loads completed.
6. Review Snowflake Query Profile for transformation failures or regressions.
7. Inspect the latest quality-control results.

## If source and Snowflake counts do not match

- confirm the same date/time scope
- check pagination and API limits
- inspect incremental watermark boundaries
- compare extracted count, loaded count, and canonical count
- inspect rejected/null-key records
- verify deduplication rules
- check deleted/archived-record semantics
- replay the affected watermark window after correction

## If a rerun is needed

The design is intended to be replay-safe:
- extraction records have stable Salesforce IDs
- canonical SQL deduplicates by Salesforce ID
- Account dimension uses MERGE
- raw evidence is preserved

## Escalation information

Include:
- DAG run ID
- affected object
- watermark interval
- extracted and loaded counts
- quality check failures
- business impact
- corrective action
- validation results
