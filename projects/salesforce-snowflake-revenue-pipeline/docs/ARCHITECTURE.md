# Architecture

## Design goal

Build a production-oriented Salesforce-to-Snowflake pattern that is easy to explain in a Senior Data Engineer interview and safe to run with synthetic data.

## Flow

1. **Extract**
   - Salesforce Account, Contact, and Opportunity objects
   - `SystemModstamp` incremental watermark
   - authentication through environment/secrets
   - API pagination handled by the Salesforce client
   - retry with exponential backoff

2. **Normalize**
   - remove Salesforce transport metadata
   - add stable business keys
   - add extraction timestamp and source object
   - deterministic duplicate handling

3. **Load RAW**
   - append source-aligned rows to Snowflake
   - preserve source timestamps and audit metadata

4. **Transform (ELT)**
   - Snowflake SQL performs canonical deduplication
   - Account dimension uses incremental `MERGE`
   - Opportunity fact and Customer 360 are curated for analytics

5. **Quality gate**
   - null business keys
   - referential integrity
   - non-negative monetary values
   - raw-distinct-to-canonical count reconciliation

6. **Operate**
   - Airflow retries and dependencies
   - manifest for counts/watermarks
   - quality results retained in a control schema
   - replay from RAW rather than manually repairing curated tables

## Senior-level tradeoffs

### Why ELT?
Snowflake is well suited to warehouse-side SQL transformations. Preserving source-aligned RAW data improves replay, auditability, and root-cause analysis.

### Why not stream everything?
CRM objects generally do not require millisecond latency. An hourly incremental load is easier to operate unless the business explicitly requires lower latency. Salesforce CDC can be introduced where the use case justifies it.

### Why retain duplicates in RAW?
Duplicate delivery can be evidence of source retries or extraction behavior. Canonical models deduplicate; the raw layer preserves evidence.

### Why use source timestamps plus extraction timestamps?
`SystemModstamp` represents source change time, while `_EXTRACTED_AT` records when the pipeline observed the record. Both are useful for freshness, replay, and incident investigation.
