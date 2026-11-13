-- Optional serving-layer integration. Substitute identifiers using your deployment process;
-- do not interpolate user input into SQL or commit Snowflake credentials.
-- Prerequisites: authorized S3 STORAGE INTEGRATION and access to Delta-derived Parquet export.
-- This SQL does NOT read Delta transaction logs; export a stable Parquet Gold snapshot first.
CREATE TABLE IF NOT EXISTS CAUSALMESH_GOLD_TRANSFERS (
    TRANSFER_ID VARCHAR NOT NULL,
    D_COUNT NUMBER(38,0),
    C_COUNT NUMBER(38,0),
    D_AMOUNT NUMBER(38,0),
    C_AMOUNT NUMBER(38,0),
    D_CURRENCY VARCHAR,
    C_CURRENCY VARCHAR,
    D_ACCOUNT VARCHAR,
    C_ACCOUNT VARCHAR,
    D_COUNTERPARTY VARCHAR,
    C_COUNTERPARTY VARCHAR,
    D_EVENT_ID VARCHAR,
    C_EVENT_ID VARCHAR,
    STATUS VARCHAR,
    DIFFERENCE_MINOR NUMBER(38,0),
    LOADED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Example after configuring STORAGE_INTEGRATION and a fixed, authorized S3 prefix:
-- CREATE STAGE CAUSALMESH_GOLD_STAGE
--   URL='s3://YOUR_BUCKET/exports/gold_transfers/'
--   STORAGE_INTEGRATION=YOUR_S3_INTEGRATION
--   FILE_FORMAT=(TYPE=PARQUET);
-- COPY INTO CAUSALMESH_GOLD_TRANSFERS (
--   TRANSFER_ID, D_COUNT, C_COUNT, D_AMOUNT, C_AMOUNT,
--   D_CURRENCY, C_CURRENCY, D_ACCOUNT, C_ACCOUNT,
--   D_COUNTERPARTY, C_COUNTERPARTY, D_EVENT_ID, C_EVENT_ID,
--   STATUS, DIFFERENCE_MINOR
-- ) FROM (
--   SELECT $1:transfer_id::VARCHAR, $1:d_count::NUMBER, $1:c_count::NUMBER,
--   $1:d_amount::NUMBER, $1:c_amount::NUMBER,
--   $1:d_currency::VARCHAR, $1:c_currency::VARCHAR,
--   $1:d_account::VARCHAR, $1:c_account::VARCHAR,
--   $1:d_counterparty::VARCHAR, $1:c_counterparty::VARCHAR,
--   $1:d_event_id::VARCHAR, $1:c_event_id::VARCHAR,
--   $1:status::VARCHAR, $1:difference_minor::NUMBER
--   FROM @CAUSALMESH_GOLD_STAGE
-- );

CREATE OR REPLACE VIEW V_CAUSALMESH_SETTLEMENT_QUALITY AS
SELECT
    STATUS,
    COUNT(*) AS TRANSFER_COUNT,
    SUM(COALESCE(D_AMOUNT, 0)) AS DEBIT_MINOR,
    SUM(COALESCE(C_AMOUNT, 0)) AS CREDIT_MINOR,
    SUM(ABS(COALESCE(DIFFERENCE_MINOR, 0))) AS ABS_AMOUNT_DIFFERENCE_MINOR
FROM CAUSALMESH_GOLD_TRANSFERS
GROUP BY STATUS;
