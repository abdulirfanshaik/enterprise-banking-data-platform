-- Snowflake optional *idempotent* refresh pattern (requires a configured external stage
-- from settlement_analytics.sql and an immutable/snapshot-consistent export).
-- Run this in the same session immediately after loading the staging data.
CREATE OR REPLACE TEMPORARY TABLE CAUSALMESH_GOLD_INCOMING LIKE CAUSALMESH_GOLD_TRANSFERS;

-- In production use COPY INTO CAUSALMESH_GOLD_INCOMING from a stable export snapshot
-- or source an upstream table containing one row per transfer_id. This procedure
-- intentionally does not invent S3 credentials or storage integrations.

MERGE INTO CAUSALMESH_GOLD_TRANSFERS target
USING (
    SELECT * EXCLUDE (RN)
    FROM (
        SELECT i.*,
               ROW_NUMBER() OVER (
                   PARTITION BY TRANSFER_ID
                   ORDER BY LOADED_AT DESC, TRANSFER_ID
               ) AS RN
        FROM CAUSALMESH_GOLD_INCOMING i
    )
    WHERE RN = 1
) source
ON target.TRANSFER_ID = source.TRANSFER_ID
WHEN MATCHED THEN UPDATE SET
    target.D_COUNT = source.D_COUNT,
    target.C_COUNT = source.C_COUNT,
    target.D_AMOUNT = source.D_AMOUNT,
    target.C_AMOUNT = source.C_AMOUNT,
    target.D_CURRENCY = source.D_CURRENCY,
    target.C_CURRENCY = source.C_CURRENCY,
    target.D_ACCOUNT = source.D_ACCOUNT,
    target.C_ACCOUNT = source.C_ACCOUNT,
    target.D_COUNTERPARTY = source.D_COUNTERPARTY,
    target.C_COUNTERPARTY = source.C_COUNTERPARTY,
    target.D_EVENT_ID = source.D_EVENT_ID,
    target.C_EVENT_ID = source.C_EVENT_ID,
    target.STATUS = source.STATUS,
    target.DIFFERENCE_MINOR = source.DIFFERENCE_MINOR,
    target.LOADED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (
    TRANSFER_ID, D_COUNT, C_COUNT, D_AMOUNT, C_AMOUNT,
    D_CURRENCY, C_CURRENCY, D_ACCOUNT, C_ACCOUNT,
    D_COUNTERPARTY, C_COUNTERPARTY, D_EVENT_ID, C_EVENT_ID,
    STATUS, DIFFERENCE_MINOR
) VALUES (
    source.TRANSFER_ID, source.D_COUNT, source.C_COUNT,
    source.D_AMOUNT, source.C_AMOUNT, source.D_CURRENCY, source.C_CURRENCY,
    source.D_ACCOUNT, source.C_ACCOUNT, source.D_COUNTERPARTY,
    source.C_COUNTERPARTY, source.D_EVENT_ID, source.C_EVENT_ID,
    source.STATUS, source.DIFFERENCE_MINOR
);
