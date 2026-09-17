-- SQLite-compatible analytical schema used by the local demo.
-- The pipeline creates these tables automatically; this file documents types.
CREATE TABLE payments (
  payment_id TEXT NOT NULL,
  source_system TEXT NOT NULL,
  source_account TEXT NOT NULL,
  destination_account TEXT NOT NULL,
  customer_id TEXT,
  payment_type TEXT NOT NULL,
  amount NUMERIC NOT NULL CHECK (amount > 0),
  currency TEXT NOT NULL,
  event_timestamp TEXT NOT NULL,
  status TEXT NOT NULL,
  merchant_category TEXT,
  device_id TEXT,
  ingestion_timestamp TEXT NOT NULL,
  source_file TEXT NOT NULL,
  UNIQUE (source_system, payment_id)
);

CREATE TABLE settlement_reconciliation (
  payment_id TEXT NOT NULL,
  payment_type TEXT NOT NULL,
  event_date TEXT NOT NULL,
  payment_amount NUMERIC NOT NULL,
  settled_amount NUMERIC,
  reconciliation_status TEXT NOT NULL
);
