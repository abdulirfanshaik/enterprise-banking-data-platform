"""SQLite transactional local engine; WAL is intentionally single-host, not a distributed DB."""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

DDL = """
CREATE TABLE IF NOT EXISTS raw_events (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  recorded_at TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  prior_hash TEXT NOT NULL,
  event_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS ledger_versions (
  leg_id TEXT NOT NULL,
  revision INTEGER NOT NULL,
  event_id TEXT NOT NULL,
  transfer_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  counterparty_id TEXT NOT NULL,
  side TEXT NOT NULL CHECK(side IN ('DEBIT','CREDIT')),
  currency TEXT NOT NULL,
  amount_minor INTEGER NOT NULL CHECK(amount_minor>0),
  effective_at TEXT NOT NULL,
  sys_from TEXT NOT NULL,
  sys_to TEXT,
  PRIMARY KEY(leg_id,revision)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_current_leg ON ledger_versions(leg_id) WHERE sys_to IS NULL;
CREATE INDEX IF NOT EXISTS idx_transfer ON ledger_versions(transfer_id, sys_to);
CREATE INDEX IF NOT EXISTS idx_temporal ON ledger_versions(account_id, effective_at, sys_from, sys_to);
CREATE TABLE IF NOT EXISTS quarantine (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  reason TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  quarantined_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS alerts (
  alert_id TEXT PRIMARY KEY,
  transfer_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  rule TEXT NOT NULL,
  explanation TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_transfer ON alerts(transfer_id);
CREATE TABLE IF NOT EXISTS consumer_offsets (
  topic TEXT NOT NULL,
  partition_no INTEGER NOT NULL,
  last_offset INTEGER NOT NULL,
  PRIMARY KEY(topic, partition_no)
);
"""


class Store:
    def __init__(self, path: str = "data/causalmesh.db") -> None:
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(path, isolation_level=None, timeout=15)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=15000")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(DDL)

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except BaseException:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> Store:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()
