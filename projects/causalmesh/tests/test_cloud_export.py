"""Portable cloud bridge tests; no JVM or cloud account required."""
import json
from pathlib import Path

import pytest

from causalmesh.cloud.contracts import StoragePaths
from causalmesh.cloud.export import export_accepted_events
from causalmesh.cloud.job import JobConfig
from causalmesh.engine import Engine
from causalmesh.storage import Store


def event(event_id: str = "evt-A") -> dict:
    return {
        "event_id": event_id, "leg_id": "leg-A", "transfer_id": "TR-A",
        "revision": 1, "account_id": "ACC-1", "counterparty_id": "ACC-2",
        "side": "DEBIT", "currency": "USD", "amount_minor": 12500,
        "effective_at": "2026-11-13T10:00:00+00:00", "schema_version": 1,
    }


def test_export_audit_verified_and_canonical(tmp_path: Path):
    database = str(tmp_path / "ledger.db")
    with Store(database) as store:
        assert Engine(store).ingest(event(), recorded_at="2026-11-13T10:01:00+00:00")["status"] == "accepted"
    out = tmp_path / "landing" / "part-000.jsonl"
    report = export_accepted_events(database, str(out))
    assert report["records"] == 1
    assert report["audit_verified"] is True
    doc = json.loads(out.read_text())
    assert doc["seq"] == 1 and doc["amount_minor"] == 12500
    assert doc["event_hash"] and doc["recorded_at"].startswith("2026")


def test_export_refuses_audit_tampering(tmp_path: Path):
    database = str(tmp_path / "ledger.db")
    with Store(database) as store:
        Engine(store).ingest(event(), recorded_at="2026-11-13T10:01:00+00:00")
        store.conn.execute("UPDATE raw_events SET event_hash=?", ("0" * 64,))
    out = tmp_path / "output.jsonl"
    with pytest.raises(RuntimeError, match="audit-chain"):
        export_accepted_events(database, str(out))
    assert not out.exists()


def test_stream_requires_delta_and_checkpoint():
    with pytest.raises(ValueError, match="Delta"):
        JobConfig("raw", "gold", mode="stream", storage_format="parquet")
    with pytest.raises(ValueError, match="checkpoint"):
        JobConfig("raw", "gold", mode="stream", storage_format="delta")
    assert JobConfig("raw", "gold", storage_format="parquet").mode == "batch"


def test_paths_are_allowlisted():
    p = StoragePaths("s3://safe-bucket/data/")
    assert p.dataset("bronze") == "s3://safe-bucket/data/bronze"
    with pytest.raises(ValueError):
        p.dataset("../../secrets")
