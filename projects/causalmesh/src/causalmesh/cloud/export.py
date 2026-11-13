"""Audit-verified transport from the SQLite event store to immutable JSONL landing files.

This is a local development bridge, not a direct SQLite-to-production streaming pipeline.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from causalmesh.engine import Engine
from causalmesh.storage import Store


def export_accepted_events(db_path: str, target: str) -> dict[str, object]:
    dst = Path(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    with Store(db_path) as store:
        audit = Engine(store).verify_audit()
        if not audit.get("valid"):
            raise RuntimeError("audit-chain verification failed; export blocked")
        rows = store.conn.execute(
            "SELECT seq,event_id,recorded_at,event_hash,prior_hash,payload_json "
            "FROM raw_events ORDER BY seq"
        ).fetchall()
        count = 0
        # Atomic rename: failed export cannot leave a partial 'ready' file.
        tmp = dst.with_suffix(dst.suffix + ".part")
        try:
            with tmp.open("w", encoding="utf-8") as out:
                for row in rows:
                    payload = json.loads(row["payload_json"])
                    envelope = {k: row[k] for k in (
                        "seq", "event_id", "recorded_at", "event_hash", "prior_hash"
                    )}
                    envelope.update(payload)
                    out.write(json.dumps(envelope, sort_keys=True, separators=(",", ":")) + "\n")
                    count += 1
            tmp.replace(dst)
        finally:
            tmp.unlink(missing_ok=True)
    return {"path": str(dst), "records": count, "audit_verified": True}


def sqlite_has_events(db_path: str) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            return conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] > 0
    except sqlite3.OperationalError:
        return False
