"""Optional analytics materialization to columnar Parquet through DuckDB."""
from __future__ import annotations

import json
import tempfile
from pathlib import Path

from causalmesh.engine import Engine


def export_parquet(engine: Engine, folder: str) -> dict:
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("Install optional dependency: pip install -e '.[lake]'") from exc
    target = Path(folder)
    target.mkdir(parents=True, exist_ok=True)
    rows = [dict(r) for r in engine.db.conn.execute(
        "SELECT transfer_id,leg_id,account_id,side,currency,amount_minor,effective_at,sys_from "
        "FROM ledger_versions WHERE sys_to IS NULL"
    )]
    result_path = target / "current_ledger.parquet"
    if not rows:
        return {"rows": 0, "path": None, "reason": "empty projection"}
    # DuckDB's JSON auto-scan provides a reproducible local bronze->gold boundary.
    with tempfile.TemporaryDirectory() as tempdir:
        source = Path(tempdir) / "ledger.jsonl"
        with source.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row) + "\n")
        connection = duckdb.connect()
        try:
            connection.execute(
                "COPY (SELECT * FROM read_json_auto(?) "
                "ORDER BY transfer_id, leg_id) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)",
                [str(source), str(result_path)],
            )
        finally:
            connection.close()
    return {"rows": len(rows), "path": str(result_path)}
