"""Read-only observability API plus non-persistent counterfactual analysis."""
from __future__ import annotations

import os
from contextlib import contextmanager
from importlib.resources import files
from typing import Any, Iterator

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, PlainTextResponse

from causalmesh.engine import Engine
from causalmesh.storage import Store


def create_app(path: str | None = None) -> FastAPI:
    db_path = path or os.environ.get("CAUSALMESH_DB", "data/causalmesh.db")
    app = FastAPI(
        title="CausalMesh Control Plane", version="0.1.0",
        description="Temporal bank-ledger explanations, verifiable replay evidence and safe simulations.",
    )

    @contextmanager
    def open_engine() -> Iterator[Engine]:
        with Store(db_path) as store:
            yield Engine(store)

    @app.get("/healthz")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "causalmesh"}

    @app.get("/", response_class=HTMLResponse, include_in_schema=False)
    @app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
    def dashboard() -> str:
        return files("causalmesh").joinpath("static/dashboard.html").read_text(encoding="utf-8")

    @app.get("/api/overview")
    def overview() -> dict[str, Any]:
        with open_engine() as eng:
            return eng.metrics()

    @app.get("/api/transfers")
    def transfers(limit: int = Query(25, ge=1, le=500)) -> list[dict[str, Any]]:
        with open_engine() as eng:
            rows = eng.db.conn.execute(
                "SELECT DISTINCT transfer_id FROM ledger_versions WHERE sys_to IS NULL "
                "ORDER BY transfer_id DESC LIMIT ?", (limit,),
            ).fetchall()
            return [eng.reconciliation(r[0]) for r in rows]

    @app.get("/api/transfers/{transfer_id}")
    def transfer(transfer_id: str) -> dict[str, Any]:
        with open_engine() as eng:
            result = eng.reconciliation(transfer_id)
            if result["leg_count"] == 0:
                raise HTTPException(404, "transfer not found")
            return result

    @app.get("/api/alerts")
    def alerts(limit: int = Query(100, ge=1, le=1000)) -> list[dict[str, Any]]:
        with open_engine() as eng:
            return [dict(x) for x in eng.db.conn.execute(
                "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?", (limit,)
            )]

    @app.get("/api/as-of/{account_id}")
    def as_of(account_id: str, business_at: str, knowledge_at: str) -> dict[str, Any]:
        with open_engine() as eng:
            try:
                return eng.balance_as_of(account_id, business_at, knowledge_at)
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc

    @app.get("/api/lineage/{transfer_id}")
    def lineage(transfer_id: str) -> dict[str, Any]:
        with open_engine() as eng:
            return eng.lineage(transfer_id)

    @app.get("/api/audit")
    def audit() -> dict[str, Any]:
        with open_engine() as eng:
            return eng.verify_audit()

    @app.post("/api/simulate")
    def simulate(payload: dict[str, Any]) -> dict[str, Any]:
        with open_engine() as eng:
            try:
                return eng.simulate(payload)
            except ValueError as exc:
                raise HTTPException(422, str(exc)) from exc

    @app.get("/metrics", response_class=PlainTextResponse)
    def prometheus() -> str:
        with open_engine() as eng:
            m = eng.metrics()
        out = []
        for metric in ("events", "quarantined", "versions", "active_legs", "alerts"):
            out.extend([f"# TYPE causalmesh_{metric} gauge", f"causalmesh_{metric} {m[metric]}"])
        for status, count in m["transfers"].items():
            out.append(f'causalmesh_transfers{{status="{status.lower()}"}} {count}')
        out.append(f"causalmesh_audit_valid {int(m['audit_valid'])}")
        return "\n".join(out) + "\n"

    return app


app = create_app()
