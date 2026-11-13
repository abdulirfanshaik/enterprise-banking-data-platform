"""Idempotent ingestion, bitemporal projections, hash proofs, selective explanations and replay.

The raw log and its derived state commit atomically. This is an exactly-once *database effect*
for a unique event_id, not an end-to-end exactly-once claim across arbitrary Kafka producers.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from causalmesh.model import LedgerEvent, canonical_json, digest, parse_time, utc_string
from causalmesh.storage import Store

GENESIS = "0" * 64
HIGH_VALUE_MINOR = 5_000_000  # USD 50,000 / equivalent raw currency minor units
LATE_AFTER = timedelta(minutes=15)


class Rejected(ValueError):
    pass


class Engine:
    def __init__(self, store: Store) -> None:
        self.db = store

    def _quarantine(self, reason: str, payload: Any) -> dict[str, str]:
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO quarantine(reason,payload_json,quarantined_at) VALUES(?,?,?)",
                (reason, canonical_json(payload), utc_string(datetime.now(timezone.utc))),
            )
        return {"status": "quarantined", "reason": reason}

    def ingest(self, payload: dict[str, Any], recorded_at: str | None = None) -> dict[str, str]:
        try:
            event = LedgerEvent.from_dict(payload)
            supplied = parse_time(recorded_at) if recorded_at else datetime.now(timezone.utc)
        except (ValueError, TypeError) as exc:
            return self._quarantine(str(exc), payload)
        canonical = canonical_json(event.payload())
        try:
            with self.db.transaction() as conn:
                existing = conn.execute(
                    "SELECT payload_json FROM raw_events WHERE event_id=?", (event.event_id,)
                ).fetchone()
                if existing:
                    if existing["payload_json"] == canonical:
                        return {"status": "duplicate", "event_id": event.event_id}
                    raise Rejected("event_id collision with different payload")
                prior = conn.execute(
                    "SELECT event_hash, recorded_at FROM raw_events ORDER BY seq DESC LIMIT 1"
                ).fetchone()
                if prior:
                    prior_time = parse_time(prior["recorded_at"])
                    if supplied <= prior_time:
                        supplied = prior_time + timedelta(microseconds=1)
                system_time = utc_string(supplied)
                self._validate_revision(conn, event)
                prev_hash = prior["event_hash"] if prior else GENESIS
                new_hash = digest(prev_hash + "|" + system_time + "|" + canonical)
                conn.execute(
                    "INSERT INTO raw_events(event_id,recorded_at,payload_json,prior_hash,event_hash) "
                    "VALUES(?,?,?,?,?)",
                    (event.event_id, system_time, canonical, prev_hash, new_hash),
                )
                self._project(conn, event, system_time)
            return {"status": "accepted", "event_id": event.event_id, "audit_hash": new_hash}
        except (Rejected, ValueError) as exc:
            return self._quarantine(str(exc), payload)

    def _validate_revision(self, conn: Any, event: LedgerEvent) -> None:
        old = conn.execute(
            "SELECT * FROM ledger_versions WHERE leg_id=? AND sys_to IS NULL", (event.leg_id,)
        ).fetchone()
        if old is None and event.revision != 1:
            raise Rejected("new leg must start at revision 1")
        if old is not None:
            if event.revision != old["revision"] + 1:
                raise Rejected("revision must increment exactly by one")
            for field in ("transfer_id", "account_id", "counterparty_id", "side", "currency"):
                if getattr(event, field) != old[field]:
                    raise Rejected(f"immutable identity field changed: {field}")
        # A new leg cannot extend a transfer beyond a debit/credit pair.
        if old is None:
            rows = conn.execute(
                "SELECT leg_id FROM ledger_versions WHERE transfer_id=? AND sys_to IS NULL",
                (event.transfer_id,),
            ).fetchall()
            if len(rows) >= 2:
                raise Rejected("a transfer may contain at most two legs")

    def _project(self, conn: Any, event: LedgerEvent, recorded_at: str) -> None:
        if event.revision > 1:
            conn.execute(
                "UPDATE ledger_versions SET sys_to=? WHERE leg_id=? AND sys_to IS NULL",
                (recorded_at, event.leg_id),
            )
        conn.execute(
            """INSERT INTO ledger_versions(
                  leg_id,revision,event_id,transfer_id,account_id,counterparty_id,
                  side,currency,amount_minor,effective_at,sys_from,sys_to)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,NULL)""",
            (
                event.leg_id, event.revision, event.event_id, event.transfer_id,
                event.account_id, event.counterparty_id, event.side, event.currency,
                event.amount_minor, event.effective_at, recorded_at,
            ),
        )
        self._refresh_alerts(conn, event.transfer_id, recorded_at)

    @staticmethod
    def _reconciliation(conn: Any, transfer_id: str) -> dict[str, Any]:
        rows = [dict(x) for x in conn.execute(
            "SELECT * FROM ledger_versions WHERE transfer_id=? AND sys_to IS NULL ORDER BY side",
            (transfer_id,),
        ).fetchall()]
        status = "PENDING"
        reason = "Awaiting both settlement legs"
        if len(rows) == 2:
            debit = next((x for x in rows if x["side"] == "DEBIT"), None)
            credit = next((x for x in rows if x["side"] == "CREDIT"), None)
            if not debit or not credit:
                status, reason = "MISMATCH", "Both leg types must be present"
            elif (
                debit["amount_minor"] == credit["amount_minor"]
                and debit["currency"] == credit["currency"]
                and debit["account_id"] == credit["counterparty_id"]
                and credit["account_id"] == debit["counterparty_id"]
            ):
                status, reason = "SETTLED", "Balanced, mirrored debit and credit legs"
            else:
                status, reason = "MISMATCH", "Amount, currency or counterparty does not reconcile"
        return {
            "transfer_id": transfer_id, "status": status, "reason": reason,
            "leg_count": len(rows), "legs": rows,
        }

    def reconciliation(self, transfer_id: str) -> dict[str, Any]:
        return self._reconciliation(self.db.conn, transfer_id)

    def _refresh_alerts(self, conn: Any, transfer_id: str, at: str) -> None:
        conn.execute("DELETE FROM alerts WHERE transfer_id=?", (transfer_id,))
        summary = self._reconciliation(conn, transfer_id)
        candidates: list[tuple[str, str, str, list[str]]] = []
        evidence = [x["event_id"] for x in summary["legs"]]
        if summary["status"] == "MISMATCH":
            candidates.append(("UNBALANCED_SETTLEMENT", "CRITICAL", summary["reason"], evidence))
        for leg in summary["legs"]:
            if leg["amount_minor"] >= HIGH_VALUE_MINOR:
                candidates.append((
                    "HIGH_VALUE", "HIGH", f"{leg['side']} >= {HIGH_VALUE_MINOR} minor units",
                    [leg["event_id"]],
                ))
            if parse_time(leg["sys_from"]) - parse_time(leg["effective_at"]) > LATE_AFTER:
                candidates.append((
                    "LATE_ARRIVAL", "MEDIUM", "Business event arrived >15 minutes late",
                    [leg["event_id"]],
                ))
        for rule, severity, explanation, ids in candidates:
            alert_id = digest(canonical_json([transfer_id, rule, ids]))[:24]
            conn.execute(
                """INSERT OR IGNORE INTO alerts(
                      alert_id,transfer_id,severity,rule,explanation,evidence_json,created_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (alert_id, transfer_id, severity, rule, explanation, canonical_json(ids), at),
            )

    def balance_as_of(
        self, account_id: str, business_at: str, knowledge_at: str
    ) -> dict[str, Any]:
        valid = utc_string(parse_time(business_at))
        known = utc_string(parse_time(knowledge_at))
        rows = self.db.conn.execute(
            """SELECT currency, side, amount_minor, leg_id, revision
               FROM ledger_versions WHERE account_id=? AND effective_at<=?
               AND sys_from<=? AND (sys_to IS NULL OR sys_to>?)""",
            (account_id, valid, known, known),
        ).fetchall()
        totals: dict[str, int] = {}
        for r in rows:
            totals[r["currency"]] = totals.get(r["currency"], 0) + (
                r["amount_minor"] if r["side"] == "CREDIT" else -r["amount_minor"]
            )
        return {
            "account_id": account_id, "business_at": valid, "knowledge_at": known,
            "balances_minor": totals, "leg_count": len(rows),
            "evidence": [{"leg_id": r["leg_id"], "revision": r["revision"]} for r in rows],
        }

    def lineage(self, transfer_id: str) -> dict[str, Any]:
        rows = [dict(x) for x in self.db.conn.execute(
            """SELECT v.event_id, v.leg_id, v.revision, v.sys_from, r.seq, r.event_hash
               FROM ledger_versions v JOIN raw_events r USING(event_id)
               WHERE v.transfer_id=? ORDER BY r.seq""",
            (transfer_id,),
        )]
        return {
            "transfer_id": transfer_id, "sources": rows,
            "derivations": ["raw_events", "ledger_versions", "reconciliation", "alerts", "dashboard"],
            "note": "Recorded fine-grained inputs plus a declared transformation path; not runtime tracing.",
        }

    def verify_audit(self) -> dict[str, Any]:
        previous = GENESIS
        examined = 0
        for row in self.db.conn.execute("SELECT * FROM raw_events ORDER BY seq"):
            calculated = digest(previous + "|" + row["recorded_at"] + "|" + row["payload_json"])
            if previous != row["prior_hash"] or calculated != row["event_hash"]:
                return {"valid": False, "checked": examined, "failed_seq": row["seq"]}
            previous = calculated
            examined += 1
        return {"valid": True, "checked": examined, "head_hash": previous}

    def replay(self) -> dict[str, Any]:
        if not self.verify_audit()["valid"]:
            raise Rejected("audit verification failed; refusing to replay altered log")
        # One transaction guarantees readers never observe a half-rebuilt projection.
        with self.db.transaction() as conn:
            log = conn.execute("SELECT * FROM raw_events ORDER BY seq").fetchall()
            conn.execute("DELETE FROM alerts")
            conn.execute("DELETE FROM ledger_versions")
            for row in log:
                event = LedgerEvent.from_dict(json.loads(row["payload_json"]))
                self._validate_revision(conn, event)
                self._project(conn, event, row["recorded_at"])
        return {"replayed": len(log), "audit": self.verify_audit()}

    def simulate(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Counterfactual state under a DB savepoint: no raw writes and zero durable side effects."""
        event = LedgerEvent.from_dict(payload)
        with self.db.transaction() as conn:
            before = self._reconciliation(conn, event.transfer_id)
            prior = conn.execute("SELECT recorded_at FROM raw_events ORDER BY seq DESC LIMIT 1").fetchone()
            now = datetime.now(timezone.utc)
            if prior and now <= parse_time(prior["recorded_at"]):
                now = parse_time(prior["recorded_at"]) + timedelta(microseconds=1)
            conn.execute("SAVEPOINT counterfactual")
            try:
                self._validate_revision(conn, event)
                self._project(conn, event, utc_string(now))
                after = self._reconciliation(conn, event.transfer_id)
                alerts = [dict(x) for x in conn.execute(
                    "SELECT rule,severity,explanation FROM alerts WHERE transfer_id=?",
                    (event.transfer_id,),
                )]
            finally:
                conn.execute("ROLLBACK TO SAVEPOINT counterfactual")
                conn.execute("RELEASE SAVEPOINT counterfactual")
        return {
            "before": {"status": before["status"], "reason": before["reason"]},
            "after": {"status": after["status"], "reason": after["reason"]},
            "would_trigger": alerts, "committed": False,
        }

    def metrics(self) -> dict[str, Any]:
        conn = self.db.conn
        def scalar(query: str) -> int:
            return int(conn.execute(query).fetchone()[0])
        transfers = [x[0] for x in conn.execute(
            "SELECT DISTINCT transfer_id FROM ledger_versions WHERE sys_to IS NULL"
        )]
        by_status = {"SETTLED": 0, "PENDING": 0, "MISMATCH": 0}
        for transfer_id in transfers:
            by_status[self.reconciliation(transfer_id)["status"]] += 1
        return {
            "events": scalar("SELECT count(*) FROM raw_events"),
            "quarantined": scalar("SELECT count(*) FROM quarantine"),
            "versions": scalar("SELECT count(*) FROM ledger_versions"),
            "active_legs": scalar("SELECT count(*) FROM ledger_versions WHERE sys_to IS NULL"),
            "alerts": scalar("SELECT count(*) FROM alerts"),
            "transfers": by_status,
            "audit_valid": self.verify_audit()["valid"],
        }
