"""Deterministic synthetic scenarios with deliberate late, duplicate and corrupted data."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

from causalmesh.engine import Engine
from causalmesh.model import utc_string


def sample_event(
    transfer: int, side: str, amount_minor: int = 120_000,
    revision: int = 1, effective_at: str | None = None,
) -> dict:
    return {
        "event_id": f"evt-{transfer:06d}-{side.lower()}-r{revision}",
        "leg_id": f"leg-{transfer:06d}-{side.lower()}",
        "transfer_id": f"TRX-{transfer:06d}",
        "revision": revision,
        "account_id": f"ACC-{100 + transfer % 41:04d}" if side == "DEBIT" else f"ACC-{200 + transfer % 47:04d}",
        "counterparty_id": f"ACC-{200 + transfer % 47:04d}" if side == "DEBIT" else f"ACC-{100 + transfer % 41:04d}",
        "side": side,
        "currency": "USD",
        "amount_minor": amount_minor,
        "effective_at": effective_at or utc_string(datetime.now(timezone.utc)),
        "schema_version": 1,
    }


def scenarios(transfers: int = 250, seed: int = 42) -> Iterator[dict]:
    randomizer = random.Random(seed)
    now = datetime.now(timezone.utc)
    for n in range(1, transfers + 1):
        # Every transfer is fully synthetic; no customer/financial data is used.
        when = utc_string(now - timedelta(seconds=randomizer.randrange(60, 800)))
        amount = randomizer.randint(100, 6_000_000) if n % 17 else 9_000_000
        debit = sample_event(n, "DEBIT", amount, effective_at=when)
        credit_amount = amount + 13 if n % 11 == 0 or n % 37 == 0 else amount
        credit = sample_event(n, "CREDIT", credit_amount, effective_at=when)
        if n % 2:
            yield credit  # out-of-order leg delivery
        yield debit
        if n % 2 == 0 and n % 13 != 0:
            yield credit
        if n % 19 == 0:
            yield debit  # byte-identical retry, should be idempotent
        if n % 23 == 0:
            yield {**debit, "amount_minor": amount + 999}  # conflicting event_id -> DLQ
        if n % 11 == 0 and n % 13 != 0:
            # Revision repairs the deliberate mismatch without rewriting old knowledge.
            yield sample_event(n, "CREDIT", amount, revision=2, effective_at=when)
        if n % 29 == 0:
            yield sample_event(100000 + n, "DEBIT", amount, effective_at=utc_string(now - timedelta(hours=3)))
    yield {"event_id": "broken", "amount_minor": "NaN"}


def populate(engine: Engine, transfers: int = 250, seed: int = 42) -> dict:
    outcomes: dict[str, int] = {"accepted": 0, "duplicate": 0, "quarantined": 0}
    for event in scenarios(transfers, seed):
        result = engine.ingest(event)
        outcomes[result["status"]] += 1
    return {"ingestion": outcomes, "metrics": engine.metrics()}
