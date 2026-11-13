"""Strict versioned event contracts; integer minor units prevent floating-point ledger errors."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any

ALLOWED_CURRENCIES = {"USD", "EUR", "GBP"}


def parse_time(value: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError("timestamp must be a string")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"invalid ISO timestamp: {value}") from exc
    if result.tzinfo is None:
        raise ValueError("timestamp requires a UTC offset")
    return result.astimezone(timezone.utc)


def utc_string(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def digest(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class LedgerEvent:
    event_id: str
    leg_id: str
    transfer_id: str
    revision: int
    account_id: str
    counterparty_id: str
    side: str
    currency: str
    amount_minor: int
    effective_at: str
    schema_version: int = 1

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> LedgerEvent:
        if not isinstance(value, dict):
            raise ValueError("payload must be a JSON object")
        expected = set(cls.__dataclass_fields__)
        unknown = set(value) - expected
        if unknown:
            raise ValueError(f"unknown fields: {sorted(unknown)}")
        try:
            event = cls(**value)
        except TypeError as exc:
            raise ValueError(f"missing or malformed fields: {exc}") from exc
        for attr in ("event_id", "leg_id", "transfer_id", "account_id", "counterparty_id"):
            field = getattr(event, attr)
            if not isinstance(field, str) or not field.strip() or len(field) > 128:
                raise ValueError(f"{attr} must be a nonempty string of at most 128 chars")
        if type(event.revision) is not int or event.revision < 1:
            raise ValueError("revision must be a positive integer")
        if type(event.schema_version) is not int or event.schema_version != 1:
            raise ValueError("unsupported schema_version")
        if event.side not in {"DEBIT", "CREDIT"}:
            raise ValueError("side must be DEBIT or CREDIT")
        if event.currency not in ALLOWED_CURRENCIES:
            raise ValueError("unsupported currency")
        if type(event.amount_minor) is not int or not (0 < event.amount_minor <= 10**12):
            raise ValueError("amount_minor must be a positive integer no larger than 1e12")
        if event.account_id == event.counterparty_id:
            raise ValueError("account cannot equal counterparty")
        normalized = utc_string(parse_time(event.effective_at))
        return cls(**{**asdict(event), "effective_at": normalized})

    def payload(self) -> dict[str, Any]:
        return asdict(self)
