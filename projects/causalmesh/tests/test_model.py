from dataclasses import asdict

import pytest

from causalmesh.demo import sample_event
from causalmesh.model import LedgerEvent, canonical_json, parse_time


def test_valid_contract_is_canonical():
    event = LedgerEvent.from_dict(sample_event(1, "DEBIT", 300))
    assert event.revision == 1
    assert event.effective_at.endswith("+00:00")
    assert LedgerEvent.from_dict(asdict(event)) == event


@pytest.mark.parametrize("field,value", [
    ("amount_minor", 0), ("amount_minor", 4.5), ("amount_minor", True),
    ("revision", 0), ("revision", 1.5), ("schema_version", 2),
    ("currency", "BTC"), ("side", "REFUND"), ("event_id", ""),
    ("effective_at", "yesterday"), ("effective_at", "2026-01-01T00:00:00"),
])
def test_rejects_bad_contracts(field, value):
    with pytest.raises(ValueError):
        LedgerEvent.from_dict({**sample_event(1, "DEBIT"), field: value})


def test_unknown_fields_forbidden():
    with pytest.raises(ValueError, match="unknown fields"):
        LedgerEvent.from_dict({**sample_event(1, "DEBIT"), "secret": "x"})


def test_canonical_json_order_independent():
    assert canonical_json({"b": 2, "a": 1}) == canonical_json({"a": 1, "b": 2})


def test_timestamp_requires_timezone():
    with pytest.raises(ValueError, match="UTC offset"):
        parse_time("2026-11-13T10:00:00")
