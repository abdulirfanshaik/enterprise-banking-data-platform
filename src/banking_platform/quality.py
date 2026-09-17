from __future__ import annotations

from collections import Counter
from decimal import Decimal
from typing import Iterable


def reconcile(payments: list[dict[str, str]], settlements: list[dict[str, str]]) -> tuple[list[dict[str, str]], dict[str, int]]:
    settlement_by_id = {row["payment_id"]: row for row in settlements}
    result = []
    counts = Counter()
    for payment in payments:
        if payment["status"] != "SUCCESS":
            state = "NOT_EXPECTED"
            settled_amount = ""
        else:
            settlement = settlement_by_id.get(payment["payment_id"])
            if settlement is None:
                state = "MISSING_SETTLEMENT"
                settled_amount = ""
            else:
                settled_amount = settlement["settled_amount"]
                state = "MATCHED" if Decimal(payment["amount"]) == Decimal(settled_amount) else "AMOUNT_MISMATCH"
        counts[state] += 1
        result.append({
            "payment_id": payment["payment_id"],
            "payment_type": payment["payment_type"],
            "event_date": payment["event_timestamp"][:10],
            "payment_amount": payment["amount"],
            "settled_amount": settled_amount,
            "reconciliation_status": state,
        })
    return result, dict(counts)


def quality_report(input_rows: int, output_rows: int, rejected_rows: int, duplicate_rows: int,
                   reconciliation_counts: dict[str, int], thresholds: dict[str, float]) -> dict:
    reject_rate = rejected_rows / input_rows if input_rows else 0.0
    duplicate_rate = duplicate_rows / input_rows if input_rows else 0.0
    expected = reconciliation_counts.get("MATCHED", 0) + reconciliation_counts.get("MISSING_SETTLEMENT", 0) + reconciliation_counts.get("AMOUNT_MISMATCH", 0)
    missing_rate = reconciliation_counts.get("MISSING_SETTLEMENT", 0) / expected if expected else 0.0
    checks = {
        "reject_rate_within_threshold": reject_rate <= thresholds["maximum_reject_rate"],
        "duplicate_rate_within_threshold": duplicate_rate <= thresholds["maximum_duplicate_rate"],
        "missing_settlement_rate_within_threshold": missing_rate <= thresholds["maximum_missing_settlement_rate"],
        "canonical_rows_nonzero": output_rows > 0,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "input_rows": input_rows,
        "canonical_rows": output_rows,
        "rejected_rows": rejected_rows,
        "duplicate_rows_removed": duplicate_rows,
        "reject_rate": round(reject_rate, 6),
        "duplicate_rate": round(duplicate_rate, 6),
        "missing_settlement_rate": round(missing_rate, 6),
        "reconciliation": reconciliation_counts,
        "checks": checks,
    }
