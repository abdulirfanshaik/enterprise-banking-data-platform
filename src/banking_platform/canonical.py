from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Iterable


CANONICAL_FIELDS = [
    "payment_id", "source_system", "source_account", "destination_account",
    "customer_id", "payment_type", "amount", "currency", "event_timestamp",
    "status", "merchant_category", "device_id", "ingestion_timestamp", "source_file",
]


def _timestamp(value: Any) -> str:
    text = str(value or "").strip()
    parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _amount(value: Any) -> str:
    amount = Decimal(str(value))
    if amount <= 0:
        raise ValueError("amount must be positive")
    return f"{amount.quantize(Decimal('0.01')):.2f}"


def _base(payment_id: Any, source_system: str, source_account: Any, destination_account: Any,
          payment_type: str, amount: Any, event_timestamp: Any, status: Any,
          source_file: str, merchant_category: Any = "", device_id: Any = "") -> dict[str, str]:
    required = {
        "payment_id": str(payment_id or "").strip(),
        "source_account": str(source_account or "").strip(),
        "destination_account": str(destination_account or "").strip(),
    }
    missing = [name for name, value in required.items() if not value]
    if missing:
        raise ValueError("missing required field(s): " + ", ".join(missing))
    return {
        "payment_id": required["payment_id"],
        "source_system": source_system,
        "source_account": required["source_account"],
        "destination_account": required["destination_account"],
        "customer_id": "",
        "payment_type": payment_type,
        "amount": _amount(amount),
        "currency": "INR",
        "event_timestamp": _timestamp(event_timestamp),
        "status": str(status or "UNKNOWN").strip().upper(),
        "merchant_category": str(merchant_category or "").strip().upper(),
        "device_id": str(device_id or "").strip(),
        "ingestion_timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "source_file": source_file,
    }


def standardize_upi(row: dict[str, Any], source_file: str) -> dict[str, str]:
    return _base(row.get("upi_transaction_id"), "SIMULATED_UPI", row.get("payer_account"),
                 row.get("payee_account"), "UPI", row.get("amount_inr"),
                 row.get("transaction_timestamp"), row.get("transaction_status"), source_file,
                 row.get("merchant_category"), row.get("device_id"))


def standardize_neft(row: dict[str, Any], source_file: str) -> dict[str, str]:
    return _base(row.get("utr_number"), "SIMULATED_NEFT", row.get("sender_account"),
                 row.get("beneficiary_account"), "NEFT", row.get("amount"),
                 row.get("transaction_date"), row.get("status"), source_file)


def standardize_rtgs(row: dict[str, Any], source_file: str) -> dict[str, str]:
    return _base(row.get("utr_number"), "SIMULATED_RTGS", row.get("sender_account"),
                 row.get("beneficiary_account"), "RTGS", row.get("amount"),
                 row.get("value_date"), row.get("status"), source_file)


def standardize_card(row: dict[str, Any], source_file: str) -> dict[str, str]:
    status = "SUCCESS" if str(row.get("response_code")) == "00" else "FAILED"
    return _base(row.get("authorization_id"), "SIMULATED_CARD", row.get("account_number_token"),
                 row.get("merchant_account"), "CARD", row.get("authorized_amount"),
                 row.get("authorization_time"), status, source_file,
                 row.get("merchant_category"), row.get("device_id"))


def standardize_rows(rows: Iterable[dict[str, Any]], transformer: Callable[[dict[str, Any], str], dict[str, str]],
                     source_file: str) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    accepted, rejected = [], []
    for index, row in enumerate(rows, start=1):
        try:
            accepted.append(transformer(row, source_file))
        except (ValueError, TypeError, InvalidOperation) as error:
            rejected.append({
                "source_file": source_file,
                "row_number": str(index),
                "error": str(error),
                "raw_payment_id": str(row.get("upi_transaction_id") or row.get("utr_number") or row.get("authorization_id") or ""),
            })
    return accepted, rejected


def deduplicate(rows: Iterable[dict[str, str]]) -> tuple[list[dict[str, str]], int]:
    seen: set[tuple[str, str]] = set()
    output = []
    duplicates = 0
    for row in sorted(rows, key=lambda item: (item["event_timestamp"], item["payment_id"])):
        key = (row["source_system"], row["payment_id"])
        if key in seen:
            duplicates += 1
            continue
        seen.add(key)
        output.append(row)
    return output, duplicates
