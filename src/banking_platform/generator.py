from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .io_utils import ensure_dir, write_csv, write_jsonl


FIRST_NAMES = ["Aarav", "Aditi", "Arjun", "Diya", "Ishaan", "Kavya", "Meera", "Neha", "Rohan", "Vivaan"]
LAST_NAMES = ["Gupta", "Iyer", "Khan", "Mehta", "Patel", "Rao", "Shah", "Sharma", "Singh", "Verma"]
CITIES = [("Mumbai", "MH"), ("Delhi", "DL"), ("Bengaluru", "KA"), ("Hyderabad", "TS"), ("Chennai", "TN"), ("Pune", "MH")]


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def generate_dataset(output: str | Path, customer_count: int = 250, account_count: int = 400,
                     transaction_count: int = 3000, seed: int = 42) -> dict[str, int]:
    rng = random.Random(seed)
    root = ensure_dir(output)
    ensure_dir(root / "core_banking")
    ensure_dir(root / "upi")
    ensure_dir(root / "neft")
    ensure_dir(root / "rtgs")
    ensure_dir(root / "cards")
    ensure_dir(root / "aml")
    ensure_dir(root / "settlement")

    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    customers = []
    for index in range(1, customer_count + 1):
        city, state = rng.choice(CITIES)
        customers.append({
            "customer_id": f"C{index:06d}",
            "first_name": rng.choice(FIRST_NAMES),
            "last_name": rng.choice(LAST_NAMES),
            "city": city,
            "state": state,
            "kyc_status": rng.choices(["VERIFIED", "PENDING", "EXPIRED"], [0.92, 0.05, 0.03])[0],
            "risk_rating": rng.choices(["LOW", "MEDIUM", "HIGH"], [0.75, 0.20, 0.05])[0],
            "customer_since": (base.date() - timedelta(days=rng.randint(30, 3650))).isoformat(),
        })

    accounts = []
    for index in range(1, account_count + 1):
        customer = rng.choice(customers)
        accounts.append({
            "account_id": f"A{index:08d}",
            "customer_id": customer["customer_id"],
            "account_type": rng.choices(["SAVINGS", "CURRENT"], [0.83, 0.17])[0],
            "branch_id": f"B{rng.randint(1, 40):04d}",
            "status": rng.choices(["ACTIVE", "DORMANT", "BLOCKED"], [0.95, 0.03, 0.02])[0],
            "balance": f"{rng.uniform(500, 1500000):.2f}",
        })

    write_csv(root / "core_banking" / "customers.csv", customers, list(customers[0]))
    write_csv(root / "core_banking" / "accounts.csv", accounts, list(accounts[0]))

    account_ids = [row["account_id"] for row in accounts]
    logical = []
    aml_labels = []
    for index in range(1, transaction_count + 1):
        payment_type = rng.choices(["UPI", "NEFT", "RTGS", "CARD"], [0.52, 0.19, 0.08, 0.21])[0]
        source_account = rng.choice(account_ids)
        destination_account = rng.choice(account_ids)
        while destination_account == source_account:
            destination_account = rng.choice(account_ids)
        amount = round(rng.lognormvariate(8.0, 1.15), 2)
        if payment_type == "RTGS":
            amount = max(amount, round(rng.uniform(200000, 1500000), 2))
        if index % 113 == 0:
            amount = float(rng.choice([200000, 500000, 1000000]))
        event_time = base + timedelta(seconds=rng.randint(0, 30 * 24 * 3600 - 1))
        status = rng.choices(["SUCCESS", "FAILED", "PENDING"], [0.93, 0.05, 0.02])[0]
        payment_id = f"P{index:010d}"
        is_suspicious = amount >= 500000 or (amount >= 200000 and amount % 10000 == 0) or index % 197 == 0
        logical.append({
            "payment_id": payment_id,
            "payment_type": payment_type,
            "source_account": source_account,
            "destination_account": destination_account,
            "amount": amount,
            "currency": "INR",
            "event_time": _iso(event_time),
            "status": status,
            "merchant_category": rng.choice(["GROCERY", "TRAVEL", "FUEL", "ECOMMERCE", "TRANSFER"]),
            "device_id": f"D{rng.randint(1, 750):06d}",
        })
        if is_suspicious:
            aml_labels.append({
                "payment_id": payment_id,
                "label": "SUSPICIOUS",
                "reason": "SYNTHETIC_TRAINING_PATTERN",
            })

    upi, neft, rtgs, cards = [], [], [], []
    for row in logical:
        if row["payment_type"] == "UPI":
            upi.append({
                "upi_transaction_id": row["payment_id"],
                "payer_account": row["source_account"],
                "payee_account": row["destination_account"],
                "amount_inr": row["amount"],
                "transaction_timestamp": row["event_time"],
                "transaction_status": row["status"],
                "merchant_category": row["merchant_category"],
                "device_id": row["device_id"],
            })
        elif row["payment_type"] == "NEFT":
            neft.append({
                "utr_number": row["payment_id"],
                "sender_account": row["source_account"],
                "beneficiary_account": row["destination_account"],
                "amount": row["amount"],
                "transaction_date": row["event_time"],
                "settlement_batch": f"N{row['event_time'][:10]}-{rng.randint(1, 12):02d}",
                "status": row["status"],
            })
        elif row["payment_type"] == "RTGS":
            rtgs.append({
                "utr_number": row["payment_id"],
                "originating_bank": "SYNTHBANK",
                "beneficiary_bank": f"BANK{rng.randint(1, 15):02d}",
                "sender_account": row["source_account"],
                "beneficiary_account": row["destination_account"],
                "amount": row["amount"],
                "value_date": row["event_time"],
                "settlement_timestamp": _iso(datetime.fromisoformat(row["event_time"].replace("Z", "+00:00")) + timedelta(minutes=rng.randint(1, 25))),
                "status": row["status"],
            })
        else:
            cards.append({
                "authorization_id": row["payment_id"],
                "account_number_token": row["source_account"],
                "merchant_account": row["destination_account"],
                "authorized_amount": row["amount"],
                "authorization_time": row["event_time"],
                "response_code": "00" if row["status"] == "SUCCESS" else "05",
                "merchant_category": row["merchant_category"],
                "device_id": row["device_id"],
            })

    # Inject deterministic duplicate and malformed rows to exercise quality controls.
    duplicate_rows = max(1, transaction_count // 50)
    for rows in (upi, neft, rtgs, cards):
        rows.extend(dict(row) for row in rows[: max(1, duplicate_rows // 4)])
    if upi:
        upi[0]["amount_inr"] = ""
    if neft:
        neft[0]["sender_account"] = ""
    if cards:
        cards[0]["authorization_time"] = "not-a-timestamp"

    write_jsonl(root / "upi" / "upi_transactions.jsonl", upi)
    write_csv(root / "neft" / "neft_transactions.csv", neft, list(neft[0]))
    write_csv(root / "rtgs" / "rtgs_transactions.csv", rtgs, list(rtgs[0]))
    write_jsonl(root / "cards" / "card_authorizations.jsonl", cards)
    write_csv(root / "aml" / "aml_labels.csv", aml_labels, ["payment_id", "label", "reason"])

    settlements = []
    for row in logical:
        if row["status"] != "SUCCESS" or rng.random() < 0.035:
            continue
        settled_amount = row["amount"]
        if int(row["payment_id"][1:]) % 251 == 0:
            settled_amount = round(settled_amount - 1.0, 2)
        event_time = datetime.fromisoformat(row["event_time"].replace("Z", "+00:00"))
        settlements.append({
            "payment_id": row["payment_id"],
            "settled_amount": f"{settled_amount:.2f}",
            "settlement_status": "SETTLED",
            "settlement_timestamp": _iso(event_time + timedelta(minutes=rng.randint(1, 90))),
        })
    write_csv(root / "settlement" / "settlements.csv", settlements, list(settlements[0]))

    summary = {
        "customers": len(customers), "accounts": len(accounts),
        "logical_transactions": len(logical), "physical_source_rows": sum(map(len, (upi, neft, rtgs, cards))),
        "aml_labels": len(aml_labels), "settlements": len(settlements),
    }
    print("Generated synthetic banking sources:", summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate deterministic synthetic banking data")
    parser.add_argument("--output", default="data/raw")
    parser.add_argument("--customers", type=int, default=250)
    parser.add_argument("--accounts", type=int, default=400)
    parser.add_argument("--transactions", type=int, default=3000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()
    generate_dataset(args.output, args.customers, args.accounts, args.transactions, args.seed)


if __name__ == "__main__":
    main()
