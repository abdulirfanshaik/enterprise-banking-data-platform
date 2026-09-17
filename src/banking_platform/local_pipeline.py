from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from .canonical import (CANONICAL_FIELDS, deduplicate, standardize_card, standardize_neft,
                        standardize_rows, standardize_rtgs, standardize_upi)
from .config import load_config
from .io_utils import ensure_dir, read_csv, read_jsonl, write_csv
from .quality import quality_report, reconcile


def _copy_to_bronze(source: Path, bronze: Path) -> None:
    if bronze.exists():
        shutil.rmtree(bronze)
    shutil.copytree(source, bronze)


def _load_and_standardize(source: Path) -> tuple[list[dict[str, str]], list[dict[str, str]], int]:
    specs = [
        (source / "upi" / "upi_transactions.jsonl", list, standardize_upi),
        (source / "neft" / "neft_transactions.csv", read_csv, standardize_neft),
        (source / "rtgs" / "rtgs_transactions.csv", read_csv, standardize_rtgs),
        (source / "cards" / "card_authorizations.jsonl", list, standardize_card),
    ]
    accepted, rejected = [], []
    input_rows = 0
    for path, reader, transformer in specs:
        rows = list(read_jsonl(path)) if path.suffix == ".jsonl" else reader(path)
        input_rows += len(rows)
        good, bad = standardize_rows(rows, transformer, str(path.relative_to(source)))
        accepted.extend(good)
        rejected.extend(bad)
    return accepted, rejected, input_rows


def _enrich_customer(payments: list[dict[str, str]], accounts: list[dict[str, str]]) -> None:
    mapping = {row["account_id"]: row["customer_id"] for row in accounts}
    for payment in payments:
        payment["customer_id"] = mapping.get(payment["source_account"], "UNKNOWN")


def _alerts(payments: list[dict[str, str]], labels: set[str], high_value: Decimal, modulus: Decimal) -> list[dict[str, str]]:
    alerts = []
    for payment in payments:
        amount = Decimal(payment["amount"])
        rules = []
        score = 0
        if payment["payment_id"] in labels:
            rules.append("SYNTHETIC_AML_LABEL")
            score += 60
        if amount >= high_value:
            rules.append("HIGH_VALUE")
            score += 30
        if amount >= high_value and amount % modulus == 0:
            rules.append("ROUND_AMOUNT")
            score += 20
        if rules:
            alerts.append({
                "alert_id": f"ALERT-{payment['payment_id']}",
                "payment_id": payment["payment_id"],
                "customer_id": payment["customer_id"],
                "event_timestamp": payment["event_timestamp"],
                "amount": payment["amount"],
                "rule_name": "+".join(rules),
                "risk_score": str(min(score, 100)),
                "disposition": "REVIEW",
            })
    return alerts


def _customer_360(customers: list[dict[str, str]], accounts: list[dict[str, str]],
                  payments: list[dict[str, str]], alerts: list[dict[str, str]]) -> list[dict[str, str]]:
    account_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for account in accounts:
        account_groups[account["customer_id"]].append(account)
    payment_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for payment in payments:
        payment_groups[payment["customer_id"]].append(payment)
    alert_count: dict[str, int] = defaultdict(int)
    for alert in alerts:
        alert_count[alert["customer_id"]] += 1
    output = []
    for customer in customers:
        customer_id = customer["customer_id"]
        owned_accounts = account_groups[customer_id]
        activity = payment_groups[customer_id]
        output.append({
            "customer_id": customer_id,
            "customer_name": f"{customer['first_name']} {customer['last_name']}",
            "city": customer["city"],
            "state": customer["state"],
            "kyc_status": customer["kyc_status"],
            "risk_rating": customer["risk_rating"],
            "account_count": str(len(owned_accounts)),
            "total_balance": f"{sum(Decimal(a['balance']) for a in owned_accounts):.2f}",
            "payment_count": str(len(activity)),
            "payment_value": f"{sum((Decimal(p['amount']) for p in activity), Decimal('0')):.2f}",
            "alert_count": str(alert_count[customer_id]),
            "last_activity_timestamp": max((p["event_timestamp"] for p in activity), default=""),
        })
    return output


def _regulatory_daily(payments: list[dict[str, str]]) -> list[dict[str, str]]:
    groups: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    for payment in payments:
        groups[(payment["event_timestamp"][:10], payment["payment_type"])].append(payment)
    result = []
    for (event_date, payment_type), rows in sorted(groups.items()):
        result.append({
            "event_date": event_date,
            "payment_type": payment_type,
            "transaction_count": str(len(rows)),
            "transaction_value": f"{sum((Decimal(r['amount']) for r in rows), Decimal('0')):.2f}",
            "success_count": str(sum(r["status"] == "SUCCESS" for r in rows)),
            "failed_count": str(sum(r["status"] == "FAILED" for r in rows)),
            "pending_count": str(sum(r["status"] == "PENDING" for r in rows)),
        })
    return result


def _sqlite_load(database: Path, tables: dict[str, tuple[list[dict[str, str]], list[str]]]) -> None:
    if database.exists():
        database.unlink()
    connection = sqlite3.connect(database)
    try:
        for name, (rows, fields) in tables.items():
            columns = ", ".join(f'"{field}" TEXT' for field in fields)
            connection.execute(f'CREATE TABLE "{name}" ({columns})')
            if rows:
                placeholders = ", ".join("?" for _ in fields)
                values = [[row.get(field, "") for field in fields] for row in rows]
                connection.executemany(f'INSERT INTO "{name}" VALUES ({placeholders})', values)
        connection.execute("CREATE INDEX idx_payments_id ON payments(payment_id)")
        connection.execute("CREATE INDEX idx_payments_customer ON payments(customer_id)")
        connection.commit()
    finally:
        connection.close()


def run_pipeline(input_dir: str | Path, output_dir: str | Path, config_path: str | Path | None = None) -> dict:
    source, output = Path(input_dir), Path(output_dir)
    if not source.exists():
        raise FileNotFoundError(f"Input directory does not exist: {source}")
    config = load_config(config_path)
    bronze, silver, gold, metrics = [ensure_dir(output / name) for name in ("bronze", "silver", "gold", "metrics")]
    _copy_to_bronze(source, bronze)

    standardized, rejected, input_rows = _load_and_standardize(source)
    payments, duplicate_count = deduplicate(standardized)
    customers = read_csv(source / "core_banking" / "customers.csv")
    accounts = read_csv(source / "core_banking" / "accounts.csv")
    _enrich_customer(payments, accounts)

    labels = {row["payment_id"] for row in read_csv(source / "aml" / "aml_labels.csv")}
    rule_config = config["aml_rules"]
    alerts = _alerts(payments, labels, Decimal(str(rule_config["high_value_amount"])), Decimal(str(rule_config["round_amount_modulus"])))
    customer_360 = _customer_360(customers, accounts, payments, alerts)
    regulatory = _regulatory_daily(payments)
    settlements = read_csv(source / "settlement" / "settlements.csv")
    reconciliation, reconciliation_counts = reconcile(payments, settlements)
    report = quality_report(input_rows, len(payments), len(rejected), duplicate_count,
                            reconciliation_counts, config["quality"])

    reject_fields = ["source_file", "row_number", "error", "raw_payment_id"]
    alert_fields = ["alert_id", "payment_id", "customer_id", "event_timestamp", "amount", "rule_name", "risk_score", "disposition"]
    c360_fields = ["customer_id", "customer_name", "city", "state", "kyc_status", "risk_rating", "account_count", "total_balance", "payment_count", "payment_value", "alert_count", "last_activity_timestamp"]
    regulatory_fields = ["event_date", "payment_type", "transaction_count", "transaction_value", "success_count", "failed_count", "pending_count"]
    recon_fields = ["payment_id", "payment_type", "event_date", "payment_amount", "settled_amount", "reconciliation_status"]

    write_csv(silver / "payments.csv", payments, CANONICAL_FIELDS)
    write_csv(silver / "rejected_records.csv", rejected, reject_fields)
    write_csv(gold / "aml_fraud_alerts.csv", alerts, alert_fields)
    write_csv(gold / "customer_360.csv", customer_360, c360_fields)
    write_csv(gold / "regulatory_daily.csv", regulatory, regulatory_fields)
    write_csv(gold / "settlement_reconciliation.csv", reconciliation, recon_fields)
    with (metrics / "quality_report.json").open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2, sort_keys=True)
        handle.write("\n")

    _sqlite_load(output / "banking_analytics.db", {
        "payments": (payments, CANONICAL_FIELDS),
        "aml_fraud_alerts": (alerts, alert_fields),
        "customer_360": (customer_360, c360_fields),
        "regulatory_daily": (regulatory, regulatory_fields),
        "settlement_reconciliation": (reconciliation, recon_fields),
    })
    print(json.dumps(report, indent=2, sort_keys=True))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local banking data pipeline")
    parser.add_argument("--input", default="data/raw")
    parser.add_argument("--output", default="data/processed")
    parser.add_argument("--config")
    args = parser.parse_args()
    run_pipeline(args.input, args.output, args.config)


if __name__ == "__main__":
    main()
