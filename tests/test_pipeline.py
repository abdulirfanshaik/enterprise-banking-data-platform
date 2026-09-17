from __future__ import annotations

import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from banking_platform.canonical import deduplicate, standardize_upi
from banking_platform.generator import generate_dataset
from banking_platform.local_pipeline import run_pipeline


class CanonicalTests(unittest.TestCase):
    def test_upi_mapping(self) -> None:
        row = {
            "upi_transaction_id": "P1", "payer_account": "A1", "payee_account": "A2",
            "amount_inr": "125.50", "transaction_timestamp": "2026-01-01T00:00:00Z",
            "transaction_status": "SUCCESS", "merchant_category": "grocery", "device_id": "D1",
        }
        result = standardize_upi(row, "upi.jsonl")
        self.assertEqual(result["payment_id"], "P1")
        self.assertEqual(result["amount"], "125.50")
        self.assertEqual(result["merchant_category"], "GROCERY")

    def test_duplicate_is_idempotent_by_source_and_id(self) -> None:
        row = standardize_upi({
            "upi_transaction_id": "P1", "payer_account": "A1", "payee_account": "A2",
            "amount_inr": "1", "transaction_timestamp": "2026-01-01T00:00:00Z",
            "transaction_status": "SUCCESS",
        }, "upi.jsonl")
        result, duplicate_count = deduplicate([row, dict(row)])
        self.assertEqual(len(result), 1)
        self.assertEqual(duplicate_count, 1)


class EndToEndTests(unittest.TestCase):
    def test_demo_pipeline_outputs_pass_quality_gate(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            generate_dataset(root / "raw", customer_count=30, account_count=45, transaction_count=240, seed=7)
            report = run_pipeline(root / "raw", root / "processed")
            self.assertEqual(report["status"], "PASS")
            self.assertGreater(report["canonical_rows"], 200)
            self.assertGreater(report["duplicate_rows_removed"], 0)
            self.assertGreater(report["rejected_rows"], 0)

            expected = [
                "silver/payments.csv", "silver/rejected_records.csv",
                "gold/aml_fraud_alerts.csv", "gold/customer_360.csv",
                "gold/regulatory_daily.csv", "gold/settlement_reconciliation.csv",
                "metrics/quality_report.json", "banking_analytics.db",
            ]
            for relative in expected:
                self.assertTrue((root / "processed" / relative).exists(), relative)

            with sqlite3.connect(root / "processed" / "banking_analytics.db") as connection:
                count = connection.execute("select count(*) from payments").fetchone()[0]
                self.assertEqual(count, report["canonical_rows"])

            with (root / "processed" / "gold" / "customer_360.csv").open(newline="", encoding="utf-8") as handle:
                self.assertEqual(len(list(csv.DictReader(handle))), 30)

            with (root / "processed" / "metrics" / "quality_report.json").open(encoding="utf-8") as handle:
                self.assertEqual(json.load(handle)["status"], "PASS")


if __name__ == "__main__":
    unittest.main()
