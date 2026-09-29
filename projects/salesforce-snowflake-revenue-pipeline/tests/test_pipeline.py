from __future__ import annotations

import unittest

from src.pipeline import business_key, deduplicate, normalize_record


class PipelineTests(unittest.TestCase):
    def test_business_key_is_stable(self):
        row = {"Id": "001A"}
        self.assertEqual(business_key("accounts", row), business_key("accounts", row))

    def test_deduplicate_keeps_latest_system_modstamp(self):
        older = normalize_record(
            "accounts",
            {"Id": "001A", "Name": "Old", "SystemModstamp": "2026-01-01T00:00:00Z"},
            "2026-01-02T00:00:00Z",
        )
        newer = normalize_record(
            "accounts",
            {"Id": "001A", "Name": "New", "SystemModstamp": "2026-02-01T00:00:00Z"},
            "2026-02-02T00:00:00Z",
        )
        result = deduplicate([older, newer])
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["Name"], "New")

    def test_normalization_adds_audit_columns(self):
        row = normalize_record(
            "contacts",
            {"Id": "003A", "SystemModstamp": "2026-01-01T00:00:00Z"},
            "2026-01-02T00:00:00Z",
        )
        self.assertEqual(row["_source_object"], "contacts")
        self.assertTrue(row["_business_key"])
        self.assertEqual(row["_extracted_at"], "2026-01-02T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
