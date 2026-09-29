from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
import time
from typing import Any, Iterable

from simple_salesforce import Salesforce

from .config import SalesforceConfig


OBJECT_QUERIES = {
    "accounts": """
        SELECT Id, Name, Industry, BillingCountry, AnnualRevenue,
               CreatedDate, LastModifiedDate, SystemModstamp
        FROM Account
        WHERE SystemModstamp > {watermark}
        ORDER BY SystemModstamp
    """,
    "contacts": """
        SELECT Id, AccountId, FirstName, LastName, Email, Title,
               CreatedDate, LastModifiedDate, SystemModstamp
        FROM Contact
        WHERE SystemModstamp > {watermark}
        ORDER BY SystemModstamp
    """,
    "opportunities": """
        SELECT Id, AccountId, Name, StageName, Amount, Probability,
               CloseDate, IsClosed, IsWon, CreatedDate,
               LastModifiedDate, SystemModstamp
        FROM Opportunity
        WHERE SystemModstamp > {watermark}
        ORDER BY SystemModstamp
    """,
}


@dataclass
class ExtractionResult:
    object_name: str
    records: list[dict[str, Any]]
    max_watermark: str | None


def _clean_record(record: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in record.items() if k != "attributes"}


def load_mock_records(mock_dir: Path, object_name: str) -> ExtractionResult:
    path = mock_dir / f"{object_name}.json"
    records = json.loads(path.read_text(encoding="utf-8"))
    cleaned = [_clean_record(r) for r in records]
    stamps = [r.get("SystemModstamp") for r in cleaned if r.get("SystemModstamp")]
    return ExtractionResult(object_name, cleaned, max(stamps) if stamps else None)


class SalesforceExtractor:
    def __init__(self, config: SalesforceConfig, max_retries: int = 4):
        self.config = config
        self.max_retries = max_retries
        self.sf = Salesforce(
            username=config.username,
            password=config.password,
            security_token=config.security_token,
            domain=config.domain,
        )

    def _query_all_with_retry(self, soql: str) -> Iterable[dict[str, Any]]:
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.sf.query_all(soql)
                return response["records"]
            except Exception:
                if attempt == self.max_retries:
                    raise
                time.sleep(min(2 ** attempt, 30))
        return []

    def extract(self, object_name: str, watermark: str) -> ExtractionResult:
        if object_name not in OBJECT_QUERIES:
            raise ValueError(f"Unsupported Salesforce object: {object_name}")

        quoted_watermark = watermark if watermark.endswith("Z") else f"{watermark}Z"
        soql = OBJECT_QUERIES[object_name].format(watermark=quoted_watermark)
        records = [_clean_record(r) for r in self._query_all_with_retry(soql)]
        stamps = [r.get("SystemModstamp") for r in records if r.get("SystemModstamp")]
        return ExtractionResult(object_name, records, max(stamps) if stamps else watermark)
