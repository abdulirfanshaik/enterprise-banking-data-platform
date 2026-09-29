from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from typing import Any

from .config import SalesforceConfig
from .salesforce_client import SalesforceExtractor, load_mock_records


OBJECTS = ("accounts", "contacts", "opportunities")


def business_key(object_name: str, record: dict[str, Any]) -> str:
    raw = f"{object_name}|{record.get('Id', '')}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_record(object_name: str, record: dict[str, Any], extracted_at: str) -> dict[str, Any]:
    normalized = dict(record)
    normalized["_source_object"] = object_name
    normalized["_business_key"] = business_key(object_name, record)
    normalized["_extracted_at"] = extracted_at
    return normalized


def deduplicate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    latest: dict[str, dict[str, Any]] = {}
    for record in records:
        key = record["_business_key"]
        current = latest.get(key)
        if current is None or record.get("SystemModstamp", "") >= current.get("SystemModstamp", ""):
            latest[key] = record
    return list(latest.values())


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def run(mode: str, output: Path, mock_dir: Path) -> dict[str, Any]:
    extracted_at = datetime.now(timezone.utc).isoformat()
    sf_config = SalesforceConfig.from_env()
    extractor = SalesforceExtractor(sf_config) if mode == "salesforce" else None

    manifest: dict[str, Any] = {
        "run_started_at": extracted_at,
        "mode": mode,
        "objects": {},
        "quality_status": "PASS",
    }

    for object_name in OBJECTS:
        result = (
            extractor.extract(object_name, sf_config.watermark)
            if extractor
            else load_mock_records(mock_dir, object_name)
        )
        normalized = [normalize_record(object_name, r, extracted_at) for r in result.records]
        canonical = deduplicate(normalized)

        duplicate_count = len(normalized) - len(canonical)
        missing_id_count = sum(1 for r in canonical if not r.get("Id"))
        if missing_id_count:
            manifest["quality_status"] = "FAIL"

        write_jsonl(output / f"{object_name}.jsonl", canonical)
        manifest["objects"][object_name] = {
            "extracted_count": len(normalized),
            "canonical_count": len(canonical),
            "duplicate_count": duplicate_count,
            "missing_id_count": missing_id_count,
            "max_watermark": result.max_watermark,
        }

    (output / "control_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8"
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["mock", "salesforce"], default="mock")
    parser.add_argument("--output", type=Path, default=Path("data/out"))
    parser.add_argument("--mock-dir", type=Path, default=Path("data/mock"))
    args = parser.parse_args()

    manifest = run(args.mode, args.output, args.mock_dir)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
