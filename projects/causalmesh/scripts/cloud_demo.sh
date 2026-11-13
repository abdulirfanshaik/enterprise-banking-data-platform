#!/usr/bin/env bash
set -euo pipefail
DB="${CAUSALMESH_DB:-data/causalmesh.db}"
RAW="${CAUSALMESH_LANDING:-data/landing/part-000.jsonl}"
OUTPUT="${CAUSALMESH_LAKE:-data/lake}"
python -m causalmesh --db "$DB" demo --transfers 50
python -m causalmesh.cloud.export_cli --db "$DB" --output "$RAW"
python -m causalmesh.cloud.job --input "$(dirname "$RAW")" --output "$OUTPUT" --format parquet
