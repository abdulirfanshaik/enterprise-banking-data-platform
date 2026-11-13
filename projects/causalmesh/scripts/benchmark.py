"""Measures one local sequential ingestion run; results are observations, not guarantees."""
from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from causalmesh.demo import scenarios  # noqa: E402
from causalmesh.engine import Engine  # noqa: E402
from causalmesh.storage import Store  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transfers", type=int, default=1000)
    args = parser.parse_args()
    if args.transfers < 1 or args.transfers > 1_000_000:
        parser.error("--transfers must be between 1 and 1,000,000")
    with tempfile.TemporaryDirectory() as folder:
        with Store(str(Path(folder) / "bench.db")) as db:
            eng = Engine(db)
            t0 = perf_counter()
            count = 0
            for payload in scenarios(args.transfers):
                eng.ingest(payload)
                count += 1
            elapsed = perf_counter() - t0
            print(json.dumps({
                "label": "synthetic single-process SQLite engine / local wall clock",
                "input_attempts": count,
                "elapsed_seconds": round(elapsed, 3),
                "attempts_per_second": round(count / elapsed, 1),
                "metrics": eng.metrics(),
                "audit": eng.verify_audit(),
            }, indent=2))


if __name__ == "__main__":
    main()
