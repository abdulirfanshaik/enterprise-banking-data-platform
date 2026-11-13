"""CLI adapter for deterministic SQLite -> JSONL landing export."""
import argparse
import json
from causalmesh.cloud.export import export_accepted_events


def main() -> None:
    parser = argparse.ArgumentParser(description="Export audit-verified events to JSONL")
    parser.add_argument("--db", default="data/causalmesh.db")
    parser.add_argument("--output", default="data/landing/part-000.jsonl")
    args = parser.parse_args()
    print(json.dumps(export_accepted_events(args.db, args.output), indent=2))


if __name__ == "__main__":
    main()
