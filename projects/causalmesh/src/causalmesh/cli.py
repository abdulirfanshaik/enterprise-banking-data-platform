"""Local-first CLI for reproducible demos, audit, replay and optional Kafka."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from causalmesh.demo import populate, scenarios
from causalmesh.engine import Engine
from causalmesh.storage import Store


def main() -> None:
    parser = argparse.ArgumentParser(prog="causalmesh", description="CausalMesh control CLI")
    parser.add_argument("--db", default=os.environ.get("CAUSALMESH_DB", "data/causalmesh.db"))
    commands = parser.add_subparsers(dest="command", required=True)
    cmd_demo = commands.add_parser("demo", help="seed synthetic failure/correction scenarios")
    cmd_demo.add_argument("--transfers", type=int, default=250)
    cmd_demo.add_argument("--seed", type=int, default=42)
    cmd_ingest = commands.add_parser("ingest", help="ingest one JSON event per line")
    cmd_ingest.add_argument("file", type=Path)
    commands.add_parser("audit", help="verify the full cryptographic chain")
    commands.add_parser("replay", help="rebuild the entire current/temporal projection")
    commands.add_parser("stats", help="inspect reliability scorecard")
    cmd_lake = commands.add_parser("export", help="materialize Parquet with optional DuckDB")
    cmd_lake.add_argument("--output", default="data/gold")
    cmd_producer = commands.add_parser("produce-kafka", help="publish synthetic events to Redpanda")
    cmd_producer.add_argument("--transfers", type=int, default=250)
    cmd_consumer = commands.add_parser("consume-kafka", help="long-running Kafka consumer")
    for cmd in (cmd_producer, cmd_consumer):
        cmd.add_argument("--bootstrap", default="localhost:19092")
    args = parser.parse_args()
    with Store(args.db) as store:
        engine = Engine(store)
        if args.command == "demo":
            if args.transfers < 1 or args.transfers > 1_000_000:
                parser.error("--transfers must be between 1 and 1,000,000")
            result = populate(engine, args.transfers, args.seed)
        elif args.command == "ingest":
            accepted = []
            for line in args.file.read_text(encoding="utf-8").splitlines():
                if line.strip():
                    try:
                        accepted.append(engine.ingest(json.loads(line)))
                    except json.JSONDecodeError:
                        accepted.append(engine.ingest({"unparsed": line}))
            result = {"results": accepted, "metrics": engine.metrics()}
        elif args.command == "audit":
            result = engine.verify_audit()
        elif args.command == "replay":
            result = engine.replay()
        elif args.command == "stats":
            result = engine.metrics()
        elif args.command == "export":
            from causalmesh.lake import export_parquet
            result = export_parquet(engine, args.output)
        elif args.command == "produce-kafka":
            from causalmesh.stream import producer
            result = {"published": producer(args.bootstrap, list(scenarios(args.transfers)))}
        else:
            from causalmesh.stream import consume
            consume(engine, args.bootstrap)
            result = {"status": "stopped"}
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
