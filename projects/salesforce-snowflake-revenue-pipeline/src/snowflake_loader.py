from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

from .config import SnowflakeConfig


TABLES = {
    "accounts": "ACCOUNT_RAW",
    "contacts": "CONTACT_RAW",
    "opportunities": "OPPORTUNITY_RAW",
}


def read_jsonl(path: Path) -> pd.DataFrame:
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
    return pd.DataFrame(rows)


def load_directory(input_dir: Path) -> None:
    cfg = SnowflakeConfig.from_env()
    conn = snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        password=cfg.password,
        warehouse=cfg.warehouse,
        database=cfg.database,
        schema=cfg.schema,
        role=cfg.role,
    )
    try:
        for object_name, table_name in TABLES.items():
            frame = read_jsonl(input_dir / f"{object_name}.jsonl")
            frame.columns = [c.upper() for c in frame.columns]
            success, chunks, rows, _ = write_pandas(
                conn,
                frame,
                table_name,
                database=cfg.database,
                schema=cfg.schema,
                auto_create_table=False,
                overwrite=False,
            )
            if not success:
                raise RuntimeError(f"Snowflake load failed for {object_name}")
            print(f"{object_name}: loaded {rows} rows in {chunks} chunk(s)")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=Path("data/out"))
    args = parser.parse_args()
    load_directory(args.input)


if __name__ == "__main__":
    main()
