from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def clean_contents(path: str | Path) -> None:
    target = Path(path).resolve()
    if target.name not in {"raw", "processed"} or target.parent.name != "data":
        raise ValueError(f"Refusing to clean unexpected path: {target}")
    if target.exists():
        for child in target.iterdir():
            if child.name == ".gitkeep":
                continue
            shutil.rmtree(child) if child.is_dir() else child.unlink()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="data/raw")
    parser.add_argument("--processed", default="data/processed")
    args = parser.parse_args()
    clean_contents(args.raw)
    clean_contents(args.processed)


if __name__ == "__main__":
    main()
