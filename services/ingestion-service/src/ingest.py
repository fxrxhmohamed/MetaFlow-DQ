"""Ingestion: land a source file in the immutable raw (bronze) layer.

Each run gets an ingestion id (ING_YYYYMMDD_NNN) and its own folder:

    data/raw/<dataset>/<ingestion_id>/
        <source file name>    byte-for-byte copy, set read-only
        manifest.json         what was ingested, from where, when, its
                              SHA-256 and row count, and how record ids
                              are formed

The copy is never modified. Later steps read it through the manifest,
check its hash, and identify every row by its record id:

    <ingestion_id>_ROW_<data row number, zero-padded>
    e.g. ING_20261011_001_ROW_0000001 is the first data row

so a record can be traced from raw to quarantine, correction and the
final dataset.

Usage
-----
    # Banking defaults
    python src/ingest.py

    # Another file or dataset
    python src/ingest.py --input landing/sales.csv --dataset sales
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import shutil
import stat
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "banking" / "banking_transactions.csv"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data" / "raw"
DEFAULT_DATASET = "banking_transactions"

INGESTION_ID_PATTERN = r"^ING_(\d{8})_(\d{3})$"
MIN_ROW_DIGITS = 6
HASH_CHUNK = 1 << 20


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(HASH_CHUNK), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_layout(path: Path, encoding: str, delimiter: str) -> tuple[list[str], int]:
    """Header and number of data rows (quoted line breaks count once)."""

    with path.open(encoding=encoding, newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        header = next(reader, [])
        rows = sum(1 for _ in reader)

    return header, rows


def next_ingestion_id(dataset_root: Path, today: str) -> str:
    """ING_<today>_NNN, one above the highest existing number for today."""

    numbers = [
        int(match.group(2))
        for folder in dataset_root.glob(f"ING_{today}_*")
        if (match := re.match(INGESTION_ID_PATTERN, folder.name))
    ]
    return f"ING_{today}_{max(numbers, default=0) + 1:03d}"


def relative_to_root(path: Path) -> str:
    """Path relative to the project root when possible (portable manifests)."""

    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.resolve().as_posix()


def ingest(
    source: Path,
    dataset: str,
    raw_root: Path,
    encoding: str = "utf-8-sig",
    delimiter: str = ",",
) -> dict[str, Any]:
    """Copy the source into a new ingestion folder and write its manifest."""

    if not source.is_file():
        raise FileNotFoundError(f"Input not found: {source}")

    started = datetime.now(timezone.utc)
    dataset_root = raw_root / dataset
    dataset_root.mkdir(parents=True, exist_ok=True)

    # Claim the folder atomically so two runs never share an id.
    while True:
        ingestion_id = next_ingestion_id(dataset_root, started.strftime("%Y%m%d"))
        folder = dataset_root / ingestion_id
        try:
            folder.mkdir()
            break
        except FileExistsError:
            continue

    stored = folder / source.name
    shutil.copyfile(source, stored)

    source_hash = sha256_of(source)
    stored_hash = sha256_of(stored)
    if stored_hash != source_hash:
        raise OSError(f"Copy of {source} does not match the source.")

    # Raw layer is immutable.
    os.chmod(stored, stat.S_IREAD | stat.S_IRGRP | stat.S_IROTH)

    columns, row_count = read_layout(stored, encoding, delimiter)
    width = max(MIN_ROW_DIGITS, len(str(row_count)))

    manifest = {
        "ingestion_id": ingestion_id,
        "dataset": dataset,
        "source_path": relative_to_root(source),
        "stored_file": stored.name,
        "stored_path": relative_to_root(stored),
        "sha256": stored_hash,
        "size_bytes": stored.stat().st_size,
        "encoding": encoding,
        "delimiter": delimiter,
        "row_count": row_count,
        "column_count": len(columns),
        "columns": columns,
        "record_id": {
            "prefix": f"{ingestion_id}_ROW_",
            "width": width,
            "first": f"{ingestion_id}_ROW_{1:0{width}d}" if row_count else None,
            "last": f"{ingestion_id}_ROW_{row_count:0{width}d}" if row_count else None,
        },
        "ingested_at": started.isoformat(),
        "profile": None,
    }

    with (folder / "manifest.json").open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2, ensure_ascii=False)

    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Land a CSV file in the immutable raw layer with an "
        "ingestion id and manifest.",
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT,
                        help="Source CSV file.")
    parser.add_argument("--dataset", default=DEFAULT_DATASET,
                        help="Dataset name (folder under the raw root).")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT,
                        help="Raw layer root folder.")
    parser.add_argument("--encoding", default="utf-8-sig")
    parser.add_argument("--delimiter", default=",")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = ingest(
        args.input.resolve(), args.dataset, args.raw_root,
        args.encoding, args.delimiter,
    )

    print(f"Ingested {manifest['source_path']}")
    print(f"  ingestion id  {manifest['ingestion_id']}")
    print(f"  stored at     {manifest['stored_path']} (read-only)")
    print(f"  rows          {manifest['row_count']}")
    print(f"  sha256        {manifest['sha256']}")
    print(f"  record ids    {manifest['record_id']['first']} .. "
          f"{manifest['record_id']['last']}")

    # Last line: the id alone, for scripts and Airflow (XCom).
    print(manifest["ingestion_id"])
    return 0


if __name__ == "__main__":
    sys.exit(main())
