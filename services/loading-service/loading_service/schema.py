"""Canonical schema files: casting bronze text to typed columns, and silver DDL."""
from pathlib import Path

import pandas as pd
import yaml

PG_TYPES = {"string": "TEXT", "integer": "BIGINT", "float": "DOUBLE PRECISION",
            "timestamp": "TIMESTAMP"}


def load_schema(path: str | Path) -> dict[str, str]:
    columns = yaml.safe_load(Path(path).read_text())["columns"]
    unknown = {t for t in columns.values() if t not in PG_TYPES}
    if unknown:
        raise ValueError(f"{path}: unsupported types {sorted(unknown)}")
    return columns


def cast_to_schema(df: pd.DataFrame, columns: dict[str, str]) -> pd.DataFrame:
    """Unparseable values become null so not_null rules catch them."""
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"source is missing columns {sorted(missing)}")
    out = pd.DataFrame(index=df.index)
    for col, typ in columns.items():
        s = df[col]
        if typ == "integer":
            out[col] = pd.to_numeric(s, errors="coerce").round().astype("Int64")
        elif typ == "float":
            out[col] = pd.to_numeric(s, errors="coerce")
        elif typ == "timestamp":
            out[col] = pd.to_datetime(s, errors="coerce", format="mixed")
        else:
            out[col] = s.astype("string")
    return out


def quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def silver_ddl(schema: str, table: str, columns: dict[str, str], load_type: str,
               business_keys: list[str]) -> str:
    cols = [f"{quote(c)} {PG_TYPES[t]}" for c, t in columns.items()]
    cols += ['"_batch_id" TEXT', '"_loaded_at" TIMESTAMPTZ NOT NULL DEFAULT now()']
    if load_type == "scd1":
        cols.append(f"PRIMARY KEY ({', '.join(map(quote, business_keys))})")
    elif load_type == "scd2":
        cols += ['"_row_hash" TEXT NOT NULL', '"_valid_from" TIMESTAMPTZ NOT NULL DEFAULT now()',
                 '"_valid_to" TIMESTAMPTZ', '"_is_current" BOOLEAN NOT NULL DEFAULT TRUE']
    return f"CREATE TABLE IF NOT EXISTS {quote(schema)}.{quote(table)} ({', '.join(cols)})"
