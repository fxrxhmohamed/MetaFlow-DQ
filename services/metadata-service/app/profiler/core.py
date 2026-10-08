"""Generic, dataset-agnostic column profiling.

Every column is read as text first (bronze is untyped), then we infer what it
*could* be. The shape signature (digits -> 9, letters -> A) is what exposes
mixed formats such as '1/1/2024 0:00' vs '2024-01-01 00:00:00' in one column.
"""
from __future__ import annotations

import math
import re
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "-"}

TIMESTAMP_FORMATS = [
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%m/%d/%y %H:%M",
]
DATE_FORMATS = ["%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"]


def shape(value: str) -> str:
    s = re.sub(r"[0-9]", "9", value)
    s = re.sub(r"[A-Za-z]", "A", s)
    return re.sub(r"(9)\1+|(A)\2+", lambda m: (m.group(1) or m.group(2)) + "+", s)


def is_null(series: pd.Series) -> pd.Series:
    return series.isna() | series.astype(str).str.strip().str.lower().isin(NULL_TOKENS)


def parse_with_formats(series: pd.Series, formats: list[str]) -> tuple[pd.Series, dict[str, int]]:
    """Try each format; first match wins per value. Returns parsed values and hits per format."""
    s = series.astype("string").str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    hits: dict[str, int] = {}
    for fmt in formats:
        todo = out.isna() & s.notna()
        if not todo.any():
            break
        parsed = pd.to_datetime(s[todo], format=fmt, errors="coerce")
        n = int(parsed.notna().sum())
        if n:
            hits[fmt] = n
            out.loc[parsed.index] = out.loc[parsed.index].fillna(parsed)
    return out, hits


@dataclass
class ColumnProfile:
    name: str
    rows: int
    nulls: int
    distinct: int
    inferred_type: str
    top_values: list[tuple[str, int]] = field(default_factory=list)
    shapes: list[tuple[str, int]] = field(default_factory=list)
    min: str | float | None = None
    max: str | float | None = None
    mean: float | None = None
    p50: float | None = None
    p99: float | None = None
    formats: dict[str, int] | None = None
    unparseable: int = 0

    @property
    def null_pct(self) -> float:
        return round(100 * self.nulls / self.rows, 3) if self.rows else 0.0

    def to_dict(self) -> dict:
        d = self.__dict__.copy()
        d["null_pct"] = self.null_pct
        return d


def profile_column(name: str, raw: pd.Series, top_n: int = 10) -> ColumnProfile:
    rows = len(raw)
    null_mask = is_null(raw)
    values = raw[~null_mask].astype(str).str.strip()
    prof = ColumnProfile(
        name=name, rows=rows, nulls=int(null_mask.sum()), distinct=int(values.nunique()),
        inferred_type="empty",
    )
    if values.empty:
        return prof
    prof.top_values = [(str(k), int(v)) for k, v in values.value_counts().head(top_n).items()]
    # shape() is per-value Python; a 100k sample is plenty to see mixed formats
    sample = values.sample(100_000, random_state=0) if len(values) > 100_000 else values
    prof.shapes = [(k, int(v)) for k, v in sample.map(shape).value_counts().head(5).items()]

    numeric = pd.to_numeric(values, errors="coerce")
    if numeric.notna().mean() > 0.98:
        is_int = bool((numeric.dropna() % 1 == 0).all())
        prof.inferred_type = "integer" if is_int else "float"
        prof.unparseable = int(numeric.isna().sum())
        prof.min, prof.max = float(numeric.min()), float(numeric.max())
        prof.mean = _r(numeric.mean())
        prof.p50, prof.p99 = _r(numeric.quantile(0.5)), _r(numeric.quantile(0.99))
        return prof

    for kind, formats in (("timestamp", TIMESTAMP_FORMATS), ("date", DATE_FORMATS)):
        parsed, hits = parse_with_formats(values, formats)
        if parsed.notna().mean() > 0.98:
            prof.inferred_type = kind
            prof.formats = hits
            prof.unparseable = int(parsed.isna().sum())
            prof.min, prof.max = str(parsed.min()), str(parsed.max())
            return prof

    prof.inferred_type = "categorical" if prof.distinct <= 50 else "string"
    lengths = values.str.len()
    prof.min, prof.max = int(lengths.min()), int(lengths.max())
    return prof


def profile_frame(df: pd.DataFrame) -> dict[str, dict]:
    return {c: profile_column(c, df[c]).to_dict() for c in df.columns}


def haversine_km(lat1, lon1, lat2, lon2) -> np.ndarray:
    lat1, lon1, lat2, lon2 = (np.radians(np.asarray(x, dtype=float)) for x in (lat1, lon1, lat2, lon2))
    a = np.sin((lat2 - lat1) / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    return 6371.0 * 2 * np.arcsin(np.sqrt(a))


def _r(x) -> float | None:
    return None if x is None or (isinstance(x, float) and math.isnan(x)) else round(float(x), 3)
