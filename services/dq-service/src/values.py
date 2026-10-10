"""Value parsing and formatting shared by checks and remediation.

The engine keeps every column as text (pd.NA for null) so the original
spelling survives until a rule decides to change it. These helpers turn
that text into typed values for comparisons, and typed values back into
canonical text for output.
"""

from __future__ import annotations

import re
import warnings
from datetime import date
from typing import Any

import numpy as np
import pandas as pd


ISO_DATE_FORMAT = "%Y-%m-%d"
ISO_TIME_FORMAT = "%H:%M:%S"

# A number inside text, e.g. 'EGP 12,284.15' or '3780.11 LE'.
NUMBER_IN_TEXT_PATTERN = r"[+-]?\d[\d,.\s]*\d|[+-]?\d"

# hh:mm, hh:mm:ss or hh.mm.ss
TIME_TEXT_PATTERN = r"^(\d{1,2})[:.](\d{2})(?:[:.](\d{2}))?$"

NUMERIC_TYPES = ("integer", "decimal")
TEMPORAL_TYPES = ("date", "time", "year_month")


def as_flags(mask: pd.Series) -> pd.Series:
    """Turn a nullable boolean result into plain True/False."""

    return mask.fillna(False).astype(bool)


def normalize_key(values: pd.Series) -> pd.Series:
    """Fold case, spacing and separators: ' Internet_Banking' ->
    'internet banking'."""

    return (
        values.astype("string")
        .str.strip()
        .str.casefold()
        .str.replace(r"[\s_\-]+", " ", regex=True)
    )


def normalize_one(value: Any) -> str:
    """normalize_key for a single value."""

    return re.sub(r"[\s_\-]+", " ", str(value).strip().casefold())


def format_number(value: float) -> str:
    """Plain decimal text without exponent or trailing zeros."""

    return np.format_float_positional(value, trim="-")


def _strptime(values: pd.Series, fmt: str) -> pd.Series:
    """Parse text with a strptime format; failures -> NaT."""

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(
            values.astype(object), format=fmt, errors="coerce",
        )


def parse_temporal(values: pd.Series, formats: list[str]) -> pd.Series:
    """Parse with the first format that fits each value."""

    result = pd.Series(pd.NaT, index=values.index, dtype="datetime64[ns]")
    pending = values.notna()

    for fmt in formats:
        if not pending.any():
            break
        parsed = _strptime(values[pending], fmt)
        result[parsed.index] = result[parsed.index].fillna(parsed)
        pending &= result.isna()

    return result


def resolve_bound(value: Any, column_type: str) -> Any:
    """Turn a schema min/max into a comparable value."""

    if value is None:
        return None

    if column_type in TEMPORAL_TYPES:
        if value == "today":
            return pd.Timestamp(date.today())
        return pd.Timestamp(value)

    return float(value)


def parse_values(values: pd.Series, spec: dict[str, Any]) -> pd.Series:
    """Typed view of a text column according to its schema entry.

    Invalid values become NaN/NaT/NA. Dates and times are accepted in
    the source format and in the canonical (remediated) format.
    """

    column_type = spec.get("type", "string")

    if column_type in NUMERIC_TYPES:
        numbers = pd.to_numeric(
            values.astype(object), errors="coerce",
        ).astype("float64")
        numbers = numbers.where(np.isfinite(numbers))

        if column_type == "integer":
            numbers = numbers.where(numbers % 1 == 0)

        return numbers

    if column_type == "boolean":
        true_values = [str(v) for v in spec.get("true_values", ["true", "1"])]
        false_values = [str(v) for v in spec.get("false_values", ["false", "0"])]
        result = pd.Series(pd.NA, index=values.index, dtype="boolean")
        result[as_flags(values.isin(true_values))] = True
        result[as_flags(values.isin(false_values))] = False
        return result

    if column_type in TEMPORAL_TYPES:
        canonical = {
            "date": ISO_DATE_FORMAT,
            "time": ISO_TIME_FORMAT,
            "year_month": "%Y-%m",
        }[column_type]
        formats = [spec.get("source_format", canonical)]
        if canonical not in formats:
            formats.append(canonical)
        return parse_temporal(values, formats)

    return values


def canonical_text(
    typed: pd.Series,
    spec: dict[str, Any],
    formats: dict[str, str],
) -> pd.Series | None:
    """Canonical output text for temporal columns; None for others."""

    column_type = spec.get("type", "string")

    if column_type not in TEMPORAL_TYPES:
        return None

    default = {
        "date": ISO_DATE_FORMAT,
        "time": ISO_TIME_FORMAT,
        "year_month": "%Y-%m",
    }[column_type]

    text = typed.dt.strftime(formats.get(column_type, default))
    return text.astype("string").where(typed.notna())


# ============================================================
# Text repair used by remediation actions
# ============================================================

def recover_numbers(values: pd.Series) -> pd.Series:
    """Read numbers written with currency text or other separators.

    '3780.11 LE' -> 3780.11, 'EGP 12,284.15' -> 12284.15,
    '1786,35' -> 1786.35. Unreadable values -> NaN.
    """

    text = values.astype("string").str.strip()
    number = (
        text.str.extract(f"({NUMBER_IN_TEXT_PATTERN})", expand=False)
        .str.replace(r"\s", "", regex=True)
    )

    has_comma = as_flags(number.str.contains(",", regex=False))
    has_dot = as_flags(number.str.contains(".", regex=False))

    # '1786,35' or '1.234,56': the comma is the decimal mark.
    decimal_comma = (
        has_comma & ~has_dot
        & as_flags(number.str.match(r"^[+-]?\d+,\d{1,2}$"))
    ) | (
        has_comma & has_dot
        & as_flags(number.str.rfind(",").gt(number.str.rfind(".")))
    )

    cleaned = number.where(
        ~decimal_comma,
        number.str.replace(".", "", regex=False)
        .str.replace(",", ".", regex=False),
    )
    cleaned = cleaned.where(
        decimal_comma,
        cleaned.str.replace(",", "", regex=False),
    )

    return pd.to_numeric(cleaned.astype(object), errors="coerce")


def repair_times(values: pd.Series) -> pd.Series:
    """'12.30.15' -> '12:30:15', '9:05' -> '09:05:00'.

    Times outside 00:00:00-23:59:59 are not repaired (NA).
    """

    parts = values.astype("string").str.strip().str.extract(TIME_TEXT_PATTERN)
    hour = pd.to_numeric(parts[0].astype(object), errors="coerce")
    minute = pd.to_numeric(parts[1].astype(object), errors="coerce")
    second = pd.to_numeric(parts[2].astype(object), errors="coerce").fillna(0)

    valid = hour.le(23) & minute.le(59) & second.le(59)
    result = pd.Series(pd.NA, index=values.index, dtype="string")

    if valid.any():
        result[valid] = pd.Series(
            [
                f"{int(h):02d}:{int(m):02d}:{int(s):02d}"
                for h, m, s in zip(hour[valid], minute[valid], second[valid])
            ],
            index=valid[valid].index,
            dtype="string",
        )

    return result
