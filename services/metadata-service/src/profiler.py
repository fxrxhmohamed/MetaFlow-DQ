"""Generic, read-only CSV data profiler (exploration only).

Scans one CSV file or every CSV under a folder and writes one JSON profile
per file. Source data is never modified.

The profiler only describes what is in the data. It does not decide what
is acceptable: there are no severities, thresholds or pass/fail results.
Use the profiles to write the data quality rules, which live in a
separate rules file.

Usage examples
--------------
    # Same behaviour as before (banking defaults)
    python profile_data.py

    # Any other dataset
    python profile_data.py --input data/raw/sales --output data/profiling/sales \
        --dataset-name sales

    # Custom null tokens and delimiter
    python profile_data.py --null-tokens NULL N/A unknown --delimiter ";"
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import warnings
from pathlib import Path
from typing import Any

import pandas as pd


# ============================================================
# Defaults (all can be overridden from the command line)
# ============================================================

def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "banking"
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "profiling"
DEFAULT_DATASET_NAME = "banking_transactions"

# Text that means "no value". Compared after strip + casefold.
DEFAULT_NULL_TOKENS = (
    "null", "none", "nan", "n/a", "na", "#n/a", "nil", "-", "--", "?",
)

# Share of non-missing values that must parse for a type to be assigned.
TYPE_THRESHOLD = 0.80

# A date-like column name lowers the bar, so badly formatted date columns
# are still profiled (and flagged) as dates.
DATE_NAME_HINT_THRESHOLD = 0.50

# Below these limits a text column is called "categorical".
CATEGORICAL_MAX_UNIQUE = 50
CATEGORICAL_MAX_UNIQUE_RATIO = 0.05

DATE_SAMPLE_SIZE = 1000


# ============================================================
# Patterns and name hints
# ============================================================

ID_NAME_TOKENS = {"id", "uuid", "guid"}
DATE_NAME_TOKENS = {"date", "datetime", "timestamp", "dob"}
BOOLEAN_NAME_TOKENS = {"is", "has", "flag"}
EMAIL_NAME_TOKENS = {"email", "mail"}
PHONE_NAME_TOKENS = {"phone", "mobile", "tel", "telephone"}

BOOLEAN_PAIRS = (
    {"true", "false"},
    {"yes", "no"},
    {"y", "n"},
    {"t", "f"},
    {"0", "1"},
)

EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
PHONE_PATTERN = r"^\+?[\d\s().\-]{7,20}$"
TIME_PATTERN = r"^\d{1,2}:\d{2}(:\d{2}(\.\d+)?)?(\s?[ap]m)?$"
LEADING_ZERO_PATTERN = r"^[+-]?0\d+$"
DAY_MONTH_PATTERN = r"^(\d{1,2})[/.\-](\d{1,2})[/.\-]\d{2,4}(?:\D|$)"


# ============================================================
# Helpers
# ============================================================

def safe_value(value: Any) -> Any:
    """Convert pandas/numpy values into JSON-safe values."""

    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass

    if isinstance(value, pd.Timestamp):
        return str(value)

    if hasattr(value, "item"):
        value = value.item()

    # inf / -inf are not valid JSON.
    if isinstance(value, float) and not math.isfinite(value):
        return None

    return value


def name_tokens(column: str) -> list[str]:
    """Split a column name into lowercase words.

    'customerID' -> ['customer', 'id'], 'Account-Balance' -> ['account',
    'balance']. Matching whole words avoids substring mistakes such as
    'count' inside 'account'.
    """

    snake = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", column.strip())
    return [
        token
        for token in re.split(r"[^A-Za-z0-9]+", snake.lower())
        if token
    ]


def missing_masks(
    series: pd.Series,
    null_tokens: frozenset[str],
) -> dict[str, pd.Series]:
    """Classify each value as true null, blank, null token, or present."""

    stripped = series.str.strip()

    null = series.isna().astype(bool)
    blank = (~null) & stripped.eq("").fillna(False).astype(bool)
    token = (~null) & stripped.str.casefold().isin(null_tokens)

    return {
        "null": null,
        "blank": blank,
        "token": token,
        "missing": null | blank | token,
    }


def to_numeric_clean(values: pd.Series) -> pd.Series:
    """Parse values as numbers; unparseable and infinite values -> NaN."""

    numeric = pd.to_numeric(
        values.astype(object),
        errors="coerce",
    ).astype("float64")

    return numeric.where(numeric.abs() != float("inf"))


def parse_dates(values: pd.Series) -> pd.Series:
    """Parse values as dates; unparseable values -> NaT."""

    as_object = values.astype(object)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")

        try:
            # pandas >= 2.0
            parsed = pd.to_datetime(
                as_object,
                errors="coerce",
                format="mixed",
                utc=True,
            )
        except (ValueError, TypeError):
            # Older pandas versions do not support format="mixed".
            parsed = pd.to_datetime(
                as_object,
                errors="coerce",
                utc=True,
            )

    return parsed.dt.tz_localize(None)


def make_note(code: str, detail: str) -> dict[str, str]:
    """A neutral observation about the data, not a rule result."""

    return {"code": code, "detail": detail}


def match_rate(values: pd.Series, pattern: str) -> float:
    """Share of values matching a regular expression."""

    if not len(values):
        return 0.0

    return float(
        values.str.match(pattern).fillna(False).astype(bool).mean()
    )


# ============================================================
# Type inference (from the data; the name is only a hint)
# ============================================================

def infer_logical_type(
    column: str,
    values: pd.Series,
) -> dict[str, Any]:
    """Infer a logical type from the non-missing values of a column.

    Returns a dict with 'type' and 'basis', plus the parsed 'numeric' or
    'dates' series when relevant so they are not computed twice.
    """

    tokens = name_tokens(column)
    token_set = set(tokens)
    value_count = len(values)

    if value_count == 0:
        return {"type": "empty", "basis": "values"}

    # ---- Identifier: never treated as a number ----
    if tokens and (tokens[-1] in ID_NAME_TOKENS or tokens[0] == "id"):
        return {"type": "identifier", "basis": "name"}

    # ---- Phone: digits only, but not a quantity ----
    if token_set & PHONE_NAME_TOKENS:
        return {"type": "phone", "basis": "name"}

    stripped = values.str.strip()
    folded = stripped.str.casefold()

    # ---- Boolean ----
    distinct = set(folded.unique().tolist())

    for pair in BOOLEAN_PAIRS:
        if distinct <= pair:
            # A column containing only "0" or only "1" is ambiguous,
            # so it needs a name hint to be called boolean.
            if (
                pair != {"0", "1"}
                or len(distinct) == 2
                or token_set & BOOLEAN_NAME_TOKENS
            ):
                return {"type": "boolean", "basis": "values"}

    # ---- Numeric ----
    numeric = to_numeric_clean(values)

    if float(numeric.notna().mean()) >= TYPE_THRESHOLD:
        valid = numeric.dropna()
        kind = "integer" if bool((valid % 1 == 0).all()) else "float"

        return {"type": kind, "basis": "values", "numeric": numeric}

    # ---- Time of day ----
    if match_rate(folded, TIME_PATTERN) >= TYPE_THRESHOLD:
        return {"type": "time", "basis": "values"}

    # ---- Date / datetime ----
    date_hint = bool(token_set & DATE_NAME_TOKENS)
    threshold = DATE_NAME_HINT_THRESHOLD if date_hint else TYPE_THRESHOLD

    # Words like "may" or "sat" parse as dates, so require a digit.
    has_digit = (
        values.str.contains(r"\d", regex=True).fillna(False).astype(bool)
    )
    candidates = values[has_digit]

    if len(candidates) / value_count >= threshold:
        # Cheap check on a sample before parsing a large column.
        sample = candidates.sample(
            min(len(candidates), DATE_SAMPLE_SIZE),
            random_state=0,
        )

        if float(parse_dates(sample).notna().mean()) >= 0.5:
            dates = parse_dates(candidates).reindex(values.index)

            if float(dates.notna().mean()) >= threshold:
                valid_dates = dates.dropna()
                has_time = bool(
                    (valid_dates != valid_dates.dt.normalize()).any()
                )

                return {
                    "type": "datetime" if has_time else "date",
                    "basis": "name_and_values" if date_hint else "values",
                    "dates": dates,
                }

    # ---- Email ----
    if match_rate(stripped, EMAIL_PATTERN) >= TYPE_THRESHOLD:
        return {"type": "email", "basis": "values"}

    if token_set & EMAIL_NAME_TOKENS:
        return {"type": "email", "basis": "name"}

    # ---- Categorical vs free text ----
    unique_count = int(values.nunique())

    if (
        unique_count <= CATEGORICAL_MAX_UNIQUE
        or unique_count / value_count <= CATEGORICAL_MAX_UNIQUE_RATIO
    ):
        return {"type": "categorical", "basis": "values"}

    return {"type": "text", "basis": "values"}


# ============================================================
# Column profiling
# ============================================================

def profile_column(
    df: pd.DataFrame,
    column: str,
    null_tokens: frozenset[str],
) -> dict[str, Any]:
    """Profile a single column without modifying the source data."""

    series = df[column].astype("string")
    masks = missing_masks(series, null_tokens)

    values = series[~masks["missing"]]
    stripped = values.str.strip()

    row_count = len(series)
    value_count = len(values)
    missing_count = int(masks["missing"].sum())
    null_token_count = int(masks["token"].sum())
    unique_count = int(values.nunique())
    whitespace_count = int(
        values.ne(stripped).fillna(False).astype(bool).sum()
    )

    missing_percentage = (
        round(missing_count / row_count * 100, 4) if row_count else 0
    )

    inferred = infer_logical_type(column, values)
    logical_type = inferred["type"]

    profile: dict[str, Any] = {
        "column_name": column,
        "logical_type": logical_type,
        "type_inference_basis": inferred["basis"],
        "row_count": row_count,
        "null_count": int(masks["null"].sum()),
        "blank_count": int(masks["blank"].sum()),
        "null_token_count": null_token_count,
        "missing_count": missing_count,
        "missing_percentage": missing_percentage,
        "unique_count": unique_count,
        "unique_percentage": round(
            unique_count / value_count * 100, 4
        ) if value_count else 0,
        "whitespace_count": whitespace_count,
        "sample_values": [
            safe_value(value) for value in values.head(5).tolist()
        ],
    }

    # Neutral notes on things worth a look when writing the rules.
    flags: list[dict[str, str]] = []

    # --------------------------------------------------------
    # Missing-value observations
    # --------------------------------------------------------

    if row_count and value_count == 0:
        flags.append(make_note(
            "ALL_MISSING", "Column has no usable values.",
        ))

    if null_token_count:
        tokens_found = (
            series[masks["token"]].str.strip().value_counts().head(5)
        )
        flags.append(make_note(
            "NULL_TOKENS",
            f"{null_token_count} values are null placeholders: "
            + ", ".join(f"'{t}' x{c}" for t, c in tokens_found.items()),
        ))

    if whitespace_count:
        flags.append(make_note(
            "WHITESPACE",
            f"{whitespace_count} values have leading/trailing spaces.",
        ))

    if unique_count == 1 and row_count > 1:
        flags.append(make_note(
            "CONSTANT", "Column contains a single distinct value.",
        ))

    # --------------------------------------------------------
    # Numeric profiling (only for columns inferred as numeric)
    # --------------------------------------------------------

    if logical_type in ("integer", "float"):
        numeric = inferred["numeric"]
        valid = numeric.dropna()
        failures = values[numeric.isna()]
        failure_count = len(failures)

        def as_number(value: Any) -> Any:
            value = safe_value(value)
            if value is not None and logical_type == "integer":
                return int(value)
            return value

        leading_zero_count = int(
            stripped.str.match(LEADING_ZERO_PATTERN)
            .fillna(False).astype(bool).sum()
        )

        profile.update({
            "numeric_parse_success_rate": round(
                len(valid) / value_count * 100, 4
            ),
            "numeric_parse_failure_count": failure_count,
            "numeric_parse_failures_sample": [
                safe_value(value) for value in failures.head(5).tolist()
            ],
            "min": as_number(valid.min()),
            "max": as_number(valid.max()),
            "mean": safe_value(valid.mean()),
            "median": safe_value(valid.median()),
            "std": safe_value(valid.std()),
            "zero_count": int(valid.eq(0).sum()),
            "negative_count": int(valid.lt(0).sum()),
            "leading_zero_count": leading_zero_count,
        })

        if failure_count:
            flags.append(make_note(
                "MIXED_TYPES",
                f"{failure_count} non-numeric values in a numeric column.",
            ))

        if leading_zero_count:
            flags.append(make_note(
                "LEADING_ZEROS",
                f"{leading_zero_count} values start with 0; this may be "
                "a code or identifier rather than a number.",
            ))

    # --------------------------------------------------------
    # Date profiling (any column whose values parse as dates)
    # --------------------------------------------------------

    if logical_type in ("date", "datetime"):
        dates = inferred["dates"]
        valid_dates = dates.dropna()
        failures = values[dates.isna()]
        failure_count = len(failures)

        # 03/04/2025 could be 3 April or 4 March.
        parts = stripped.str.extract(DAY_MONTH_PATTERN)
        first = pd.to_numeric(parts[0].astype(object), errors="coerce")
        second = pd.to_numeric(parts[1].astype(object), errors="coerce")
        ambiguous_count = int(
            ((first <= 12) & (second <= 12) & (first != second)).sum()
        )

        profile.update({
            "date_parse_success_count": len(valid_dates),
            "date_parse_failure_count": failure_count,
            "date_parse_failures_sample": [
                safe_value(value) for value in failures.head(5).tolist()
            ],
            "min_date": str(valid_dates.min()),
            "max_date": str(valid_dates.max()),
            "ambiguous_day_month_count": ambiguous_count,
        })

        if failure_count:
            flags.append(make_note(
                "MIXED_TYPES",
                f"{failure_count} values could not be parsed as dates.",
            ))

        if ambiguous_count:
            flags.append(make_note(
                "AMBIGUOUS_DATES",
                f"{ambiguous_count} values could be read as either "
                "day/month or month/day; min/max dates may be wrong.",
            ))

    # --------------------------------------------------------
    # Format validation
    # --------------------------------------------------------

    if logical_type in ("email", "phone"):
        if logical_type == "email":
            valid_format = (
                stripped.str.match(EMAIL_PATTERN)
                .fillna(False).astype(bool)
            )
        else:
            valid_format = (
                stripped.str.match(PHONE_PATTERN)
                .fillna(False).astype(bool)
                & stripped.str.count(r"\d").ge(7).fillna(False).astype(bool)
            )

        invalid = values[~valid_format]

        profile["invalid_format_count"] = len(invalid)
        profile["invalid_format_sample"] = [
            safe_value(value) for value in invalid.head(5).tolist()
        ]

        if len(invalid):
            flags.append(make_note(
                "INVALID_FORMAT",
                f"{len(invalid)} values are not valid {logical_type} "
                "values.",
            ))

    if logical_type == "identifier":
        # Repeats beyond the first occurrence of each value.
        profile["duplicate_value_count"] = int(values.duplicated().sum())

    # --------------------------------------------------------
    # Consistency profiling
    # --------------------------------------------------------

    if value_count:
        variants = pd.DataFrame({
            "original": values,
            "normalized": stripped.str.casefold(),
        })

        variant_count = int(
            variants.groupby("normalized")["original"]
            .nunique()
            .gt(1)
            .sum()
        )

        profile["potential_case_or_whitespace_variants"] = variant_count

        profile["top_values"] = [
            {"value": safe_value(value), "count": int(count)}
            for value, count in values.value_counts().head(10).items()
        ]

        if variant_count and logical_type in ("categorical", "text"):
            flags.append(make_note(
                "CASE_OR_WHITESPACE_VARIANTS",
                f"{variant_count} values appear in more than one "
                "spelling (case or spacing).",
            ))

    profile["observations"] = flags

    return profile


# ============================================================
# File profiling
# ============================================================

def read_csv(file_path: Path, delimiter: str) -> tuple[pd.DataFrame, str]:
    """Read a CSV as strings, trying common encodings."""

    for encoding in ("utf-8-sig", "cp1252", "latin-1"):
        try:
            # Read as strings to preserve original source values.
            df = pd.read_csv(
                file_path,
                dtype="string",
                keep_default_na=False,
                sep=delimiter,
                encoding=encoding,
            )
            return df, encoding
        except UnicodeDecodeError:
            continue

    raise ValueError(f"Could not decode {file_path}")


def profile_file(
    file_path: Path,
    base_dir: Path,
    dataset_name: str,
    null_tokens: frozenset[str],
    delimiter: str,
) -> dict[str, Any]:
    """Profile one CSV file."""

    print(f"\nProfiling: {file_path}")

    df, encoding = read_csv(file_path, delimiter)

    columns = [
        profile_column(df, column, null_tokens)
        for column in df.columns
    ]

    duplicate_rows = int(df.duplicated().sum())

    file_flags: list[dict[str, str]] = []

    if duplicate_rows:
        file_flags.append(make_note(
            "DUPLICATE_ROWS",
            f"{duplicate_rows} rows are exact copies of an earlier row.",
        ))

    untidy_names = [c for c in df.columns if c != c.strip()]

    if untidy_names:
        file_flags.append(make_note(
            "COLUMN_NAME_WHITESPACE",
            f"Column names with surrounding spaces: {untidy_names}",
        ))

    # Columns that are fully populated and unique: possible keys.
    candidate_keys = [
        column["column_name"]
        for column in columns
        if len(df)
        and column["missing_count"] == 0
        and column["unique_count"] == len(df)
    ]

    return {
        "dataset_name": dataset_name,
        "source_file": file_path.name,
        "source_relative_path": file_path.relative_to(base_dir).as_posix(),
        "encoding": encoding,
        "row_count": len(df),
        "column_count": len(df.columns),
        "duplicate_row_count": duplicate_rows,
        "duplicate_row_percentage": round(
            duplicate_rows / len(df) * 100, 4
        ) if len(df) else 0,
        "candidate_key_columns": candidate_keys,
        "observations": file_flags,
        "columns": columns,
    }


# ============================================================
# Main
# ============================================================

def output_name_for(file_path: Path, base_dir: Path) -> str:
    """Build a collision-free output filename from the relative path."""

    relative_path = file_path.relative_to(base_dir)

    name = (
        str(relative_path.with_suffix(""))
        .replace("\\", "__")
        .replace("/", "__")
        + "_profile.json"
    )

    return re.sub(r'[<>:"|?*]', "_", name)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile CSV files and write one JSON report per file.",
    )
    parser.add_argument(
        "--input", type=Path, default=DEFAULT_INPUT,
        help="CSV file, or folder searched recursively for CSV files.",
    )
    parser.add_argument(
        "--output", type=Path, default=DEFAULT_OUTPUT,
        help="Folder where the JSON profiles are written.",
    )
    parser.add_argument(
        "--dataset-name", default=DEFAULT_DATASET_NAME,
        help="Label stored in every profile.",
    )
    parser.add_argument(
        "--null-tokens", nargs="*", default=list(DEFAULT_NULL_TOKENS),
        help="Text values treated as missing (case-insensitive).",
    )
    parser.add_argument(
        "--delimiter", default=",",
        help="CSV field separator.",
    )

    return parser.parse_args()


def main() -> int:
    args = parse_args()

    input_path: Path = args.input.resolve()
    output_dir: Path = args.output

    if input_path.is_file():
        files = [input_path]
        base_dir = input_path.parent
    elif input_path.is_dir():
        files = sorted(
            path for path in input_path.rglob("*")
            if path.is_file() and path.suffix.lower() == ".csv"
        )
        base_dir = input_path
    else:
        raise FileNotFoundError(f"Input not found: {input_path}")

    if not files:
        raise FileNotFoundError(f"No CSV files found in {input_path}")

    null_tokens = frozenset(
        token.strip().casefold() for token in args.null_tokens
    ) - {""}

    output_dir.mkdir(parents=True, exist_ok=True)

    print(f"Found {len(files)} CSV files.")

    failed: list[Path] = []

    for file_path in files:
        # One unreadable file should not stop the whole run.
        try:
            profile = profile_file(
                file_path,
                base_dir,
                args.dataset_name,
                null_tokens,
                args.delimiter,
            )
        except Exception as error:  # noqa: BLE001
            print(f"FAILED: {file_path} ({type(error).__name__}: {error})")
            failed.append(file_path)
            continue

        output_path = output_dir / output_name_for(file_path, base_dir)

        with output_path.open("w", encoding="utf-8") as output_file:
            json.dump(
                profile,
                output_file,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        print(f"Created: {output_path}")

    if failed:
        print(f"\nCompleted with {len(failed)} failed file(s).")
        return 1

    print("\nData profiling completed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
