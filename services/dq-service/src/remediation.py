"""Remediation actions: try to repair values that failed a rule.

Each action receives the failing rows and returns repaired values for
the target column (the rule's column unless the remediation names a
"target") plus a mask of the values it actually changed.
Values it cannot repair are left untouched; the engine re-checks the
rule afterwards and records the outcome in the audit trail.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pandas as pd

from checks import derived_values
from values import (
    ISO_DATE_FORMAT,
    as_flags,
    format_number,
    normalize_key,
    normalize_one,
    parse_temporal,
    recover_numbers,
    repair_times,
)

if TYPE_CHECKING:
    from dq_engine import Batch
    from registry import Rule


def remediate(
    batch: Batch,
    rule: Rule,
    failing: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Repaired values (for the failing rows) and which ones changed."""

    options = rule.remediation or {}
    action = options["action"]
    values = batch.data.loc[failing, rule.column]

    if action == "trim":
        stripped = values.str.strip()
        new = stripped.mask(as_flags(stripped.eq("")))
        return new, as_flags(values.ne(new)) | new.isna()

    if action == "set_null":
        if "values" in options:
            changed = _matches_any(batch, rule.column, values, options["values"])
        else:
            changed = pd.Series(True, index=values.index)
        return pd.Series(pd.NA, index=values.index, dtype="string"), changed

    if action == "canonicalize":
        allowed = rule.params.get("values") or batch.spec(rule.column).get(
            "allowed_values", [],
        )
        lookup = {normalize_one(v): str(v) for v in allowed}
        lookup.update({
            normalize_one(k): str(v)
            for k, v in (options.get("map") or {}).items()
        })
        new = normalize_key(values).map(lookup).astype("string")
        return new, new.notna() & as_flags(new.ne(values))

    if action == "parse_number":
        numbers = recover_numbers(values)
        new = numbers.map(format_number, na_action="ignore").astype("string")
        return new, numbers.notna()

    if action == "parse_date":
        dates = parse_temporal(values, list(options.get("formats", [])))
        new = dates.dt.strftime(ISO_DATE_FORMAT).astype("string")
        return new, dates.notna()

    if action == "parse_time":
        new = repair_times(values)
        return new, new.notna()

    if action == "reformat_id":
        new = pd.Series(pd.NA, index=values.index, dtype="string")
        for pattern in options.get("patterns", []):
            groups = values.str.extract(pattern, flags=re.IGNORECASE)[0]
            found = groups.notna() & new.isna()
            new[found] = groups[found].map(options["template"].format)
        return new, new.notna()

    if action == "derive":
        new = derived_values(batch, rule)[failing]
        return new, new.notna()

    if action == "swap_day_month":
        return _swap_day_month(batch, rule, failing)

    raise ValueError(f"Rule {rule.id}: unsupported remediation {action}")


def _swap_day_month(
    batch: Batch,
    rule: Rule,
    failing: pd.Series,
) -> tuple[pd.Series, pd.Series]:
    """Re-read the source date day-first when that agrees with the
    derived column.

    '1/3/2025' read as 3 January disagrees with month '2025-03'; read as
    1 March it agrees, so the date becomes 2025-03-01.
    """

    dates = batch.typed(rule.params["source"])[failing]
    swapped = pd.to_datetime(
        pd.DataFrame({
            "year": dates.dt.year,
            "month": dates.dt.day,
            "day": dates.dt.month,
        }),
        errors="coerce",
    )
    recorded = batch.typed(rule.column)[failing].dt.strftime(
        rule.params["format"],
    )
    changed = (
        swapped.notna()
        & as_flags(swapped.dt.strftime(rule.params["format"]).eq(recorded))
    )
    new = swapped.dt.strftime(ISO_DATE_FORMAT).astype("string")

    return new, changed


def _matches_any(
    batch: Batch,
    column: str,
    values: pd.Series,
    targets: list,
) -> pd.Series:
    """Values equal to any target (numerically for numeric columns)."""

    if batch.spec(column).get("type") in ("integer", "decimal"):
        numbers = pd.to_numeric(values.astype(object), errors="coerce")
        return as_flags(numbers.isin([float(t) for t in targets]))

    return as_flags(
        normalize_key(values).isin([normalize_one(t) for t in targets])
    )
