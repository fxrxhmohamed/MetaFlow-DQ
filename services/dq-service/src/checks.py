"""Rule detection: which rows break a rule.

Every check returns a boolean mask over the batch (True = the row fails)
and only ever flags rows inside the given scope (active rows that match
the rule's "when" condition).
"""

from __future__ import annotations

import operator
from typing import TYPE_CHECKING, Any

import pandas as pd

from values import as_flags, format_number, resolve_bound

if TYPE_CHECKING:
    from dq_engine import Batch
    from registry import Rule


OPERATORS = {
    ">=": operator.ge,
    ">": operator.gt,
    "<=": operator.le,
    "<": operator.lt,
    "==": operator.eq,
    "!=": operator.ne,
}


# ============================================================
# Conditions ("when")
# ============================================================

def condition_mask(batch: Batch, condition: dict[str, Any] | None) -> pd.Series:
    """Rows matching a rule's applicability condition."""

    index = batch.data.index

    if not condition:
        return pd.Series(True, index=index)

    if "all" in condition:
        mask = pd.Series(True, index=index)
        for part in condition["all"]:
            mask &= condition_mask(batch, part)
        return mask

    if "any" in condition:
        mask = pd.Series(False, index=index)
        for part in condition["any"]:
            mask |= condition_mask(batch, part)
        return mask

    if "not" in condition:
        return ~condition_mask(batch, condition["not"])

    values = batch.data[condition["column"]]

    if "in" in condition:
        return as_flags(values.isin([str(v) for v in condition["in"]]))
    if "not_in" in condition:
        return values.notna() & ~as_flags(
            values.isin([str(v) for v in condition["not_in"]])
        )
    if "equals" in condition:
        return as_flags(values.eq(str(condition["equals"])))
    if "is_null" in condition:
        return values.isna() if condition["is_null"] else values.notna()
    if "not_null" in condition:
        return values.notna() if condition["not_null"] else values.isna()

    raise ValueError(f"Unsupported condition: {condition}")


# ============================================================
# Row-level checks
# ============================================================

def _range_failures(
    typed: pd.Series,
    params: dict[str, Any],
    column_type: str,
) -> pd.Series:
    low = resolve_bound(params.get("min"), column_type)
    high = resolve_bound(params.get("max"), column_type)
    out = pd.Series(False, index=typed.index)

    if low is not None:
        out |= (
            typed.le(low) if params.get("exclusive_min") else typed.lt(low)
        )
    if high is not None:
        out |= (
            typed.ge(high) if params.get("exclusive_max") else typed.gt(high)
        )

    return as_flags(out) & typed.notna()


def _offset(params: dict[str, Any]) -> pd.DateOffset | None:
    offset = params.get("offset")
    if not offset:
        return None
    return pd.DateOffset(**{k: int(v) for k, v in offset.items()})


def detect(batch: Batch, rule: Rule, scope: pd.Series) -> pd.Series:
    """Rows in scope that break a single-column or cross-column rule."""

    kind = rule.type
    column = rule.column
    values = batch.data[column] if column else None

    if kind == "not_null":
        failing = values.isna()

    elif kind == "is_null":
        failing = values.notna()

    elif kind == "type":
        failing = values.notna() & batch.typed(column).isna()

    elif kind == "pattern":
        matches = as_flags(values.str.fullmatch(rule.params["pattern"]))
        failing = values.notna() & ~matches

    elif kind in ("allowed_values", "reference"):
        # reference: values loaded from the lookup file at config load.
        allowed = [str(v) for v in rule.params["values"]]
        failing = values.notna() & ~as_flags(values.isin(allowed))

    elif kind == "expression":
        # e.g. 'abs(amount_egp - transaction_amount) <= 0.01'; rows with a
        # missing input are not judged.
        frame = pd.DataFrame({c: batch.typed(c) for c in rule.columns})
        holds = frame.eval(rule.params["expression"], engine="python")
        failing = frame.notna().all(axis=1) & ~as_flags(
            pd.Series(holds, index=frame.index).astype("boolean")
        )

    elif kind == "range":
        failing = _range_failures(
            batch.typed(column),
            rule.params,
            batch.spec(column).get("type", "decimal"),
        )

    elif kind == "unique":
        present = scope & values.notna()
        failing = pd.Series(False, index=values.index)
        failing[present] = values[present].duplicated(keep=False)

    elif kind == "compare":
        left = batch.typed(rule.params["left"])
        right = batch.typed(rule.params["right"])
        offset = _offset(rule.params)
        if offset is not None:
            right = right + offset
        holds = OPERATORS[rule.params["operator"]](left, right)
        failing = left.notna() & right.notna() & ~as_flags(holds)

    elif kind == "derived":
        expected = derived_values(batch, rule)
        actual = batch.typed(column).dt.strftime(rule.params["format"])
        failing = expected.notna() & (
            actual.isna() | as_flags(actual.ne(expected))
        )

    else:
        raise ValueError(f"detect() does not handle {kind}")

    return scope & as_flags(failing)


def derived_values(batch: Batch, rule: Rule) -> pd.Series:
    """Expected value of a derived column, from its source column."""

    source = batch.typed(rule.params["source"])
    return source.dt.strftime(rule.params["format"]).astype("string")


def generated_values(batch: Batch, rule: Rule) -> pd.Series:
    """Values of a generated column, computed from cleaned columns.

    convert_currency  amount x rate of its currency, e.g. 1000 USD at
                      52.40 -> '52400'
    case              value of the first case whose condition matches,
                      else the default (NA without one)

    NA where the value cannot be computed (unknown currency, no match).
    """

    method = rule.params["method"]

    if method == "convert_currency":
        amount = batch.typed(rule.params["amount"])
        rates = {
            str(code): float(rate)
            for code, rate in rule.params["rates"].items()
        }
        rate = batch.data[rule.params["currency"]].map(rates)
        converted = (
            amount * pd.to_numeric(rate.astype(object), errors="coerce")
        ).round(int(rule.params.get("decimals", 2)))
        return converted.map(format_number, na_action="ignore").astype("string")

    if method == "case":
        result = pd.Series(pd.NA, index=batch.data.index, dtype="string")
        for case in rule.params["cases"]:
            hit = result.isna() & condition_mask(batch, case.get("when"))
            result[hit] = str(case["value"])
        if "default" in rule.params:
            result = result.fillna(str(rule.params["default"]))
        return result

    raise ValueError(f"Rule {rule.id}: unknown generate method {method}")


# ============================================================
# Preprocessing checks (one mask per column)
# ============================================================

def whitespace_mask(values: pd.Series) -> pd.Series:
    return values.notna() & as_flags(values.ne(values.str.strip()))


def null_token_mask(values: pd.Series, tokens: set[str]) -> pd.Series:
    folded = values.str.strip().str.casefold()
    return values.notna() & as_flags(folded.isin(tokens))


# ============================================================
# Relationships
# ============================================================

def dependency_columns(batch: Batch, rule: Rule) -> tuple[str, list[str]]:
    """Determinant and dependent columns of a functional dependency."""

    schema = batch.registry.schema

    if "entity" in rule.params:
        entity = schema["entities"][rule.params["entity"]]
        dependents = list(entity.get("attributes", []))
        dependents += list((entity.get("references") or {}).values())
        return entity["key"], dependents

    dependency = schema["dependencies"][rule.params["dependency"]]
    return dependency["determinant"], list(dependency["dependents"])


def functional_dependency(
    batch: Batch,
    rule: Rule,
    scope: pd.Series,
) -> tuple[pd.Series, dict[str, int]]:
    """Rows whose dependent value differs from the key's majority value.

    E.g. a customer_id that shows 'Male' on 20 rows and 'Female' on one:
    the one row fails. Returns the failing rows and counts per column.
    """

    determinant, dependents = dependency_columns(batch, rule)
    keys = batch.data[determinant]
    base = scope & keys.notna()

    failing = pd.Series(False, index=keys.index)
    per_column: dict[str, int] = {}

    key_codes = pd.Series(pd.factorize(keys[base])[0], index=keys[base].index)

    for column in dependents:
        values = batch.data.loc[base, column]
        present = values.notna()

        if not present.any():
            continue

        value_codes = pd.factorize(values[present])[0]
        pairs = pd.DataFrame({
            "key": key_codes[present].to_numpy(),
            "value": value_codes,
        })

        # value_counts sorts by frequency, so the first row per key is
        # its majority value.
        majority = (
            pairs.value_counts()
            .reset_index()
            .drop_duplicates("key")
            .set_index("key")["value"]
        )
        expected = majority.reindex(pairs["key"]).to_numpy()
        bad = value_codes != expected

        per_column[column] = int(bad.sum())
        failing[values[present].index[bad]] = True

    return failing, per_column


def null_tokens(batch: Batch) -> set[str]:
    """Null placeholders from the schema registry, compared casefolded."""

    tokens = batch.registry.dataset.get("null_tokens", [])
    return {str(t).strip().casefold() for t in tokens} - {""}
