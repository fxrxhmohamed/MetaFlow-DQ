"""Function rules. Each takes the column and the assignment params and returns a
boolean Series that is True where the row passes.

Nulls pass every check except not_null, so one missing value is reported once
(by not_null) instead of by every rule on the column.
"""
import pandas as pd


def not_null(s: pd.Series) -> pd.Series:
    return s.notna()


def unique(s: pd.Series) -> pd.Series:
    # keep=False flags every copy: we can't tell which one is right.
    return ~s.duplicated(keep=False) | s.isna()


def in_range(s: pd.Series, min=None, max=None) -> pd.Series:
    ok = pd.Series(True, index=s.index)
    if min is not None:
        ok &= s >= min
    if max is not None:
        ok &= s <= max
    return ok.fillna(False).astype(bool) | s.isna()


def in_set(s: pd.Series, values) -> pd.Series:
    return s.isin(values) | s.isna()


def matches_regex(s: pd.Series, pattern: str) -> pd.Series:
    return s.astype("string").str.fullmatch(pattern).fillna(False).astype(bool) | s.isna()


CHECKS = {
    "not_null": not_null,
    "unique": unique,
    "in_range": in_range,
    "in_set": in_set,
    "matches_regex": matches_regex,
}
