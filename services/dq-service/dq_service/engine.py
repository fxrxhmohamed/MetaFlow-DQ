"""Run the rules assigned to a dataset against a DataFrame.

Severity decides what happens to a failing row:
  drop -> row goes to quarantine and is not loaded
  warn -> row is loaded, the failure is counted
"""
from dataclasses import dataclass, field

import pandas as pd

from .checks import CHECKS


@dataclass
class RuleResult:
    assignment_id: str
    rule_id: str
    column_name: str
    severity: str
    checked_count: int
    failed_count: int


@dataclass
class DQResult:
    valid: pd.DataFrame
    quarantine: pd.DataFrame          # dropped rows plus a _failed_rules column
    warn_count: int                   # loaded rows with at least one warn failure
    rule_results: list[RuleResult] = field(default_factory=list)

    @property
    def outcome(self) -> str:
        if len(self.quarantine):
            return "FAIL"
        if self.warn_count:
            return "WARN"
        return "PASS"


def _evaluate(df: pd.DataFrame, assignment: dict, rule: dict) -> pd.Series:
    column = assignment["column_name"]
    if column not in df.columns:
        raise ValueError(f"rule {assignment['id']!r}: column {column!r} not in data")
    params = assignment.get("params") or {}
    if rule["rule_type"] == "function":
        check = CHECKS.get(rule["rule"])
        if check is None:
            raise ValueError(f"rule {rule['id']!r}: unknown function {rule['rule']!r}")
        passed = check(df[column], **params)
    elif rule["rule_type"] == "expression":
        expr = rule["rule"].format(column=f"`{column}`", **params)
        passed = df.eval(expr)
    else:
        raise ValueError(f"rule {rule['id']!r}: unknown rule_type {rule['rule_type']!r}")
    return pd.Series(passed, index=df.index).fillna(False).astype(bool)


def validate(df: pd.DataFrame, assignments: list[dict], rules: dict[str, dict]) -> DQResult:
    """assignments: dq_rules_assignment rows; rules: dq_rules rows keyed by id."""
    drop_mask = pd.Series(False, index=df.index)
    warn_mask = pd.Series(False, index=df.index)
    failed_rules = pd.Series([[] for _ in range(len(df))], index=df.index, dtype=object)
    results = []

    for a in assignments:
        failed = ~_evaluate(df, a, rules[a["rule_id"]])
        if a["severity"] == "drop":
            drop_mask |= failed
        else:
            warn_mask |= failed
        for idx in failed[failed].index:
            failed_rules[idx].append(a["id"])
        results.append(RuleResult(a["id"], a["rule_id"], a["column_name"], a["severity"],
                                  checked_count=len(df), failed_count=int(failed.sum())))

    quarantine = df[drop_mask].copy()
    quarantine["_failed_rules"] = failed_rules[drop_mask].map(",".join)
    return DQResult(
        valid=df[~drop_mask].copy(),
        quarantine=quarantine,
        warn_count=int((warn_mask & ~drop_mask).sum()),
        rule_results=results,
    )
