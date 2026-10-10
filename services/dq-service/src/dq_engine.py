"""Data Quality Engine.

Reads the schema registry and quality rules, validates a CSV batch,
remediates what the rules allow, and splits the records into:

    accepted    clean, standardised records (warnings flagged per row)
    quarantine  records with an unresolved critical failure, with their
                original values and the failed rule ids

Every repair attempt is written to a remediation audit (original value,
cleaned value, rule id, action, outcome) and a run summary is written to
dq_result.json.

Usage
-----
    # Banking defaults (input path comes from the schema registry)
    python src/dq_engine.py

    # Another file, rules or output folder
    python src/dq_engine.py --input data/raw/banking/extract.csv \
        --rules config/rules/quality_rules.yaml --output-root data
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from checks import (
    condition_mask,
    detect,
    functional_dependency,
    null_token_mask,
    null_tokens,
    whitespace_mask,
)
from registry import ConfigError, Registry, Rule, load_registry
from remediation import remediate
from values import canonical_text, parse_values


# ============================================================
# Defaults
# ============================================================

def _default_project_root() -> Path:
    """Project root is 3 levels above this file; fall back to the cwd."""

    parents = Path(__file__).resolve().parents
    return parents[3] if len(parents) > 3 else Path.cwd()


PROJECT_ROOT = _default_project_root()

DEFAULT_RULES = PROJECT_ROOT / "config" / "rules" / "quality_rules.yaml"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "data"

AUDIT_COLUMNS = [
    "source_row", "record_key", "column", "original_value",
    "cleaned_value", "rule_id", "action", "outcome",
]


# ============================================================
# Batch state
# ============================================================

class Batch:
    """The records being validated, with their working (cleaned) values.

    raw     original text from the file (never modified)
    data    working text, updated by remediation
    active  False for rows removed as exact duplicates
    """

    def __init__(self, raw: pd.DataFrame, registry: Registry) -> None:
        self.raw = raw
        self.data = raw.copy()
        self.registry = registry
        self.active = pd.Series(True, index=raw.index)
        self.failures: list[tuple[Rule, pd.Series]] = []
        self.audit: list[pd.DataFrame] = []
        self._typed: dict[str, pd.Series] = {}

        key_column = (registry.dataset.get("primary_key") or [None])[0]
        self.record_keys = (
            raw[key_column] if key_column in raw.columns
            else pd.Series(pd.NA, index=raw.index, dtype="string")
        )

    def spec(self, column: str) -> dict[str, Any]:
        return self.registry.columns.get(column, {"type": "string"})

    def typed(self, column: str) -> pd.Series:
        """Typed view of a column (cached until the column changes)."""

        if column not in self._typed:
            self._typed[column] = parse_values(
                self.data[column], self.spec(column),
            )
        return self._typed[column]

    def write(self, column: str, new_values: pd.Series) -> None:
        self.data.loc[new_values.index, column] = new_values
        self._typed.pop(column, None)

    def record_audit(
        self,
        rule: Rule,
        column: str,
        index: pd.Index,
        original: pd.Series,
        action: str,
        outcome: pd.Series | str,
    ) -> None:
        if not len(index):
            return

        self.audit.append(pd.DataFrame({
            "source_row": index + 1,
            "record_key": self.record_keys[index].to_numpy(),
            "column": column,
            "original_value": original[index].to_numpy(),
            "cleaned_value": self.data.loc[index, column].to_numpy()
            if column in self.data.columns else pd.NA,
            "rule_id": rule.id,
            "action": action,
            "outcome": outcome.to_numpy()
            if isinstance(outcome, pd.Series) else outcome,
        }))

    def fail(self, rule: Rule, failing: pd.Series) -> None:
        if failing.any():
            self.failures.append((rule, failing))


# ============================================================
# Rule execution
# ============================================================

def new_stats(rule: Rule) -> dict[str, Any]:
    return {
        **rule.describe(),
        "checked": 0,
        "detected": 0,
        "remediated": 0,
        "unresolved": 0,
    }


def run_value_rule(
    batch: Batch,
    rule: Rule,
    stats: dict[str, Any],
    column: str | None = None,
) -> None:
    """Detect -> remediate -> re-check for one rule on one column."""

    column = column or rule.column
    scope = batch.active & condition_mask(batch, rule.when)

    if rule.type == "trim_whitespace":
        failing = scope & whitespace_mask(batch.data[column])
    elif rule.type == "null_tokens":
        failing = scope & null_token_mask(batch.data[column], null_tokens(batch))
    else:
        failing = detect(batch, rule, scope)

    stats["checked"] += int(scope.sum())
    stats["detected"] += int(failing.sum())

    if rule.remediation and failing.any():
        # Usually the rule's own column; derived rules may repair their
        # source column instead.
        target = rule.remediation.get("target", column)
        original = batch.data[target].copy()
        bound = Rule(**{**rule.__dict__, "column": column})
        new_values, changed = remediate(batch, bound, failing)
        batch.write(target, new_values[changed])

        if rule.type in ("trim_whitespace", "null_tokens"):
            still = failing & ~failing.index.isin(changed[changed].index)
        else:
            still = detect(batch, rule, scope)

        attempted = failing[failing].index
        outcome = pd.Series("remediated", index=attempted)
        outcome[still[attempted]] = "not_fixed"
        batch.record_audit(
            rule, target, attempted, original,
            rule.remediation["action"], outcome,
        )
        failing = still

    stats["unresolved"] += int(failing.sum())
    stats["remediated"] = stats["detected"] - stats["unresolved"]

    batch.fail(rule, failing)


def run_duplicate_rows(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    """Exact copies of an earlier row (on the original text)."""

    duplicated = batch.active & batch.raw.duplicated(keep="first")
    stats["checked"] = int(batch.active.sum())
    stats["detected"] = int(duplicated.sum())

    if (rule.remediation or {}).get("action") == "drop_duplicates":
        index = duplicated[duplicated].index
        batch.active &= ~duplicated
        empty = pd.Series(pd.NA, index=batch.raw.index, dtype="string")
        batch.record_audit(
            rule, "*", index, empty, "drop_duplicates", "removed",
        )
        stats["remediated"] = stats["detected"]
        return

    stats["unresolved"] = stats["detected"]
    batch.fail(rule, duplicated)


def run_dependency(batch: Batch, rule: Rule, stats: dict[str, Any]) -> None:
    scope = batch.active & condition_mask(batch, rule.when)
    failing, per_column = functional_dependency(batch, rule, scope)

    stats["checked"] = int(scope.sum())
    stats["detected"] = stats["unresolved"] = int(failing.sum())
    stats["conflicts_by_column"] = {
        column: count for column, count in per_column.items() if count
    }
    batch.fail(rule, failing)


def run_rule(batch: Batch, rule: Rule) -> dict[str, Any]:
    stats = new_stats(rule)

    if rule.type == "duplicate_rows":
        run_duplicate_rows(batch, rule, stats)

    elif rule.type == "functional_dependency":
        run_dependency(batch, rule, stats)

    elif rule.type in ("trim_whitespace", "null_tokens"):
        columns = (
            list(batch.data.columns) if rule.columns in ([], ["*"])
            else rule.columns
        )
        for column in columns:
            run_value_rule(batch, rule, stats, column)

    else:
        run_value_rule(batch, rule, stats)

    return stats


# ============================================================
# Accept / quarantine
# ============================================================

def rule_id_lists(
    failures: list[tuple[Rule, pd.Series]],
    index: pd.Index,
) -> pd.Series:
    """';'-joined rule ids per row."""

    joined = pd.Series("", index=index, dtype=object)

    for rule, mask in failures:
        joined = joined.where(~mask, joined + rule.id + ";")

    return joined.str.rstrip(";")


def split_records(batch: Batch) -> dict[str, Any]:
    """Decide accepted vs quarantined rows and build both outputs."""

    actions = batch.registry.severity_actions
    index = batch.data.index

    quarantine_failures = [
        (rule, mask) for rule, mask in batch.failures
        if actions[rule.severity] == "quarantine"
    ]
    flag_failures = [
        (rule, mask) for rule, mask in batch.failures
        if actions[rule.severity] == "accept_with_flag"
    ]

    quarantined = pd.Series(False, index=index)
    for _, mask in quarantine_failures:
        quarantined |= mask
    quarantined &= batch.active
    accepted = batch.active & ~quarantined

    # Values still invalid in accepted rows are nulled so the accepted
    # data always loads with its declared types.
    for rule, mask in batch.failures:
        if rule.on_accept != "set_null" or not rule.column:
            continue
        target = mask & accepted & batch.data[rule.column].notna()
        if target.any():
            original = batch.data[rule.column].copy()
            batch.write(
                rule.column,
                pd.Series(pd.NA, index=target[target].index, dtype="string"),
            )
            batch.record_audit(
                rule, rule.column, target[target].index, original,
                "set_null", "nullified_in_accepted",
            )

    warnings_text = rule_id_lists(flag_failures, index)
    errors_text = rule_id_lists(quarantine_failures, index)
    formats = batch.registry.dataset.get("canonical_formats", {})

    clean = batch.data.copy()
    for column in clean.columns:
        if column not in batch.registry.columns:
            continue
        text = canonical_text(batch.typed(column), batch.spec(column), formats)
        if text is not None:
            clean[column] = text

    accepted_frame = clean[accepted].copy()
    accepted_frame.insert(0, "dq_source_row", accepted[accepted].index + 1)
    accepted_frame["dq_warnings"] = warnings_text[accepted]

    quarantine_frame = batch.raw[quarantined].copy()
    quarantine_frame.insert(0, "dq_source_row", quarantined[quarantined].index + 1)
    quarantine_frame["dq_errors"] = errors_text[quarantined]
    quarantine_frame["dq_warnings"] = warnings_text[quarantined]

    return {
        "accepted": accepted_frame,
        "quarantine": quarantine_frame,
        "accepted_mask": accepted,
        "quarantined_mask": quarantined,
        "flagged_mask": accepted & warnings_text.ne(""),
    }


# ============================================================
# Results
# ============================================================

def summarize(
    batch: Batch,
    split: dict[str, Any],
    rule_stats: list[dict[str, Any]],
    audit: pd.DataFrame,
    run: dict[str, Any],
) -> dict[str, Any]:
    input_rows = len(batch.raw)
    processed = int(batch.active.sum())
    accepted = int(split["accepted_mask"].sum())
    quarantined = int(split["quarantined_mask"].sum())

    def rate(part: int, whole: int) -> float:
        return round(part / whole * 100, 4) if whole else 0.0

    dimensions: dict[str, dict[str, int]] = {}
    for stats in rule_stats:
        bucket = dimensions.setdefault(stats["dimension"], {
            "rules": 0, "detected": 0, "remediated": 0, "unresolved": 0,
        })
        bucket["rules"] += 1
        for key in ("detected", "remediated", "unresolved"):
            bucket[key] += stats[key]

    accepted_mask = split["accepted_mask"]
    columns = {
        column: {
            "missing_in_input": int(batch.raw[column].isna().sum()),
            "missing_in_accepted": int(
                batch.data.loc[accepted_mask, column].isna().sum()
            ),
        }
        for column in batch.raw.columns
    }

    outcomes = (
        audit.groupby(["action", "outcome"]).size()
        .rename("count").reset_index().to_dict("records")
        if len(audit) else []
    )

    return {
        **run,
        "counts": {
            "input_rows": input_rows,
            "duplicate_rows_removed": input_rows - processed,
            "processed_rows": processed,
            "accepted_rows": accepted,
            "accepted_with_warnings_rows": int(split["flagged_mask"].sum()),
            "quarantined_rows": quarantined,
            "acceptance_rate": rate(accepted, processed),
            "quarantine_rate": rate(quarantined, processed),
            "remediated_values": int(
                (audit["outcome"] == "remediated").sum()
            ) if len(audit) else 0,
        },
        "dimensions": dimensions,
        "remediation_outcomes": outcomes,
        "rules": rule_stats,
        "columns": columns,
    }


# ============================================================
# Main
# ============================================================

def read_source(path: Path, dataset: dict[str, Any]) -> pd.DataFrame:
    """Read every column as text; empty cells become null."""

    source = dataset.get("source", {})
    frame = pd.read_csv(
        path,
        dtype="string",
        keep_default_na=False,
        sep=source.get("delimiter", ","),
        encoding=source.get("encoding", "utf-8-sig"),
    )
    return frame.mask(frame.eq(""))


def check_columns(frame: pd.DataFrame, registry: Registry) -> dict[str, list]:
    """Schema drift: missing columns stop the run, extra ones are kept."""

    expected = list(registry.columns)
    missing = [c for c in expected if c not in frame.columns]
    extra = [c for c in frame.columns if c not in registry.columns]

    if missing:
        raise ConfigError(f"Input is missing schema columns: {missing}")

    return {"missing_columns": missing, "extra_columns": extra}


def write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate a CSV batch against the schema registry "
        "and quality rules; split accepted and quarantined records.",
    )
    parser.add_argument(
        "--input", type=Path, default=None,
        help="CSV file to validate (default: source path in the schema).",
    )
    parser.add_argument(
        "--rules", type=Path, default=DEFAULT_RULES,
        help="Quality rules YAML.",
    )
    parser.add_argument(
        "--schema", type=Path, default=None,
        help="Schema registry YAML (default: the one named in the rules).",
    )
    parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT,
        help="Folder holding clean/, quarantine/ and quality/ outputs.",
    )
    parser.add_argument(
        "--run-id", default=None,
        help="Run identifier (default: UTC timestamp).",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    clock = time.perf_counter()

    registry = load_registry(args.rules, args.schema)
    dataset = registry.dataset
    name = dataset.get("name", "dataset")
    run_id = args.run_id or started.strftime("%Y%m%dT%H%M%SZ")

    input_path = args.input or PROJECT_ROOT / dataset["source"]["path"]
    print(f"Run {run_id}: {input_path}")
    print(f"Loaded {len(registry.all_rules())} rules.")

    raw = read_source(input_path, dataset)
    drift = check_columns(raw, registry)
    if drift["extra_columns"]:
        print(f"Columns not in the schema (kept, not validated): "
              f"{drift['extra_columns']}")

    batch = Batch(raw, registry)
    rule_stats = []

    for rule in registry.all_rules():
        step = time.perf_counter()
        stats = run_rule(batch, rule)
        rule_stats.append(stats)
        print(f"  {rule.id:<45} detected {stats['detected']:>8}  "
              f"unresolved {stats['unresolved']:>8}  "
              f"({time.perf_counter() - step:.1f}s)")

    split = split_records(batch)
    audit = (
        pd.concat(batch.audit, ignore_index=True)
        if batch.audit else pd.DataFrame(columns=AUDIT_COLUMNS)
    )

    root = args.output_root
    outputs = {
        "accepted": root / "clean" / name / run_id / "accepted.csv",
        "quarantine": root / "quarantine" / name / run_id / "quarantine.csv",
        "audit": root / "quality" / name / run_id / "remediation_audit.csv",
        "result": root / "quality" / name / run_id / "dq_result.json",
    }

    write_csv(split["accepted"], outputs["accepted"])
    write_csv(split["quarantine"], outputs["quarantine"])
    write_csv(audit, outputs["audit"])

    run = {
        "run_id": run_id,
        "dataset": name,
        "source_file": str(input_path),
        "started_at": started.isoformat(),
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.perf_counter() - clock, 2),
        "config": {
            "rules_file": str(args.rules),
            "schema_version": registry.schema.get("version"),
            "rules_version": registry.rules_config.get("version"),
            "rule_count": len(registry.all_rules()),
        },
        "schema_drift": drift,
        "outputs": {key: str(path) for key, path in outputs.items()},
    }
    result = summarize(batch, split, rule_stats, audit, run)

    with outputs["result"].open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, ensure_ascii=False, default=str)

    counts = result["counts"]
    print(
        f"\nAccepted {counts['accepted_rows']} "
        f"({counts['accepted_with_warnings_rows']} with warnings), "
        f"quarantined {counts['quarantined_rows']}, "
        f"duplicates removed {counts['duplicate_rows_removed']}."
    )
    for key, path in outputs.items():
        print(f"  {key:<11} {path}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ConfigError as error:
        print(f"Configuration error: {error}", file=sys.stderr)
        sys.exit(2)
