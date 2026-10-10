"""Load the schema registry and quality rules into Rule objects.

Schema checks are generated from the schema registry (one rule per
column and check) and merged with the explicit rules. Configuration
errors are reported up front, before any data is read.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


SCHEMA_VALUE_CHECKS = ("type", "pattern", "allowed_values", "range")
SCHEMA_FINAL_CHECKS = ("not_null", "unique")

COLUMN_TYPES = {
    "string", "integer", "decimal", "boolean", "date", "time", "year_month",
}

RULE_TYPES = {
    "not_null", "is_null", "allowed_values", "pattern", "range", "type",
    "unique", "compare", "derived", "functional_dependency",
    "duplicate_rows", "trim_whitespace", "null_tokens",
}

REMEDIATION_ACTIONS = {
    "trim", "set_null", "canonicalize", "parse_number", "parse_date",
    "parse_time", "reformat_id", "derive", "swap_day_month",
    "drop_duplicates",
}

COMPARE_OPERATORS = {">=", ">", "<=", "<", "==", "!="}

# Keys that are not rule parameters.
RULE_KEYS = {
    "id", "name", "type", "column", "columns", "dimension", "severity",
    "when", "remediation", "on_accept", "enabled",
}


class ConfigError(ValueError):
    """The schema registry or rules file is invalid."""


@dataclass
class Rule:
    id: str
    type: str
    severity: str
    dimension: str
    name: str = ""
    column: str | None = None
    columns: list[str] = field(default_factory=list)
    params: dict[str, Any] = field(default_factory=dict)
    when: dict[str, Any] | None = None
    remediation: dict[str, Any] | None = None
    on_accept: str = "keep"
    origin: str = "rules"

    def describe(self) -> dict[str, Any]:
        """JSON-safe summary for results."""

        return {
            "rule_id": self.id,
            "name": self.name,
            "type": self.type,
            "column": self.column,
            "dimension": self.dimension,
            "severity": self.severity,
            "remediation": (self.remediation or {}).get("action"),
            "origin": self.origin,
        }


@dataclass
class Registry:
    schema: dict[str, Any]
    rules_config: dict[str, Any]
    severity_actions: dict[str, str]
    preprocessing: list[Rule]
    schema_rules: list[Rule]
    business_rules: list[Rule]

    @property
    def columns(self) -> dict[str, dict[str, Any]]:
        return self.schema["columns"]

    @property
    def dataset(self) -> dict[str, Any]:
        return self.schema.get("dataset", {})

    def all_rules(self) -> list[Rule]:
        return self.preprocessing + self.schema_rules + self.business_rules


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise ConfigError(f"File not found: {path}")

    with path.open(encoding="utf-8") as handle:
        content = yaml.safe_load(handle)

    if not isinstance(content, dict):
        raise ConfigError(f"{path} is empty or not a mapping.")

    return content


def _schema_rule_name(column: str, check: str, spec: dict[str, Any]) -> str:
    return {
        "type": f"{column} is a valid {spec.get('type', 'string')}",
        "pattern": f"{column} matches {spec.get('pattern')}",
        "allowed_values": f"{column} is one of the allowed values",
        "range": f"{column} is within its allowed range",
        "not_null": f"{column} is present",
        "unique": f"{column} is unique",
    }[check]


def _schema_check_applies(check: str, spec: dict[str, Any]) -> bool:
    if check == "type":
        return spec.get("type", "string") != "string"
    if check == "pattern":
        return "pattern" in spec
    if check == "allowed_values":
        return "allowed_values" in spec
    if check == "range":
        return "min" in spec or "max" in spec
    if check == "not_null":
        return spec.get("nullable") is False
    if check == "unique":
        return bool(spec.get("unique"))
    return False


def _build_schema_rules(
    schema: dict[str, Any],
    config: dict[str, Any],
) -> list[Rule]:
    """One rule per column and applicable check, in check order."""

    defaults = config.get("defaults", {})
    overrides = config.get("columns", {}) or {}
    on_accept = config.get("on_accept", "keep")

    unknown = set(overrides) - set(schema["columns"])
    if unknown:
        raise ConfigError(
            f"schema_checks.columns names unknown columns: {sorted(unknown)}"
        )

    value_rules: list[Rule] = []
    final_rules: list[Rule] = []

    for check in SCHEMA_VALUE_CHECKS + SCHEMA_FINAL_CHECKS:
        for column, spec in schema["columns"].items():
            if not _schema_check_applies(check, spec):
                continue

            settings = {
                **defaults.get(check, {}),
                **(overrides.get(column, {}) or {}).get(check, {}),
            }

            if settings.get("enabled", True) is False:
                continue

            params: dict[str, Any] = {}
            if check == "pattern":
                params["pattern"] = spec["pattern"]
            elif check == "allowed_values":
                params["values"] = [str(v) for v in spec["allowed_values"]]
            elif check == "range":
                for key in ("min", "max", "exclusive_min", "exclusive_max"):
                    if key in spec:
                        params[key] = spec[key]

            rule = Rule(
                id=f"SCH-{column}-{check}",
                type=check,
                column=column,
                severity=settings.get("severity", "critical"),
                dimension=settings.get("dimension", "validity"),
                name=_schema_rule_name(column, check, spec),
                params=params,
                remediation=settings.get("remediation"),
                on_accept=(
                    settings.get("on_accept", on_accept)
                    if check in SCHEMA_VALUE_CHECKS else "keep"
                ),
                origin="schema",
            )

            if check in SCHEMA_VALUE_CHECKS:
                value_rules.append(rule)
            else:
                final_rules.append(rule)

    # Per column: type -> pattern -> allowed_values -> range, so later
    # checks see the repaired value.
    order = {name: i for i, name in enumerate(schema["columns"])}
    value_rules.sort(key=lambda r: (
        order[r.column], SCHEMA_VALUE_CHECKS.index(r.type),
    ))

    return value_rules + final_rules


def _build_rule(entry: dict[str, Any], origin: str) -> Rule | None:
    if entry.get("enabled", True) is False:
        return None

    missing = [key for key in ("id", "type", "severity") if key not in entry]
    if missing:
        raise ConfigError(f"Rule {entry.get('id', entry)} is missing {missing}")

    columns = entry.get("columns", [])
    if columns == "all":
        columns = ["*"]

    return Rule(
        id=str(entry["id"]),
        type=entry["type"],
        severity=entry["severity"],
        dimension=entry.get("dimension", "validity"),
        name=entry.get("name", ""),
        column=entry.get("column"),
        columns=list(columns),
        params={k: v for k, v in entry.items() if k not in RULE_KEYS},
        when=entry.get("when"),
        remediation=entry.get("remediation"),
        on_accept=entry.get("on_accept", "keep"),
        origin=origin,
    )


def _condition_columns(condition: dict[str, Any] | None) -> set[str]:
    if not condition:
        return set()

    columns: set[str] = set()
    for key in ("all", "any"):
        for part in condition.get(key, []):
            columns |= _condition_columns(part)
    if "not" in condition:
        columns |= _condition_columns(condition["not"])
    if "column" in condition:
        columns.add(condition["column"])

    return columns


def _validate(registry: Registry) -> None:
    schema_columns = set(registry.columns)
    entities = registry.schema.get("entities", {})
    dependencies = registry.schema.get("dependencies", {})
    seen: set[str] = set()

    for column, spec in registry.columns.items():
        if spec.get("type", "string") not in COLUMN_TYPES:
            raise ConfigError(f"Column {column} has unknown type {spec['type']}")

    for rule in registry.all_rules():
        where = f"Rule {rule.id}"

        if rule.id in seen:
            raise ConfigError(f"{where}: duplicate rule id")
        seen.add(rule.id)

        if rule.type not in RULE_TYPES:
            raise ConfigError(f"{where}: unknown type {rule.type}")

        if rule.severity not in registry.severity_actions:
            raise ConfigError(f"{where}: unknown severity {rule.severity}")

        if rule.on_accept not in ("keep", "set_null"):
            raise ConfigError(f"{where}: on_accept must be keep or set_null")

        referenced = _condition_columns(rule.when)
        if rule.column:
            referenced.add(rule.column)
        for key in ("left", "right", "source"):
            if key in rule.params:
                referenced.add(rule.params[key])
        referenced |= {c for c in rule.columns if c != "*"}

        unknown = referenced - schema_columns
        if unknown:
            raise ConfigError(f"{where}: unknown columns {sorted(unknown)}")

        if rule.remediation:
            action = rule.remediation.get("action")
            if action not in REMEDIATION_ACTIONS:
                raise ConfigError(f"{where}: unknown remediation {action}")
            target = rule.remediation.get("target")
            if target and target not in schema_columns:
                raise ConfigError(f"{where}: unknown remediation target {target}")
            if action in ("derive", "swap_day_month") and rule.type != "derived":
                raise ConfigError(f"{where}: {action} needs a derived rule")

        if rule.type == "compare":
            if rule.params.get("operator") not in COMPARE_OPERATORS:
                raise ConfigError(f"{where}: invalid operator")

        if rule.type == "functional_dependency":
            entity = rule.params.get("entity")
            dependency = rule.params.get("dependency")
            if entity and entity not in entities:
                raise ConfigError(f"{where}: unknown entity {entity}")
            if dependency and dependency not in dependencies:
                raise ConfigError(f"{where}: unknown dependency {dependency}")
            if not (entity or dependency):
                raise ConfigError(f"{where}: needs entity or dependency")

        if rule.type == "derived" and "source" not in rule.params:
            raise ConfigError(f"{where}: derived rules need a source")


def load_registry(rules_path: Path, schema_path: Path | None = None) -> Registry:
    """Read both files, build the rules and validate them."""

    rules_config = _read_yaml(rules_path)

    if schema_path is None:
        if "schema" not in rules_config:
            raise ConfigError("No schema path given and none in the rules file.")
        schema_path = Path(rules_config["schema"])
        if not schema_path.is_absolute():
            # Relative to the project root (the rules file is in config/rules).
            schema_path = rules_path.resolve().parents[2] / schema_path

    schema = _read_yaml(schema_path)

    if not isinstance(schema.get("columns"), dict) or not schema["columns"]:
        raise ConfigError(f"{schema_path} defines no columns.")

    severity_actions = {
        name: level.get("action", "accept")
        for name, level in rules_config.get("severity_levels", {}).items()
    } or {
        "critical": "quarantine",
        "warning": "accept_with_flag",
        "info": "accept",
    }

    preprocessing = [
        rule for entry in rules_config.get("preprocessing", []) or []
        if (rule := _build_rule(entry, "preprocessing"))
    ]
    business = [
        rule for entry in rules_config.get("rules", []) or []
        if (rule := _build_rule(entry, "rules"))
    ]

    registry = Registry(
        schema=schema,
        rules_config=rules_config,
        severity_actions=severity_actions,
        preprocessing=preprocessing,
        schema_rules=_build_schema_rules(
            schema, rules_config.get("schema_checks", {}) or {},
        ),
        business_rules=business,
    )

    _validate(registry)

    return registry
