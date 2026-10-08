"""Parse and validate the YAML config into one list of records per control table.

config/rules/*.yaml      -> dq_rules
config/pipelines/*.yaml  -> bronze_control, silver_control, dq_rules_assignment, gold_control
"""
from pathlib import Path

import yaml

# Columns each control table takes from config (SCD2 bookkeeping columns excluded).
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "bronze_control": ("id", "source_system", "source_type", "source_location",
                       "bronze_schema", "bronze_table", "options"),
    "silver_control": ("id", "bronze_id", "silver_schema", "silver_table", "quarantine_table",
                       "load_type", "business_keys", "watermark_column", "schema_file"),
    "dq_rules": ("id", "rule_type", "rule", "description"),
    "dq_rules_assignment": ("id", "silver_id", "column_name", "rule_id", "severity", "params"),
    "gold_control": ("id", "dbt_model", "depends_on"),
}
OPTIONAL_DEFAULTS = {
    "options": {}, "business_keys": [], "watermark_column": None,
    "description": None, "params": {}, "depends_on": [],
}
ENUMS = {
    "source_type": {"csv", "kafka"},
    "load_type": {"append", "scd1", "scd2"},
    "rule_type": {"function", "expression"},
    "severity": {"drop", "warn"},
}


class ConfigError(ValueError):
    pass


def _record(table: str, raw: dict, source: str) -> dict:
    record = {}
    for col in TABLE_COLUMNS[table]:
        if col in raw:
            record[col] = raw[col]
        elif col in OPTIONAL_DEFAULTS:
            record[col] = OPTIONAL_DEFAULTS[col]
        else:
            raise ConfigError(f"{source}: {table} entry {raw.get('id', raw)!r} is missing '{col}'")
        if col in ENUMS and record[col] not in ENUMS[col]:
            raise ConfigError(
                f"{source}: {table}.{col}={record[col]!r} must be one of {sorted(ENUMS[col])}")
    record["config_file_name"] = source
    return record


def load_config_dir(config_dir: str | Path) -> dict[str, list[dict]]:
    config_dir = Path(config_dir)
    tables: dict[str, list[dict]] = {t: [] for t in TABLE_COLUMNS}

    for path in sorted((config_dir / "rules").glob("*.yaml")):
        source = path.relative_to(config_dir.parent).as_posix()
        for raw in (yaml.safe_load(path.read_text()) or {}).get("rules", []):
            tables["dq_rules"].append(_record("dq_rules", {**raw, "id": raw.get("rule_id")}, source))

    for path in sorted((config_dir / "pipelines").glob("*.yaml")):
        source = path.relative_to(config_dir.parent).as_posix()
        doc = yaml.safe_load(path.read_text()) or {}
        for raw in doc.get("bronze", []):
            tables["bronze_control"].append(_record("bronze_control", raw, source))
        for raw in doc.get("silver", []):
            tables["silver_control"].append(_record("silver_control", raw, source))
            for check in raw.get("dq", []):
                assignment = {
                    "id": f"{raw['id']}.{check['column']}.{check['rule_id']}",
                    "silver_id": raw["id"],
                    "column_name": check["column"],
                    **check,
                }
                tables["dq_rules_assignment"].append(
                    _record("dq_rules_assignment", assignment, source))
        for raw in doc.get("gold", []):
            tables["gold_control"].append(_record("gold_control", raw, source))

    _check_references(tables)
    return tables


def _check_references(tables: dict[str, list[dict]]) -> None:
    ids = {}
    for table, records in tables.items():
        seen = set()
        for r in records:
            if r["id"] in seen:
                raise ConfigError(f"duplicate {table} id {r['id']!r} ({r['config_file_name']})")
            seen.add(r["id"])
        ids[table] = seen

    for r in tables["silver_control"]:
        if r["bronze_id"] not in ids["bronze_control"]:
            raise ConfigError(f"silver {r['id']!r} references unknown bronze {r['bronze_id']!r}")
        if r["load_type"] in ("scd1", "scd2") and not r["business_keys"]:
            raise ConfigError(f"silver {r['id']!r}: load_type {r['load_type']} needs business_keys")
    for r in tables["dq_rules_assignment"]:
        if r["rule_id"] not in ids["dq_rules"]:
            raise ConfigError(f"assignment {r['id']!r} references unknown rule {r['rule_id']!r}")
    for r in tables["gold_control"]:
        missing = set(r["depends_on"]) - ids["silver_control"]
        if missing:
            raise ConfigError(f"gold {r['id']!r} depends on unknown silver {sorted(missing)}")
