from pathlib import Path

import pytest
import yaml

from metadata_service.config import ConfigError, load_config_dir
from metadata_service.scd2 import plan_changes, record_hash

REPO_CONFIG = Path(__file__).resolve().parents[1] / "config"


def test_repo_config_is_valid():
    tables = load_config_dir(REPO_CONFIG)
    assert [r["id"] for r in tables["bronze_control"]] == ["metro_bike_trips_raw"]
    assert [r["id"] for r in tables["silver_control"]] == ["metro_bike_trips"]
    assert "metro_bike_trips.trip_id.not_null" in {r["id"] for r in tables["dq_rules_assignment"]}
    assert all(r["config_file_name"].startswith("config/") for rs in tables.values() for r in rs)


def _write(tmp_path, pipeline: dict, rules: list[dict]):
    (tmp_path / "config" / "rules").mkdir(parents=True)
    (tmp_path / "config" / "pipelines").mkdir()
    (tmp_path / "config" / "rules" / "r.yaml").write_text(yaml.safe_dump({"rules": rules}))
    (tmp_path / "config" / "pipelines" / "p.yaml").write_text(yaml.safe_dump(pipeline))
    return tmp_path / "config"


BRONZE = {"id": "b", "source_system": "s", "source_type": "csv", "source_location": "x.csv",
          "bronze_schema": "bronze", "bronze_table": "t"}
SILVER = {"id": "s", "bronze_id": "b", "silver_schema": "silver", "silver_table": "t",
          "quarantine_table": "t", "load_type": "append", "schema_file": "x.yaml"}
RULE = {"rule_id": "not_null", "rule_type": "function", "rule": "not_null"}


def test_unknown_rule_reference_rejected(tmp_path):
    silver = {**SILVER, "dq": [{"column": "a", "rule_id": "nope", "severity": "drop"}]}
    with pytest.raises(ConfigError, match="unknown rule"):
        load_config_dir(_write(tmp_path, {"bronze": [BRONZE], "silver": [silver]}, [RULE]))


def test_bad_enum_rejected(tmp_path):
    with pytest.raises(ConfigError, match="load_type"):
        load_config_dir(_write(tmp_path, {"bronze": [BRONZE],
                                          "silver": [{**SILVER, "load_type": "merge"}]}, [RULE]))


def test_scd1_needs_business_keys(tmp_path):
    with pytest.raises(ConfigError, match="business_keys"):
        load_config_dir(_write(tmp_path, {"bronze": [BRONZE],
                                          "silver": [{**SILVER, "load_type": "scd1"}]}, [RULE]))


def test_plan_changes_insert_update_expire():
    a, b, c = {"id": "a", "v": 1}, {"id": "b", "v": 2}, {"id": "c", "v": 3}
    active = {"a": record_hash(a), "b": "stale", "gone": "x"}
    changes = plan_changes(active, [a, b, c])
    assert changes.unchanged == 1
    assert [r["id"] for r in changes.inserts] == ["b", "c"]
    assert sorted(changes.expire_ids) == ["b", "gone"]
