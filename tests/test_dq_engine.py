import pandas as pd

from dq_service import validate

RULES = {
    "not_null": {"id": "not_null", "rule_type": "function", "rule": "not_null"},
    "unique": {"id": "unique", "rule_type": "function", "rule": "unique"},
    "in_range": {"id": "in_range", "rule_type": "function", "rule": "in_range"},
    "positive": {"id": "positive", "rule_type": "expression", "rule": "{column} > 0"},
    "end_after_start": {"id": "end_after_start", "rule_type": "expression",
                        "rule": "end_time >= start_time"},
}


def _a(column, rule_id, severity, **params):
    return {"id": f"t.{column}.{rule_id}", "column_name": column, "rule_id": rule_id,
            "severity": severity, "params": params}


def _df():
    return pd.DataFrame({
        "trip_id": pd.array([1, 2, 2, None, 5], dtype="Int64"),
        "duration": [10, 0, 5, 7, 2000],
        "start_time": pd.to_datetime(["2024-01-01 10:00"] * 5),
        "end_time": pd.to_datetime(["2024-01-01 10:10", "2024-01-01 10:00",
                                    "2024-01-01 09:00", "2024-01-01 10:07", "2024-01-02 20:00"]),
    })


def test_drop_rows_are_quarantined_with_reasons():
    result = validate(_df(), [_a("trip_id", "not_null", "drop"),
                              _a("trip_id", "unique", "drop")], RULES)
    assert result.valid["trip_id"].tolist() == [1, 5]
    assert result.quarantine["_failed_rules"].tolist() == [
        "t.trip_id.unique", "t.trip_id.unique", "t.trip_id.not_null"]
    assert result.outcome == "FAIL"


def test_warn_rows_are_kept_and_counted():
    result = validate(_df(), [_a("duration", "in_range", "warn", min=1, max=1440)], RULES)
    assert len(result.valid) == 5
    assert result.warn_count == 2
    assert result.outcome == "WARN"
    assert result.rule_results[0].failed_count == 2


def test_expression_rules():
    result = validate(_df(), [_a("duration", "positive", "drop"),
                              _a("end_time", "end_after_start", "drop")], RULES)
    assert result.valid.index.tolist() == [0, 3, 4]


def test_clean_data_passes():
    df = _df().iloc[[0]]
    result = validate(df, [_a("trip_id", "not_null", "drop")], RULES)
    assert result.outcome == "PASS" and len(result.quarantine) == 0
