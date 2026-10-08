import json
from pathlib import Path

from app.snowflake.loader import (
    TRIP_COLUMNS,
    SnowflakeLoader,
    build_copy_trips,
    parse_copy_result,
    positional_select,
    stage_path,
)


def test_positional_select_survives_reordered_and_missing_columns():
    header = ["trip_id", "start_time", "duration"]  # reordered, most columns missing
    sel = positional_select(header, ["trip_id", "duration", "start_time", "bike_id"])
    assert sel == ["$1", "$3", "$2", "NULL"]


def test_copy_sql_carries_lineage():
    sql = build_copy_trips(TRIP_COLUMNS, "trips/year=2025/quarter=3/batch_id=b1/",
                           "metro-trips-2025-q3.csv", year=2025, quarter=3,
                           batch_id="b1", sha256="abc")
    assert "COPY INTO RAW.TRIPS" in sql
    assert "@RAW.LANDING_STAGE/trips/year=2025/quarter=3/batch_id=b1/" in sql
    assert "METADATA$FILE_ROW_NUMBER" in sql and "'abc'" in sql
    assert r"metro-trips-2025-q3\\.csv(\\.gz)?" in sql
    assert "ON_ERROR = 'CONTINUE'" in sql


def test_parse_copy_result_sums_files():
    desc = [("file",), ("status",), ("rows_parsed",), ("rows_loaded",), ("error_limit",),
            ("errors_seen",), ("first_error",)]
    rows = [("a.csv.gz", "PARTIALLY_LOADED", 10, 9, 10, 1, "Numeric value 'x' is not recognized")]
    t = parse_copy_result(rows, desc)
    assert (t["rows_loaded"], t["errors_seen"], t["status"]) == (9, 1, "PARTIALLY_LOADED")


class FakeCursor:
    def __init__(self, log):
        self.log, self.description, self._rows = log, [], []

    def execute(self, sql, params=None):
        self.log.append(sql)
        if sql.startswith("COPY"):
            self.description = [("file",), ("status",), ("rows_parsed",), ("rows_loaded",),
                                ("errors_seen",), ("first_error",)]
            self._rows = [("f", "LOADED", 1, 1, 0, None)]
        else:
            self._rows = []

    def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self):
        self.log = []

    def cursor(self):
        return FakeCursor(self.log)


def test_load_manifest_puts_copies_and_audits(tmp_path):
    batch = tmp_path / "trips" / "year=2026" / "quarter=1" / "batch_id=b1"
    batch.mkdir(parents=True)
    (batch / "metro-trips-2026-q1.csv").write_text("x")
    (batch / "_manifest.json").write_text(json.dumps({
        "dataset": "trips", "batch_id": "b1", "partition": {"year": 2026, "quarter": 1},
        "files": [{"file_name": "metro-trips-2026-q1.csv", "header": TRIP_COLUMNS,
                   "row_count": 1, "sha256": "abc"}]}))
    conn = FakeConn()
    loader = SnowflakeLoader(conn, tmp_path)
    assert loader.pending_manifests("trips") == [batch / "_manifest.json"]
    [res] = loader.load_manifest(batch / "_manifest.json")
    assert res.status == "LOADED" and res.rows_loaded == 1
    kinds = [s.split()[0] for s in conn.log]
    assert kinds[-3:] == ["PUT", "COPY", "INSERT"]
    assert stage_path(tmp_path, batch / "x.csv") == "trips/year=2026/quarter=1/batch_id=b1/"
