"""Load landed batches into Snowflake RAW.

Flow per batch (driven by the ingestion manifest, never by guessing):
    1. skip if AUDIT.LOAD_LOG already has this file's sha256 as LOADED
    2. PUT the CSV to @RAW.LANDING_STAGE/<same path as the landing zone>
    3. COPY INTO RAW.<table> selecting columns BY HEADER POSITION from the
       manifest, so a quarter that reorders or drops a column still lands in
       the right target columns (missing ones become NULL)
    4. write the COPY result to AUDIT.LOAD_LOG

The SQL builders are pure functions so they can be unit tested without Snowflake.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger("loading")

TRIP_COLUMNS = [
    "trip_id", "duration", "start_time", "end_time",
    "start_station", "start_lat", "start_lon",
    "end_station", "end_lat", "end_lon",
    "bike_id", "plan_duration", "trip_route_category", "passholder_type", "bike_type",
]
STATION_COLUMNS = ["kiosk_id", "kiosk_name", "go_live_date", "region", "status", "latitude", "longitude"]
STAGE = "@RAW.LANDING_STAGE"


def _q(value: str) -> str:
    """SQL string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "''") + "'"


def stage_path(landing_root: Path, file: Path) -> str:
    """Stage folder mirroring the landing layout, e.g. trips/year=2025/quarter=3/batch_id=x/"""
    return file.parent.relative_to(landing_root).as_posix() + "/"


def build_put(file: Path, stage_dir: str) -> str:
    return (f"PUT 'file://{file.resolve().as_posix()}' {STAGE}/{stage_dir} "
            "AUTO_COMPRESS=TRUE OVERWRITE=FALSE PARALLEL=4")


def positional_select(header: list[str], target_columns: list[str]) -> list[str]:
    """Map each target column to $<n> by its position in this file's header.
    Columns the file does not have become NULL (header drift is reported, not fatal)."""
    pos = {name: i + 1 for i, name in enumerate(header)}
    return [f"${pos[c]}" if c in pos else "NULL" for c in target_columns]


def build_copy_trips(header: list[str], stage_dir: str, file_name: str, *,
                     year: int, quarter: int, batch_id: str, sha256: str) -> str:
    select = positional_select(header, TRIP_COLUMNS)
    target = [c.upper() for c in TRIP_COLUMNS] + [
        "_YEAR", "_QUARTER", "_BATCH_ID", "_SOURCE_FILE", "_FILE_ROW_NUMBER", "_FILE_SHA256"]
    exprs = select + [str(int(year)), str(int(quarter)), _q(batch_id),
                      "METADATA$FILENAME", "METADATA$FILE_ROW_NUMBER", _q(sha256)]
    return _copy("RAW.TRIPS", target, exprs, stage_dir, file_name, "RAW.FF_CSV_TEXT")


def build_copy_stations(header: list[str], stage_dir: str, file_name: str, *,
                        snapshot_date: str, batch_id: str) -> str:
    select = positional_select(header, STATION_COLUMNS)
    target = [c.upper() for c in STATION_COLUMNS] + [
        "_SNAPSHOT_DATE", "_BATCH_ID", "_SOURCE_FILE", "_FILE_ROW_NUMBER"]
    exprs = select + [f"{_q(snapshot_date)}::DATE", _q(batch_id),
                      "METADATA$FILENAME", "METADATA$FILE_ROW_NUMBER"]
    return _copy("RAW.STATIONS", target, exprs, stage_dir, file_name, "RAW.FF_CSV_TEXT")


def build_copy_status(table: str, stage_dir: str, file_name: str) -> str:
    target = ["SNAPSHOT_TS", "SOURCE_URL", "PAYLOAD", "_SOURCE_FILE"]
    exprs = ["$1:snapshot_ts::TIMESTAMP_TZ", "$1:source_url::VARCHAR", "$1:payload",
             "METADATA$FILENAME"]
    return _copy(table, target, exprs, stage_dir, file_name, "RAW.FF_JSON")


def _copy(table, target, exprs, stage_dir, file_name, file_format) -> str:
    pattern = ".*" + file_name.replace(".", r"\\.") + r"(\\.gz)?"
    return (
        f"COPY INTO {table} ({', '.join(target)})\n"
        f"FROM (SELECT {', '.join(exprs)}\n"
        f"      FROM {STAGE}/{stage_dir})\n"
        f"PATTERN = '{pattern}'\n"
        f"FILE_FORMAT = (FORMAT_NAME = '{file_format}')\n"
        "ON_ERROR = 'CONTINUE'"
    )


@dataclass
class LoadResult:
    dataset: str
    batch_id: str
    source_file: str
    target_table: str
    rows_in_file: int | None
    rows_parsed: int = 0
    rows_loaded: int = 0
    errors_seen: int = 0
    first_error: str | None = None
    status: str = "SKIPPED"
    started_at: str = ""
    finished_at: str = ""


def parse_copy_result(cursor_rows: list[tuple], description: list) -> dict:
    """COPY returns one row per file: file, status, rows_parsed, rows_loaded, ..., first_error."""
    cols = [d[0].lower() for d in description]
    totals = {"rows_parsed": 0, "rows_loaded": 0, "errors_seen": 0, "first_error": None,
              "status": "SKIPPED"}
    for row in cursor_rows:
        r = dict(zip(cols, row))
        if "rows_parsed" not in r:  # "Copy executed with 0 files processed."
            continue
        totals["rows_parsed"] += int(r.get("rows_parsed") or 0)
        totals["rows_loaded"] += int(r.get("rows_loaded") or 0)
        totals["errors_seen"] += int(r.get("errors_seen") or 0)
        totals["first_error"] = totals["first_error"] or r.get("first_error")
        totals["status"] = r.get("status") or totals["status"]
    return totals


class SnowflakeLoader:
    def __init__(self, conn, landing_root: str | Path):
        self.conn = conn
        self.root = Path(landing_root)

    # -- discovery -----------------------------------------------------------
    def pending_manifests(self, dataset: str) -> list[Path]:
        manifests = sorted((self.root / dataset).rglob("_manifest.json"))
        loaded = self._loaded_hashes(dataset)
        out = []
        for m in manifests:
            data = json.loads(m.read_text())
            if data.get("skipped_as_duplicate_of"):
                continue
            if all(f["sha256"] in loaded for f in data["files"]):
                continue
            out.append(m)
        return out

    def _loaded_hashes(self, dataset: str) -> set[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT FILE_SHA256 FROM AUDIT.LOAD_LOG WHERE DATASET = %s "
                    "AND STATUS IN ('LOADED', 'PARTIALLY_LOADED')", (dataset,))
        return {r[0] for r in cur.fetchall() if r[0]}

    # -- loads ---------------------------------------------------------------
    def load_manifest(self, manifest_path: Path) -> list[LoadResult]:
        m = json.loads(manifest_path.read_text())
        results = []
        for f in m["files"]:
            file = manifest_path.parent / f["file_name"]
            sdir = stage_path(self.root, file)
            if m["dataset"] == "trips":
                table = "RAW.TRIPS"
                copy_sql = build_copy_trips(
                    f["header"], sdir, file.name, year=m["partition"]["year"],
                    quarter=m["partition"]["quarter"], batch_id=m["batch_id"], sha256=f["sha256"])
            elif m["dataset"] == "stations":
                table = "RAW.STATIONS"
                copy_sql = build_copy_stations(
                    f["header"], sdir, file.name,
                    snapshot_date=m["partition"]["snapshot_date"], batch_id=m["batch_id"])
            else:
                raise ValueError(f"unknown dataset {m['dataset']}")
            results.append(self._put_and_copy(m["dataset"], m["batch_id"], file, sdir, table,
                                              copy_sql, f["row_count"], f["sha256"]))
        return results

    def load_status_snapshot(self, path: Path, source: str = "geojson") -> LoadResult:
        table = "RAW.STATION_STATUS_GEOJSON" if source == "geojson" else "RAW.STATION_STATUS_GBFS"
        sdir = stage_path(self.root, path)
        return self._put_and_copy(f"station_status_{source}", path.stem, path, sdir, table,
                                  build_copy_status(table, sdir, path.name), 1, None)

    def _put_and_copy(self, dataset, batch_id, file, sdir, table, copy_sql, rows_in_file, sha):
        res = LoadResult(dataset, batch_id, file.name, table, rows_in_file,
                         started_at=datetime.now(timezone.utc).isoformat())
        cur = self.conn.cursor()
        try:
            cur.execute(build_put(file, sdir))
            cur.execute(copy_sql)
            totals = parse_copy_result(cur.fetchall(), cur.description)
            res.rows_parsed, res.rows_loaded = totals["rows_parsed"], totals["rows_loaded"]
            res.errors_seen, res.first_error = totals["errors_seen"], totals["first_error"]
            res.status = totals["status"]
            if rows_in_file is not None and res.status == "LOADED" and res.rows_loaded != rows_in_file \
                    and dataset != "station_status_geojson":
                log.warning("%s: manifest says %s rows, Snowflake loaded %s",
                            file.name, rows_in_file, res.rows_loaded)
        except Exception as exc:  # noqa: BLE001 - always audit, then re-raise
            res.status, res.first_error = "LOAD_FAILED", str(exc)[:1000]
            raise
        finally:
            res.finished_at = datetime.now(timezone.utc).isoformat()
            self._audit(res, sha)
        return res

    def _audit(self, r: LoadResult, sha: str | None) -> None:
        self.conn.cursor().execute(
            "INSERT INTO AUDIT.LOAD_LOG (DATASET, BATCH_ID, SOURCE_FILE, FILE_SHA256, TARGET_TABLE, "
            "ROWS_IN_FILE, ROWS_PARSED, ROWS_LOADED, ERRORS_SEEN, FIRST_ERROR, STATUS, STARTED_AT, "
            "FINISHED_AT) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (r.dataset, r.batch_id, r.source_file, sha, r.target_table, r.rows_in_file,
             r.rows_parsed, r.rows_loaded, r.errors_seen, r.first_error, r.status,
             r.started_at, r.finished_at),
        )
