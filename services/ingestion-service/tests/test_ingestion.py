import io
import json
import zipfile
from pathlib import Path

import pytest

from app.services import ingestion
from app.services.ingestion import (
    EXPECTED_TRIP_COLUMNS,
    IngestionService,
    describe_csv,
    flatten_gbfs_status,
    normalize_header,
    safe_extract_csvs,
)
from app.services.sources import TripQuarter, discover_station_table_url, discover_trip_quarters

TRIPS_CSV = (
    ",".join(EXPECTED_TRIP_COLUMNS) + "\n"
    "538837390,5,01/01/2026 0:00,01/01/2026 0:05,4606,34.168629,-118.377068,4601,34.161709,"
    "-118.372818,15524,30,One Way,Monthly Pass,standard\n"
)


def _zip(members: dict[str, str]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, body in members.items():
            zf.writestr(name, body)
    return buf.getvalue()


def test_normalize_header():
    assert normalize_header("Region ") == "region"
    assert normalize_header("﻿Kiosk ID") == "kiosk_id"


def test_discover_from_page_html():
    html = '''
      <a href="https://bikeshare.metro.net/wp-content/uploads/2026/07/metro-trips-2026-q2.zip">Q2</a>
      <a href="https://bikeshare.metro.net/wp-content/uploads/2021/04/metro-trips-2021-q1-1.zip">x</a>
      <a href="https://bikeshare.metro.net/wp-content/uploads/2020/01/metro-bike-share-trips-2019-q4.csv.zip">y</a>
      <a href="https://bikeshare.metro.net/wp-content/uploads/2026/07/metro-bike-share-stations-2026-07-15.csv">s</a>
    '''
    found = discover_trip_quarters(html)
    assert [t.key for t in found] == ["2019-q4", "2021-q1", "2026-q2"]
    assert [t.key for t in discover_trip_quarters(html, {2026})] == ["2026-q2"]
    assert discover_station_table_url(html).endswith("stations-2026-07-15.csv")


def test_safe_extract_ignores_junk_and_traversal(tmp_path):
    archive = tmp_path / "a.zip"
    archive.write_bytes(_zip({
        "../../evil.csv": "x\n",
        "__MACOSX/._metro-trips-2026-q1.csv": "junk",
        "readme.txt": "hi",
        "nested/metro-trips-2026-q1.csv": TRIPS_CSV,
    }))
    out = tmp_path / "out"
    out.mkdir()
    files = safe_extract_csvs(archive, out)
    assert sorted(p.name for p in files) == ["evil.csv", "metro-trips-2026-q1.csv"]
    assert all(p.parent == out for p in files)  # flattened, nothing written outside out/


def test_describe_csv_reports_header_drift(tmp_path):
    p = tmp_path / "t.csv"
    p.write_text(TRIPS_CSV.replace("bike_type", "bike_category"))
    info = describe_csv(p, EXPECTED_TRIP_COLUMNS)
    assert info["row_count"] == 1
    assert info["missing_columns"] == ["bike_type"]
    assert info["unexpected_columns"] == ["bike_category"]


def test_ingest_quarter_is_idempotent(tmp_path, monkeypatch):
    payload = _zip({"metro-trips-2026-q1.csv": TRIPS_CSV})

    def fake_download(url, dest, **_):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(payload)
        import hashlib
        return hashlib.sha256(payload).hexdigest(), len(payload)

    monkeypatch.setattr(ingestion, "download", fake_download)
    svc = IngestionService(tmp_path)
    tq = TripQuarter(2026, 1, "https://example.test/metro-trips-2026-q1.zip")
    first = svc.ingest_trip_quarter(tq)
    second = svc.ingest_trip_quarter(tq)
    assert first.files[0]["row_count"] == 1
    assert second.skipped_as_duplicate_of == first.batch_id
    landed = list((tmp_path / "trips" / "year=2026" / "quarter=1").rglob("*.csv"))
    assert len(landed) == 1
    manifest = json.loads((Path(landed[0]).parent / "_manifest.json").read_text())
    assert manifest["partition"] == {"year": 2026, "quarter": 1}


def test_flatten_gbfs_strips_prefix():
    from datetime import datetime, timezone
    payload = {"data": {"stations": [{
        "station_id": "bcycle_lametro_3005", "num_bikes_available": 3, "num_docks_available": 23,
        "num_bikes_available_types": {"classic": 3, "smart": 0, "electric": 0},
        "is_installed": 1, "is_renting": 1, "is_returning": 1, "last_reported": 1791486077}]}}
    rows = flatten_gbfs_status(payload, datetime.now(timezone.utc))
    assert rows[0]["kiosk_id"] == 3005


def test_zip_without_csv_fails(tmp_path):
    archive = tmp_path / "a.zip"
    archive.write_bytes(_zip({"readme.txt": "x"}))
    with pytest.raises(ingestion.IngestionError):
        safe_extract_csvs(archive, tmp_path)
