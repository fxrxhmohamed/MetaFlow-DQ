"""Batch ingestion: download Metro Bike Share files into the landing zone.

Landing layout (immutable, one folder per batch so re-runs never overwrite):

    {landing}/trips/year=2025/quarter=3/batch_id=<id>/metro-trips-2025-q3.csv
    {landing}/trips/year=2025/quarter=3/batch_id=<id>/_manifest.json
    {landing}/stations/snapshot_date=2026-07-15/batch_id=<id>/stations.csv
    {landing}/station_status/source=geojson/dt=2026-10-08/<ts>.json

Nothing is parsed or typed here: bronze keeps the file exactly as published,
and the manifest records what we got (checksum, header, row count, encoding)
so the profiler, DQ engine and loader can all trust the same facts.
"""
from __future__ import annotations

import csv
import hashlib
import json
import logging
import shutil
import tempfile
import time
import uuid
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import requests

from .sources import (
    GBFS_STATION_STATUS_URL,
    STATION_STATUS_GEOJSON_URL,
    STATION_TABLE_URL,
    TripQuarter,
)

log = logging.getLogger("ingestion")

EXPECTED_TRIP_COLUMNS = [
    "trip_id", "duration", "start_time", "end_time",
    "start_station", "start_lat", "start_lon",
    "end_station", "end_lat", "end_lon",
    "bike_id", "plan_duration", "trip_route_category", "passholder_type", "bike_type",
]
EXPECTED_STATION_COLUMNS = [
    "kiosk_id", "kiosk_name", "go_live_date", "region", "status", "latitude", "longitude",
]

USER_AGENT = "metaflow-dq-ingestion/0.1 (+https://github.com/fxrxhmohamed/metaflow-dq)"
CHUNK = 1 << 20


class IngestionError(RuntimeError):
    pass


@dataclass
class Manifest:
    dataset: str
    batch_id: str
    source_url: str
    downloaded_at: str
    archive_sha256: str
    archive_bytes: int
    files: list[dict] = field(default_factory=list)
    partition: dict = field(default_factory=dict)
    skipped_as_duplicate_of: str | None = None

    def write(self, folder: Path) -> Path:
        path = folder / "_manifest.json"
        path.write_text(json.dumps(asdict(self), indent=2))
        return path


def normalize_header(name: str) -> str:
    """'Kiosk ID' -> 'kiosk_id', 'Region ' -> 'region', strips a UTF-8 BOM."""
    return "_".join(name.replace("﻿", "").strip().lower().split())


def new_batch_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]


def download(url: str, dest: Path, retries: int = 4, timeout: int = 60) -> tuple[str, int]:
    """Stream url to dest; return (sha256, bytes). Retries with exponential backoff."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")
    for attempt in range(retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout,
                              headers={"User-Agent": USER_AGENT}) as r:
                r.raise_for_status()
                h, n = hashlib.sha256(), 0
                with tmp.open("wb") as fh:
                    for chunk in r.iter_content(CHUNK):
                        fh.write(chunk)
                        h.update(chunk)
                        n += len(chunk)
            tmp.replace(dest)
            return h.hexdigest(), n
        except requests.RequestException as exc:
            if attempt == retries:
                raise IngestionError(f"download failed for {url}: {exc}") from exc
            wait = 2 ** (attempt + 1)
            log.warning("download %s failed (%s), retrying in %ss", url, exc, wait)
            time.sleep(wait)
    raise AssertionError("unreachable")


def detect_encoding(sample: bytes) -> str:
    if sample.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        sample.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        # Station names carry en dashes; older exports were cp1252.
        return "cp1252"


def describe_csv(path: Path, expected: list[str]) -> dict:
    """Header, row count and drift against the expected columns, without loading the file."""
    with path.open("rb") as fh:
        encoding = detect_encoding(fh.read(64 * 1024))
    with path.open("r", encoding=encoding, newline="") as fh:
        reader = csv.reader(fh)
        raw_header = next(reader, [])
        rows = 0
        ragged = 0
        for row in reader:
            if not any(cell.strip() for cell in row):
                continue
            rows += 1
            if len(row) != len(raw_header):
                ragged += 1
    header = [normalize_header(c) for c in raw_header]
    return {
        "file_name": path.name,
        "encoding": encoding,
        "raw_header": raw_header,
        "header": header,
        "row_count": rows,
        "ragged_rows": ragged,
        "missing_columns": [c for c in expected if c not in header],
        "unexpected_columns": [c for c in header if c not in expected],
        "column_order_matches": header == expected,
        "sha256": _sha256_file(path),
    }


def safe_extract_csvs(archive: Path, out_dir: Path) -> list[Path]:
    """Extract only .csv members, flattening paths (blocks zip-slip and __MACOSX junk)."""
    extracted: list[Path] = []
    with zipfile.ZipFile(archive) as zf:
        for info in zf.infolist():
            name = PurePosixPath(info.filename)
            if info.is_dir() or "__MACOSX" in name.parts or name.name.startswith("._"):
                continue
            if name.suffix.lower() != ".csv":
                continue
            target = out_dir / name.name
            with zf.open(info) as src, target.open("wb") as dst:
                shutil.copyfileobj(src, dst, CHUNK)
            extracted.append(target)
    if not extracted:
        raise IngestionError(f"{archive.name} contains no CSV file")
    return extracted


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK), b""):
            h.update(chunk)
    return h.hexdigest()


def _previous_batch_with_sha(partition_dir: Path, sha: str) -> str | None:
    for manifest in partition_dir.glob("batch_id=*/_manifest.json"):
        data = json.loads(manifest.read_text())
        if data.get("archive_sha256") == sha and not data.get("skipped_as_duplicate_of"):
            return data["batch_id"]
    return None


class IngestionService:
    def __init__(self, landing_root: str | Path):
        self.root = Path(landing_root)

    # -- trips -------------------------------------------------------------
    def ingest_trip_quarter(self, tq: TripQuarter, force: bool = False) -> Manifest:
        partition = self.root / "trips" / f"year={tq.year}" / f"quarter={tq.quarter}"
        batch_id = new_batch_id()
        batch_dir = partition / f"batch_id={batch_id}"
        batch_dir.mkdir(parents=True, exist_ok=True)

        archive = batch_dir / PurePosixPath(tq.url).name
        sha, size = download(tq.url, archive)
        manifest = Manifest(
            dataset="trips", batch_id=batch_id, source_url=tq.url,
            downloaded_at=datetime.now(timezone.utc).isoformat(),
            archive_sha256=sha, archive_bytes=size,
            partition={"year": tq.year, "quarter": tq.quarter},
        )

        previous = None if force else _previous_batch_with_sha(partition, sha)
        if previous:
            # Same bytes as an earlier batch: keep the manifest as an audit trail,
            # drop the payload so the loader does not load it twice.
            archive.unlink()
            manifest.skipped_as_duplicate_of = previous
            manifest.write(batch_dir)
            log.info("%s unchanged since batch %s, skipping", tq.key, previous)
            return manifest

        for csv_path in safe_extract_csvs(archive, batch_dir):
            info = describe_csv(csv_path, EXPECTED_TRIP_COLUMNS)
            if info["missing_columns"]:
                log.warning("%s: missing columns %s", csv_path.name, info["missing_columns"])
            manifest.files.append(info)
        archive.unlink()  # the extracted CSV + sha in the manifest is the bronze copy
        manifest.write(batch_dir)
        log.info("landed %s: %s rows", tq.key, sum(f["row_count"] for f in manifest.files))
        return manifest

    # -- station table -------------------------------------------------------
    def ingest_station_table(self, url: str = STATION_TABLE_URL) -> Manifest:
        snapshot = _date_from_station_url(url) or datetime.now(timezone.utc).date().isoformat()
        batch_id = new_batch_id()
        batch_dir = self.root / "stations" / f"snapshot_date={snapshot}" / f"batch_id={batch_id}"
        batch_dir.mkdir(parents=True, exist_ok=True)
        target = batch_dir / "stations.csv"
        sha, size = download(url, target)
        manifest = Manifest(
            dataset="stations", batch_id=batch_id, source_url=url,
            downloaded_at=datetime.now(timezone.utc).isoformat(),
            archive_sha256=sha, archive_bytes=size, partition={"snapshot_date": snapshot},
        )
        manifest.files.append(describe_csv(target, EXPECTED_STATION_COLUMNS))
        manifest.write(batch_dir)
        return manifest

    # -- live station status -------------------------------------------------
    def snapshot_station_status(self, source: str = "geojson") -> dict:
        """Poll one live feed and store the raw JSON. Run on a schedule (e.g. every 5 min):
        the feed is a point-in-time snapshot and history only exists if we keep it."""
        url = STATION_STATUS_GEOJSON_URL if source == "geojson" else GBFS_STATION_STATUS_URL
        now = datetime.now(timezone.utc)
        resp = requests.get(url, timeout=30, headers={"User-Agent": USER_AGENT})
        resp.raise_for_status()
        payload = resp.json()
        folder = self.root / "station_status" / f"source={source}" / f"dt={now.date().isoformat()}"
        folder.mkdir(parents=True, exist_ok=True)
        path = folder / f"{now.strftime('%Y%m%dT%H%M%SZ')}.json"
        path.write_text(json.dumps({"snapshot_ts": now.isoformat(), "source_url": url,
                                    "payload": payload}))
        records = flatten_geojson_status(payload, now) if source == "geojson" else \
            flatten_gbfs_status(payload, now)
        return {"path": str(path), "snapshot_ts": now.isoformat(), "stations": len(records)}


def _date_from_station_url(url: str) -> str | None:
    stem = PurePosixPath(url).stem  # metro-bike-share-stations-2026-07-15
    tail = stem.rsplit("-", 3)[-3:]
    try:
        return datetime.strptime("-".join(tail), "%Y-%m-%d").date().isoformat()
    except ValueError:
        return None


def flatten_geojson_status(payload: dict, snapshot_ts: datetime) -> list[dict]:
    rows = []
    for feat in payload.get("features", []):
        p = dict(feat.get("properties") or {})
        coords = (feat.get("geometry") or {}).get("coordinates") or [None, None]
        p["geom_lon"], p["geom_lat"] = coords[0], coords[1]
        p["snapshot_ts"] = snapshot_ts.isoformat()
        rows.append(p)
    return rows


def flatten_gbfs_status(payload: dict, snapshot_ts: datetime) -> list[dict]:
    rows = []
    for s in payload.get("data", {}).get("stations", []):
        types = s.get("num_bikes_available_types") or {}
        rows.append({
            # GBFS ids are 'bcycle_lametro_3005'; trips and the station table use 3005.
            "kiosk_id": int(str(s["station_id"]).rsplit("_", 1)[-1]),
            "num_bikes_available": s.get("num_bikes_available"),
            "num_docks_available": s.get("num_docks_available"),
            "classic": types.get("classic"), "smart": types.get("smart"),
            "electric": types.get("electric"),
            "is_installed": s.get("is_installed"), "is_renting": s.get("is_renting"),
            "is_returning": s.get("is_returning"),
            "last_reported": s.get("last_reported"),
            "snapshot_ts": snapshot_ts.isoformat(),
        })
    return rows


def describe_bytes_as_csv(data: bytes, expected: list[str]) -> dict:
    """Describe an uploaded CSV (API upload path) the same way as a downloaded one."""
    with tempfile.TemporaryDirectory() as d:
        p = Path(d) / "upload.csv"
        p.write_bytes(data)
        return describe_csv(p, expected)
