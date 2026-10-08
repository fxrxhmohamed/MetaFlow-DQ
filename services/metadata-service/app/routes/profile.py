from pathlib import Path

from fastapi import APIRouter, HTTPException

from ..config import LANDING_ROOT, REPORTS_ROOT
from ..profiler.__main__ import jsonable
from ..profiler.stations import profile_station_status, profile_station_table
from ..profiler.trips import profile_trips

router = APIRouter(prefix="/profile", tags=["profiling"])


def _latest(folder: Path, pattern: str) -> Path | None:
    files = sorted(folder.rglob(pattern))
    return files[-1] if files else None


@router.post("/trips")
def trips(year: int | None = None, quarter: int | None = None) -> dict:
    root = LANDING_ROOT / "trips"
    if year:
        root = root / f"year={year}"
        if quarter:
            root = root / f"quarter={quarter}"
    files = sorted(root.rglob("*.csv"))
    if not files:
        raise HTTPException(404, f"no landed trip files under {root}")
    report = jsonable(profile_trips(files, _latest(LANDING_ROOT / "stations", "*.csv")))
    _save(report, f"trips_{year or 'all'}_{quarter or 'all'}.json")
    return report


@router.post("/stations")
def stations() -> dict:
    path = _latest(LANDING_ROOT / "stations", "*.csv")
    if path is None:
        raise HTTPException(404, "no landed station table")
    report = jsonable(profile_station_table(path))
    _save(report, "stations.json")
    return report


@router.post("/station-status")
def station_status() -> dict:
    files = sorted((LANDING_ROOT / "station_status" / "source=geojson").rglob("*.json"))
    if not files:
        raise HTTPException(404, "no station status snapshots")
    report = jsonable(profile_station_status(files, _latest(LANDING_ROOT / "stations", "*.csv")))
    _save(report, "station_status.json")
    return report


def _save(report: dict, name: str) -> None:
    import json
    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    (REPORTS_ROOT / name).write_text(json.dumps(report, indent=2, default=str))
