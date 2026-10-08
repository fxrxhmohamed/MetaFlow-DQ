import requests
from fastapi import APIRouter

from ..config import LANDING_ROOT
from ..models.ingestion import FileReport, IngestResult, TripIngestRequest
from ..services.ingestion import IngestionError, IngestionService
from ..services.sources import (
    DATA_PAGE_URL,
    discover_trip_quarters,
    quarters_for,
)

router = APIRouter(prefix="/ingest", tags=["ingestion"])
service = IngestionService(LANDING_ROOT)


def _plan(req: TripIngestRequest):
    years = set(req.years)
    plan = {t.key: t for t in quarters_for(years)}
    if req.discover:
        try:
            html = requests.get(DATA_PAGE_URL, timeout=30).text
            plan.update({t.key: t for t in discover_trip_quarters(html, years)})
        except requests.RequestException:
            pass  # fall back to the pinned catalog
    items = sorted(plan.values(), key=lambda t: (t.year, t.quarter))
    if req.quarters:
        items = [t for t in items if t.quarter in req.quarters]
    return items


@router.post("/trips", response_model=list[IngestResult])
def ingest_trips(req: TripIngestRequest) -> list[IngestResult]:
    results = []
    for tq in _plan(req):
        try:
            m = service.ingest_trip_quarter(tq, force=req.force)
        except IngestionError as exc:
            results.append(IngestResult(dataset="trips", key=tq.key, status="failed", error=str(exc)))
            continue
        files = [FileReport(**{k: f[k] for k in FileReport.model_fields}) for f in m.files]
        results.append(IngestResult(
            dataset="trips", key=tq.key, batch_id=m.batch_id,
            status="unchanged" if m.skipped_as_duplicate_of else "landed",
            rows=sum(f.row_count for f in files), files=files,
        ))
    return results


@router.post("/stations", response_model=IngestResult)
def ingest_stations() -> IngestResult:
    m = service.ingest_station_table()
    files = [FileReport(**{k: f[k] for k in FileReport.model_fields}) for f in m.files]
    return IngestResult(dataset="stations", key=m.partition["snapshot_date"], batch_id=m.batch_id,
                        status="landed", rows=sum(f.row_count for f in files), files=files)


@router.post("/station-status")
def snapshot_status(source: str = "geojson") -> dict:
    return service.snapshot_station_status(source)
