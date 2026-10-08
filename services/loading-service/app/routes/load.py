from dataclasses import asdict
from typing import Literal

from fastapi import APIRouter

from ..config import LANDING_ROOT
from ..snowflake.client import snowflake_connection
from ..snowflake.loader import SnowflakeLoader

router = APIRouter(prefix="/load", tags=["loading"])


@router.post("/{dataset}")
def load_pending(dataset: Literal["trips", "stations"]) -> list[dict]:
    """Load every landed trips/stations batch not yet in AUDIT.LOAD_LOG."""
    with snowflake_connection() as conn:
        loader = SnowflakeLoader(conn, LANDING_ROOT)
        results = []
        for manifest in loader.pending_manifests(dataset):
            results.extend(asdict(r) for r in loader.load_manifest(manifest))
        return results


@router.post("/station-status/{source}")
def load_status(source: Literal["geojson", "gbfs"]) -> list[dict]:
    folder = LANDING_ROOT / "station_status" / f"source={source}"
    with snowflake_connection() as conn:
        loader = SnowflakeLoader(conn, LANDING_ROOT)
        cur = conn.cursor()
        cur.execute("SELECT SOURCE_FILE FROM AUDIT.LOAD_LOG WHERE DATASET = %s AND STATUS = 'LOADED'",
                    (f"station_status_{source}",))
        done = {r[0] for r in cur.fetchall()}
        return [asdict(loader.load_status_snapshot(p, source))
                for p in sorted(folder.rglob("*.json")) if p.name not in done]
