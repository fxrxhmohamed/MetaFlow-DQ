"""Command-line backfill, same code path as the API (useful from Airflow BashOperator)."""
import argparse
import json
import logging

from .config import LANDING_ROOT
from .services.ingestion import IngestionService
from .services.sources import quarters_for


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    t = sub.add_parser("trips")
    t.add_argument("--years", type=int, nargs="+", default=[2024, 2025, 2026])
    t.add_argument("--force", action="store_true")
    sub.add_parser("stations")
    s = sub.add_parser("status")
    s.add_argument("--source", choices=["geojson", "gbfs"], default="geojson")
    args = p.parse_args()

    svc = IngestionService(LANDING_ROOT)
    if args.cmd == "trips":
        for tq in quarters_for(set(args.years)):
            m = svc.ingest_trip_quarter(tq, force=args.force)
            print(json.dumps({"quarter": tq.key, "batch_id": m.batch_id,
                              "rows": sum(f["row_count"] for f in m.files),
                              "duplicate_of": m.skipped_as_duplicate_of}))
    elif args.cmd == "stations":
        print(json.dumps(svc.ingest_station_table().files, indent=2))
    else:
        print(json.dumps(svc.snapshot_station_status(args.source)))


if __name__ == "__main__":
    main()
