"""CLI: python -m app.profiler {trips|stations|status} <path> [--stations <csv|dir>] [--out file]"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .stations import profile_station_status, profile_station_table
from .trips import profile_trips


def _csvs(path: Path) -> list[Path]:
    return sorted(path.rglob("*.csv")) if path.is_dir() else [path]


def _latest_csv(path: Path | None) -> Path | None:
    if path is None:
        return None
    files = _csvs(path)
    return files[-1] if files else None


def jsonable(obj):
    """Stringify dict keys and unwrap numpy/pandas scalars so json.dumps never chokes."""
    if isinstance(obj, dict):
        return {str(k): jsonable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [jsonable(v) for v in obj]
    if hasattr(obj, "item") and not isinstance(obj, (str, bytes)):
        try:
            return obj.item()
        except (ValueError, AttributeError):
            return str(obj)
    return obj


def print_summary(report: dict) -> None:
    print(f"rows: {report['rows']:,}")
    for f in sorted(report["findings"], key=lambda f: ("error", "warning", "info").index(f["severity"])):
        if f["count"]:
            print(f"  [{f['severity']:<7}] {f['check']:<34} {f['count']:>9,}  ({f['pct']}%)  {f['detail']}")


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="python -m app.profiler")
    p.add_argument("kind", choices=["trips", "stations", "status"])
    p.add_argument("path", type=Path)
    p.add_argument("--stations", type=Path, help="station table CSV or landing folder")
    p.add_argument("--out", type=Path)
    a = p.parse_args(argv)

    if a.kind == "trips":
        report = profile_trips(_csvs(a.path), _latest_csv(a.stations))
    elif a.kind == "stations":
        report = profile_station_table(_latest_csv(a.path))
    else:
        files = sorted(a.path.rglob("*.json")) if a.path.is_dir() else [a.path]
        report = profile_station_status(files, _latest_csv(a.stations))

    print_summary(report)
    if a.out:
        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(jsonable(report), indent=2, default=str))
        print(f"full report: {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
