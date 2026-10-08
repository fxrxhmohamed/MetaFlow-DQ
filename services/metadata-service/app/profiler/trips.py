"""Profile Metro Bike Share quarterly trip files.

Reads every landed quarter as text, then runs the checks that matter for this
dataset. Each check returns a Finding; the ones marked with `rule=` map 1:1 to
rules in config/rules/quality_rules.yaml so profiling and DQ stay in sync.

    python -m app.profiler trips data/landing/trips --stations data/landing/stations \
        --out reports/trips_profile.json
"""
from __future__ import annotations

import re
from pathlib import Path

import numpy as np
import pandas as pd

from .core import TIMESTAMP_FORMATS, haversine_km, is_null, parse_with_formats, profile_frame
from .findings import Finding, sample_rows

EXPECTED = [
    "trip_id", "duration", "start_time", "end_time",
    "start_station", "start_lat", "start_lon",
    "end_station", "end_lat", "end_lon",
    "bike_id", "plan_duration", "trip_route_category", "passholder_type", "bike_type",
]
VIRTUAL_STATION = 3000
# Rough LA County service envelope; anything outside is a bad coordinate.
LAT_RANGE = (33.6, 34.4)
LON_RANGE = (-118.75, -117.9)
EXPECTED_PLAN_DAYS = {"Walk-up": {0, 1}, "One Day Pass": {1}, "Monthly Pass": {30},
                      "Annual Pass": {365}, "Flex Pass": {365}}
MAX_PLAUSIBLE_KMH = 35.0

_QUARTER_RE = re.compile(r"(?:year=)?(?P<y>20\d\d)\D{1,9}?(?:quarter=|q)(?P<q>[1-4])", re.I)


def file_quarter(path: Path) -> str | None:
    m = _QUARTER_RE.search(str(path))
    return f"{m['y']}-Q{m['q']}" if m else None


def read_trip_files(paths: list[Path]) -> tuple[pd.DataFrame, list[dict]]:
    frames, headers = [], []
    for p in paths:
        df = pd.read_csv(p, dtype=str, keep_default_na=False, encoding_errors="replace")
        raw_cols = list(df.columns)
        df.columns = ["_".join(c.replace("﻿", "").strip().lower().split()) for c in df.columns]
        headers.append({"file": str(p), "quarter": file_quarter(p), "rows": len(df),
                        "raw_header": raw_cols,
                        "missing": [c for c in EXPECTED if c not in df.columns],
                        "extra": [c for c in df.columns if c not in EXPECTED]})
        for c in EXPECTED:
            if c not in df.columns:
                df[c] = ""
        df["_source_file"] = p.name
        df["_file_quarter"] = file_quarter(p)
        df["_row_number"] = np.arange(1, len(df) + 1)
        frames.append(df)
    if not frames:
        raise FileNotFoundError("no trip CSV files found")
    return pd.concat(frames, ignore_index=True), headers


def type_trips(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    t = pd.DataFrame(index=raw.index)
    for c in ("trip_id", "duration", "start_station", "end_station", "plan_duration"):
        t[c] = pd.to_numeric(raw[c].str.strip(), errors="coerce").astype("Int64")
    for c in ("start_lat", "start_lon", "end_lat", "end_lon"):
        t[c] = pd.to_numeric(raw[c].str.strip(), errors="coerce")
    formats = {}
    for c in ("start_time", "end_time"):
        t[c], formats[c] = parse_with_formats(raw[c], TIMESTAMP_FORMATS)
    for c in ("bike_id", "trip_route_category", "passholder_type", "bike_type",
              "_source_file", "_file_quarter"):
        t[c] = raw[c].str.strip().replace("", pd.NA)
    return t, formats


def load_station_table(path: Path | None) -> pd.DataFrame | None:
    if path is None:
        return None
    files = sorted(path.rglob("*.csv")) if path.is_dir() else [path]
    if not files:
        return None
    st = pd.read_csv(files[-1], dtype=str, keep_default_na=False)
    st.columns = ["_".join(c.strip().lower().split()) for c in st.columns]
    st["kiosk_id"] = pd.to_numeric(st["kiosk_id"], errors="coerce").astype("Int64")
    st["latitude"] = pd.to_numeric(st["latitude"], errors="coerce")
    st["longitude"] = pd.to_numeric(st["longitude"], errors="coerce")
    st["go_live_date"] = pd.to_datetime(st["go_live_date"], format="%m/%d/%Y", errors="coerce")
    return st.drop_duplicates("kiosk_id", keep="last")


def profile_trips(paths: list[Path], station_path: Path | None = None) -> dict:
    raw, headers = read_trip_files(paths)
    t, ts_formats = type_trips(raw)
    stations = load_station_table(station_path)
    n = len(t)
    F: list[Finding] = []
    cols = ["_source_file", "trip_id", "start_time", "end_time", "duration",
            "start_station", "end_station", "bike_id", "passholder_type", "plan_duration"]

    def add(check, severity, mask, detail, rule=None, sample_cols=None):
        mask = mask.fillna(False).astype(bool)
        F.append(Finding(check, severity, int(mask.sum()), n, detail, rule,
                         sample_rows(raw, mask, sample_cols or cols)))

    # --- structure -----------------------------------------------------------
    for h in headers:
        if h["missing"] or h["extra"]:
            F.append(Finding("header_drift", "error", 1, len(headers),
                             f"{h['file']}: missing={h['missing']} extra={h['extra']}",
                             "schema_columns_present"))
    multi_fmt = {c: f for c, f in ts_formats.items() if len(f) > 1}
    F.append(Finding("timestamp_formats", "warning" if multi_fmt else "info",
                     len(multi_fmt), 2, f"formats seen per column: {ts_formats}. The data page "
                     "says ISO 8601 but files may use M/D/YYYY H:MM without seconds."))

    # --- keys ----------------------------------------------------------------
    add("trip_id_null", "error", t["trip_id"].isna(), "trip_id missing or non-numeric", "trip_id_not_null")
    dup_in_file = t.duplicated(["_source_file", "trip_id"], keep=False) & t["trip_id"].notna()
    add("trip_id_duplicate_in_file", "error", dup_in_file, "same trip_id twice in one file",
        "trip_id_unique_in_file")
    dup_any = t.duplicated(["trip_id"], keep=False) & t["trip_id"].notna()
    add("trip_id_duplicate_across_files", "warning", dup_any & ~dup_in_file,
        "trip_id reused across quarters. The publisher says ids are only unique within a "
        "file, so the warehouse key must be (source_quarter, trip_id).")
    exact_dup = raw.duplicated(EXPECTED, keep="first")
    add("exact_duplicate_rows", "error", exact_dup, "fully identical rows", "no_exact_duplicates")

    # --- time ----------------------------------------------------------------
    add("start_time_unparseable", "error", t["start_time"].isna() & ~is_null(raw["start_time"]),
        "start_time present but unparseable", "start_time_parseable")
    add("end_time_unparseable", "error", t["end_time"].isna() & ~is_null(raw["end_time"]),
        "end_time present but unparseable", "end_time_parseable")
    add("end_before_start", "error", t["end_time"] < t["start_time"],
        "end_time earlier than start_time", "end_after_start")
    span_min = (t["end_time"] - t["start_time"]).dt.total_seconds() / 60
    diff = (t["duration"].astype(float) - span_min).abs()
    add("duration_mismatch", "warning", diff > 1,
        "duration differs from end_time - start_time by more than 1 minute (timestamps are "
        "minute precision, so +/-1 is rounding)", "duration_matches_timestamps")
    add("duration_out_of_range", "error", (t["duration"] < 1) | (t["duration"] > 1440),
        "duration outside the published 1..1440 minute window", "duration_in_range")
    add("duration_at_cap", "info", t["duration"] == 1440,
        "trips exactly at the 24h cap (likely bikes not docked, not real rides)")
    q_of_start = t["start_time"].dt.year.astype("Int64").astype(str) + "-Q" + \
        t["start_time"].dt.quarter.astype("Int64").astype(str)
    add("trip_outside_file_quarter", "warning",
        t["_file_quarter"].notna() & t["start_time"].notna() & (q_of_start != t["_file_quarter"]),
        "start_time is not in the quarter the file is published for", "start_in_file_quarter")

    # --- stations & geo -------------------------------------------------------
    for side in ("start", "end"):
        add(f"{side}_station_null", "error", t[f"{side}_station"].isna(),
            f"{side}_station missing", f"{side}_station_not_null")
        add(f"{side}_virtual_station", "info", t[f"{side}_station"] == VIRTUAL_STATION,
            f"{side}_station is 3000 'Virtual Station' (staff remote check-in/out, lat/lon 0 or blank)")
        lat, lon = t[f"{side}_lat"], t[f"{side}_lon"]
        add(f"{side}_coords_missing", "warning", lat.isna() | lon.isna(),
            f"{side}_lat/lon blank", f"{side}_coords_present")
        add(f"{side}_coords_out_of_area", "error",
            lat.notna() & lon.notna() & ~(lat.between(*LAT_RANGE) & lon.between(*LON_RANGE)),
            f"{side} coordinates outside LA (zeros, swapped or sign-flipped)",
            f"{side}_coords_in_service_area")
        if stations is not None:
            known = t[f"{side}_station"].isin(stations["kiosk_id"])
            add(f"{side}_station_unknown", "error", t[f"{side}_station"].notna() & ~known,
                f"{side}_station not in the station table", f"{side}_station_exists")
            m = t[[f"{side}_station", "start_time"]].merge(
                stations[["kiosk_id", "go_live_date"]], how="left",
                left_on=f"{side}_station", right_on="kiosk_id")
            m.index = t.index
            add(f"{side}_before_go_live", "warning", m["start_time"] < m["go_live_date"],
                f"trip uses {side} station before its go-live date (station id reused or date wrong)")

    rt = t["trip_route_category"]
    same = t["start_station"] == t["end_station"]
    add("route_category_inconsistent", "error",
        ((rt == "Round Trip") & ~same) | ((rt == "One Way") & same),
        "trip_route_category disagrees with start/end station", "route_category_consistent")

    dist = haversine_km(t["start_lat"], t["start_lon"], t["end_lat"], t["end_lon"])
    t["_distance_km"] = dist
    speed = dist / (t["duration"].astype(float) / 60)
    add("implausible_speed", "warning", (t["duration"] > 0) & (speed > MAX_PLAUSIBLE_KMH),
        f"straight-line speed above {MAX_PLAUSIBLE_KMH} km/h", "speed_plausible")

    # --- plans ---------------------------------------------------------------
    combo = t.groupby(["passholder_type", "plan_duration"], dropna=False).size()
    bad_plan = pd.Series(False, index=t.index)
    for ptype, allowed in EXPECTED_PLAN_DAYS.items():
        bad_plan |= (t["passholder_type"] == ptype) & ~t["plan_duration"].isin(list(allowed))
    add("plan_duration_inconsistent", "warning", bad_plan,
        "plan_duration does not match passholder_type (the data page says walk-up = 0, "
        "but sample rows carry 1)", "plan_matches_passholder")

    # --- bikes ---------------------------------------------------------------
    add("bike_id_null", "warning", t["bike_id"].isna(), "bike_id missing", "bike_id_not_null")
    bt = t.dropna(subset=["bike_id", "start_time"]).sort_values(["bike_id", "start_time"])
    prev_end_time = bt.groupby("bike_id")["end_time"].shift()
    prev_end_station = bt.groupby("bike_id")["end_station"].shift()
    overlap = pd.Series(False, index=t.index)
    overlap.loc[bt.index] = (bt["start_time"] < prev_end_time).to_numpy()
    add("bike_overlapping_trips", "error", overlap,
        "same bike starts a trip before its previous trip ended", "bike_no_overlap")
    teleport = pd.Series(False, index=t.index)
    has_prev = prev_end_station.notna()
    teleport.loc[bt.index] = (has_prev & (bt["start_station"] != prev_end_station)).to_numpy()
    add("bike_rebalanced_or_gap", "info", teleport,
        "bike starts at a different station than its last trip ended (rebalancing, "
        "removed staff trips, or a dropped record)")
    types_per_bike = t.dropna(subset=["bike_id"]).groupby("bike_id")["bike_type"].nunique()
    multi = t["bike_id"].isin(types_per_bike[types_per_bike > 1].index)
    add("bike_id_multiple_types", "warning", multi,
        "one bike_id appears with more than one bike_type", "bike_type_stable_per_bike")

    # --- domains -------------------------------------------------------------
    domains = {c: t[c].value_counts(dropna=False).to_dict()
               for c in ("passholder_type", "plan_duration", "trip_route_category", "bike_type")}

    station_coord_drift = _station_coordinate_drift(t, stations)

    return {
        "rows": n,
        "files": headers,
        "columns": profile_frame(raw[EXPECTED]),
        "timestamp_formats": ts_formats,
        "domains": {k: {str(kk): int(vv) for kk, vv in v.items()} for k, v in domains.items()},
        "passholder_x_plan": {f"{a}|{b}": int(v) for (a, b), v in combo.items()},
        "findings": [f.to_dict() for f in F],
        "station_coordinate_drift": station_coord_drift,
        "insights": _insights(t),
    }


def _station_coordinate_drift(t: pd.DataFrame, stations: pd.DataFrame | None) -> dict:
    """Trip files carry station lat/lon per row. Count distinct coordinates per
    station and how far they sit from the station table (stations get moved)."""
    sides = [t[[f"{p}_station", f"{p}_lat", f"{p}_lon"]].set_axis(
        ["start_station", "start_lat", "start_lon"], axis=1) for p in ("start", "end")]
    s = pd.concat(sides).dropna().drop_duplicates()
    s = s[s["start_station"] != VIRTUAL_STATION]
    per = s.groupby("start_station").size()
    out = {"stations_with_multiple_coords": int((per > 1).sum()),
           "stations_seen": int(per.size)}
    if stations is not None:
        m = s.merge(stations, left_on="start_station", right_on="kiosk_id")
        m["km"] = haversine_km(m["start_lat"], m["start_lon"], m["latitude"], m["longitude"])
        far = m[m["km"] > 0.05].sort_values("km", ascending=False)
        out["coords_over_50m_from_station_table"] = int(far["start_station"].nunique())
        out["worst"] = far.head(10)[["start_station", "kiosk_name", "start_lat", "start_lon",
                                     "latitude", "longitude", "km"]].round(4).to_dict("records")
    return out


def _insights(t: pd.DataFrame) -> dict:
    ok = t.dropna(subset=["start_time"])
    quarter = ok["start_time"].dt.to_period("Q").astype(str)
    by_q = ok.groupby(quarter).agg(
        trips=("trip_id", "size"),
        median_duration=("duration", "median"),
        electric_share=("bike_type", lambda s: round(float((s == "electric").mean()), 3)),
        round_trip_share=("trip_route_category", lambda s: round(float((s == "Round Trip").mean()), 3)),
        active_bikes=("bike_id", "nunique"),
        active_stations=("start_station", "nunique"),
    )
    return {
        "by_quarter": by_q.reset_index(names="quarter").to_dict("records"),
        "trips_by_hour": ok["start_time"].dt.hour.value_counts().sort_index().to_dict(),
        "trips_by_weekday": ok["start_time"].dt.day_name().value_counts().to_dict(),
        "median_duration_by_passholder": ok.groupby("passholder_type")["duration"].median().to_dict(),
        "top_start_stations": ok["start_station"].value_counts().head(15).to_dict(),
        "top_routes": {f"{a}->{b}": int(v) for (a, b), v in
                       ok.groupby(["start_station", "end_station"]).size()
                         .sort_values(ascending=False).head(15).items()},
        "median_distance_km_one_way": float(np.nanmedian(
            t.loc[t["trip_route_category"] == "One Way", "_distance_km"]))
            if (t["trip_route_category"] == "One Way").any() else None,
    }
