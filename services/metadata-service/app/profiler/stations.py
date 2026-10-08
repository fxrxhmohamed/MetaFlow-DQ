"""Profile the station table (CSV) and live station status snapshots (GeoJSON / GBFS)."""
from __future__ import annotations

import json
import re
from pathlib import Path

import pandas as pd

from .core import DATE_FORMATS, haversine_km, parse_with_formats, profile_frame
from .findings import Finding, sample_rows
from .trips import LAT_RANGE, LON_RANGE

STATION_COLUMNS = ["kiosk_id", "kiosk_name", "go_live_date", "region", "status", "latitude", "longitude"]
# Stations that are not physical docks: staff/virtual, dockless buckets, event pop-ups.
NON_PHYSICAL = re.compile(r"virtual|free bikes|out of service|public bike rack", re.I)
TEMPORARY = re.compile(r"ciclavia|ciclamini|pop[- ]?up|\bhub\b|temp|pride ride|open streets", re.I)


def profile_station_table(path: Path) -> dict:
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    raw_header = list(raw.columns)
    raw.columns = ["_".join(c.strip().lower().split()) for c in raw.columns]
    n = len(raw)
    F: list[Finding] = []

    def add(check, severity, mask, detail, rule=None):
        mask = mask.fillna(False).astype(bool)
        F.append(Finding(check, severity, int(mask.sum()), n, detail, rule,
                         sample_rows(raw, mask, None, 8)))

    padded = [c for c in raw_header if c != c.strip()]
    if padded:
        F.append(Finding("header_whitespace", "warning", len(padded), len(raw_header),
                         f"header names with stray whitespace: {padded!r}"))

    kid = pd.to_numeric(raw["kiosk_id"], errors="coerce")
    lat = pd.to_numeric(raw["latitude"], errors="coerce")
    lon = pd.to_numeric(raw["longitude"], errors="coerce")
    live, fmt_hits = parse_with_formats(raw["go_live_date"], DATE_FORMATS)
    name = raw["kiosk_name"].str.strip()

    add("kiosk_id_duplicate", "error", kid.duplicated(keep=False), "kiosk_id repeated",
        "station_id_unique")
    add("kiosk_id_not_sorted", "info", kid.diff() < 0,
        "rows out of id order (appended later, e.g. 4514, 4244): sort by id, never trust row order")
    add("name_reused_by_other_id", "warning", name.duplicated(keep=False),
        "same kiosk_name under several kiosk_ids (station replaced/re-numbered or event hubs "
        "created twice). Station identity is the id, never the name.")
    add("zero_coordinates", "warning", (lat == 0) | (lon == 0),
        "lat/lon = 0 (Virtual Station, Free Bikes buckets)")
    add("positive_longitude", "error", lon > 0, "longitude sign flipped (LA is ~ -118)",
        "station_coords_in_service_area")
    add("coords_out_of_area", "error",
        (lat != 0) & (lon != 0) & ~(lat.between(*LAT_RANGE) & lon.between(*LON_RANGE)),
        "coordinates outside LA", "station_coords_in_service_area")
    has_xy = (lat != 0) & (lon != 0) & lat.notna() & lon.notna()
    region = raw["region"].str.strip()
    centre = pd.DataFrame({"r": region, "lat": lat, "lon": lon})[has_xy].groupby("r").median()
    c = centre.reindex(region)
    km_from_region = pd.Series(haversine_km(lat, lon, c["lat"].to_numpy(), c["lon"].to_numpy()),
                               index=raw.index)
    add("far_from_own_region", "warning", has_xy & (km_from_region > 10),
        "coordinates over 10 km from the median of the station's region (wrong region or "
        "coordinates copied from another station, e.g. 4373/4374 'Westside' racks sit in "
        "North Hollywood)", "station_region_consistent")
    coord_key = lat.round(5).astype(str) + "," + lon.round(5).astype(str)
    add("coords_shared_by_ids", "info", coord_key.duplicated(keep=False) & (lat != 0),
        "several ids at the same point (station re-numbered when a program relaunched)")
    add("go_live_unparseable", "error", live.isna(), "go_live_date not a date", "go_live_parseable")
    padded_date = raw["go_live_date"].str.match(r"^0\d/|^\d{1,2}/0\d/")
    unpadded = raw["go_live_date"].str.match(r"^[1-9]/|^\d{1,2}/[1-9]/")
    if padded_date.any() and unpadded.any():
        add("go_live_mixed_padding", "info", padded_date,
            f"go_live_date mixes '07/07/2016' and '7/7/2016' styles (formats: {fmt_hits})")
    add("region_na", "warning", raw["region"].str.strip().str.upper().isin(["N/A", ""]),
        "region is N/A or blank")
    add("non_physical_station", "info", name.str.contains(NON_PHYSICAL),
        "virtual / dockless / bike-rack pseudo stations: exclude from capacity metrics")
    add("temporary_event_station", "info", name.str.contains(TEMPORARY),
        "CicLAvia and pop-up event hubs: short-lived, flag in dim_station")

    return {
        "rows": n,
        "raw_header": raw_header,
        "columns": profile_frame(raw),
        "domains": {"region": raw["region"].value_counts().to_dict(),
                    "status": raw["status"].value_counts().to_dict()},
        "go_live_by_year": live.dt.year.value_counts().sort_index().astype(int).to_dict(),
        "findings": [f.to_dict() for f in F],
    }


def profile_station_status(paths: list[Path], station_table: Path | None = None) -> dict:
    """Profile one or more stored GeoJSON snapshots (see ingestion snapshot_station_status)."""
    rows = []
    for p in paths:
        doc = json.loads(p.read_text())
        payload = doc.get("payload", doc)
        ts = doc.get("snapshot_ts")
        for feat in payload.get("features", []):
            r = dict(feat.get("properties") or {})
            g = (feat.get("geometry") or {}).get("coordinates") or [None, None]
            r["geom_lon"], r["geom_lat"], r["snapshot_ts"] = g[0], g[1], ts
            rows.append(r)
    df = pd.DataFrame(rows)
    n = len(df)
    F: list[Finding] = []

    def add(check, severity, mask, detail, rule=None):
        mask = mask.fillna(False).astype(bool)
        F.append(Finding(check, severity, int(mask.sum()), n, detail, rule,
                         sample_rows(df, mask, [c for c in ("kioskId", "name", "bikesAvailable",
                                    "docksAvailable", "totalDocks", "kioskPublicStatus",
                                    "kioskConnectionStatus", "kioskUnresponsiveTime")
                                    if c in df.columns], 8)))

    types = df[["classicBikesAvailable", "smartBikesAvailable", "electricBikesAvailable"]].sum(axis=1)
    add("bikes_ne_sum_of_types", "error", df["bikesAvailable"] != types,
        "bikesAvailable != classic + smart + electric", "status_bike_types_sum")
    add("bikes_plus_docks_gt_total", "error",
        df["bikesAvailable"] + df["docksAvailable"] > df["totalDocks"],
        "more bikes+free docks than docks exist", "status_capacity_not_exceeded")
    add("bikes_plus_docks_lt_total", "info",
        df["bikesAvailable"] + df["docksAvailable"] < df["totalDocks"],
        "gap = docks out of service / reserved; keep it as disabled_docks")
    add("unresponsive_kiosk", "warning", df["kioskConnectionStatus"] != "Active",
        "kiosk not reporting: its counts are stale, exclude from availability KPIs")
    add("geometry_ne_properties", "warning",
        ((df["geom_lat"] - df["latitude"]).abs() > 1e-5) | ((df["geom_lon"] - df["longitude"]).abs() > 1e-5),
        "GeoJSON geometry and latitude/longitude properties disagree")
    add("open_close_wraparound", "info", (df["openTime"] == "05:45:00") & (df["closeTime"] == "05:44:00"),
        "openTime 05:45 / closeTime 05:44 encodes a 24h station, not a 1 minute closure")
    add("timezone_label_static", "info", df["timeZone"] == "Pacific Standard Time",
        "Windows zone name, constant all year; convert with America/Los_Angeles (DST aware)")

    if station_table is not None:
        st = pd.read_csv(station_table, dtype=str)
        st.columns = ["_".join(c.strip().lower().split()) for c in st.columns]
        ids = set(pd.to_numeric(st["kiosk_id"], errors="coerce").dropna().astype(int))
        add("status_station_not_in_table", "error", ~df["kioskId"].isin(ids),
            "live kiosk missing from the station table", "station_exists")
        inactive = set(pd.to_numeric(st.loc[st["status"].str.strip() == "Inactive", "kiosk_id"],
                                     errors="coerce").dropna().astype(int))
        add("live_but_table_inactive", "warning", df["kioskId"].isin(inactive),
            "kiosk reports live status but the station table says Inactive")

    return {
        "rows": n, "snapshots": len(paths),
        "columns": profile_frame(df.astype(str)),
        "domains": {c: df[c].astype(str).value_counts().to_dict()
                    for c in ("kioskPublicStatus", "kioskStatus", "kioskConnectionStatus",
                              "kioskType", "isVirtual", "isVisible", "isEventBased",
                              "isArchived", "hasGeofence", "clientVersion") if c in df.columns},
        "findings": [f.to_dict() for f in F],
    }
