"""Catalog of Metro Bike Share public data sources.

The quarterly trip files are published at https://bikeshare.metro.net/about/data/.
File names are NOT stable across years (``metro-bike-share-trips-2019-q4.csv.zip``,
``metro-trips-2021-q1-1.zip``, ``metro-trips-2020-q2-v2.zip`` ...), and the upload
folder (``/wp-content/uploads/YYYY/MM/``) is the publish month, not the data
quarter. So the URLs are pinned here and can also be re-discovered from the page.
"""
from __future__ import annotations

import re
from dataclasses import dataclass

DATA_PAGE_URL = "https://bikeshare.metro.net/about/data/"

# Live station status. The GeoJSON is the "Station Status" format from the data
# page; the GBFS feed carries the same counts in a standard schema plus
# last_reported per station, which the GeoJSON lacks.
STATION_STATUS_GEOJSON_URL = "https://bikeshare.metro.net/stations/json/"
GBFS_ROOT_URL = "https://gbfs.bcycle.com/bcycle_lametro/gbfs.json"
GBFS_STATION_STATUS_URL = "https://gbfs.bcycle.com/bcycle_lametro/station_status.json"
GBFS_STATION_INFORMATION_URL = "https://gbfs.bcycle.com/bcycle_lametro/station_information.json"

# Station table is republished under a new dated name on each update.
STATION_TABLE_URL = (
    "https://bikeshare.metro.net/wp-content/uploads/2026/07/"
    "metro-bike-share-stations-2026-07-15.csv"
)


@dataclass(frozen=True)
class TripQuarter:
    year: int
    quarter: int
    url: str

    @property
    def key(self) -> str:
        return f"{self.year}-q{self.quarter}"


_UPLOADS = "https://bikeshare.metro.net/wp-content/uploads"

TRIP_QUARTERS: tuple[TripQuarter, ...] = (
    TripQuarter(2024, 1, f"{_UPLOADS}/2024/04/metro-trips-2024-q1.zip"),
    TripQuarter(2024, 2, f"{_UPLOADS}/2024/07/metro-trips-2024-q2.zip"),
    TripQuarter(2024, 3, f"{_UPLOADS}/2024/10/metro-trips-2024-q3.zip"),
    TripQuarter(2024, 4, f"{_UPLOADS}/2025/01/metro-trips-2024-q4.zip"),
    TripQuarter(2025, 1, f"{_UPLOADS}/2025/04/metro-trips-2025-q1.zip"),
    TripQuarter(2025, 2, f"{_UPLOADS}/2025/07/metro-trips-2025-q2.zip"),
    TripQuarter(2025, 3, f"{_UPLOADS}/2025/10/metro-trips-2025-q3.zip"),
    TripQuarter(2025, 4, f"{_UPLOADS}/2026/01/metro-trips-2025-q4.zip"),
    TripQuarter(2026, 1, f"{_UPLOADS}/2026/04/metro-trips-2026-q1.zip"),
    TripQuarter(2026, 2, f"{_UPLOADS}/2026/07/metro-trips-2026-q2.zip"),
    # 2026 Q3 is published around mid October 2026; discover_trip_quarters()
    # picks it up from the data page once it is there.
)

_TRIP_HREF = re.compile(
    r'href="(?P<url>https?://[^"]*?(?:metro-trips|metro-bike-share-trips)-'
    r'(?P<year>\d{4})-q(?P<q>[1-4])[^"]*?\.zip)"',
    re.IGNORECASE,
)
_STATION_HREF = re.compile(
    r'href="(?P<url>https?://[^"]*?metro-bike-share-stations-[^"]*?\.csv)"', re.IGNORECASE
)


def discover_trip_quarters(html: str, years: set[int] | None = None) -> list[TripQuarter]:
    """Parse the data page HTML for quarterly trip zips (newest link wins per quarter)."""
    found: dict[tuple[int, int], TripQuarter] = {}
    for m in _TRIP_HREF.finditer(html):
        year, q = int(m["year"]), int(m["q"])
        if years and year not in years:
            continue
        found.setdefault((year, q), TripQuarter(year, q, m["url"]))
    return sorted(found.values(), key=lambda t: (t.year, t.quarter))


def discover_station_table_url(html: str) -> str | None:
    m = _STATION_HREF.search(html)
    return m["url"] if m else None


def quarters_for(years: set[int]) -> list[TripQuarter]:
    return [t for t in TRIP_QUARTERS if t.year in years]
