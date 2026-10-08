import shutil

from app.profiler.trips import file_quarter, profile_trips

from conftest import FIXTURES

HEADER = ("trip_id,duration,start_time,end_time,start_station,start_lat,start_lon,end_station,"
          "end_lat,end_lon,bike_id,plan_duration,trip_route_category,passholder_type,bike_type\n")


def _findings(report):
    return {f["check"]: f["count"] for f in report["findings"]}


def test_user_sample_is_clean(tmp_path):
    shutil.copy(FIXTURES / "metro-trips-2026-q1-sample.csv", tmp_path)
    report = profile_trips(sorted(tmp_path.glob("*.csv")), FIXTURES / "stations_subset.csv")
    f = _findings(report)
    assert report["rows"] == 19
    assert report["timestamp_formats"]["start_time"] == {"%m/%d/%Y %H:%M": 19}
    for check in ("end_before_start", "duration_mismatch", "route_category_inconsistent",
                  "bike_overlapping_trips", "trip_id_duplicate_in_file", "trip_outside_file_quarter"):
        assert f[check] == 0, check
    # Station 3011 is ~75 m from where the station table puts it.
    assert report["station_coordinate_drift"]["worst"][0]["start_station"] == 3011


def test_detects_injected_problems(tmp_path):
    rows = [
        # ok
        "1,10,01/05/2025 8:00,01/05/2025 8:10,3005,34.0485,-118.258537,3035,34.048401,-118.260948,B1,30,One Way,Monthly Pass,standard",
        # duplicate trip_id, and bike B1 starts before its previous trip ended
        "1,10,01/05/2025 8:05,01/05/2025 8:15,3035,34.048401,-118.260948,3005,34.0485,-118.258537,B1,30,One Way,Monthly Pass,standard",
        # end before start, ISO timestamp format mixed into the file
        "3,5,2025-01-06 09:00:00,2025-01-06 08:00:00,3005,34.0485,-118.258537,3005,34.0485,-118.258537,B2,30,Round Trip,Monthly Pass,electric",
        # round trip label but different stations; duration disagrees with timestamps
        "4,99,01/07/2025 10:00,01/07/2025 10:10,3005,34.0485,-118.258537,3035,34.048401,-118.260948,B3,1,Round Trip,Walk-up,standard",
        # virtual station with zero coords, trip from another quarter
        "5,30,04/01/2025 10:00,04/01/2025 10:30,3000,0,0,3005,34.0485,-118.258537,B4,365,One Way,Monthly Pass,standard",
        # unknown station, duration over cap
        "6,1500,01/08/2025 10:00,01/09/2025 11:00,9999,34.05,-118.25,3005,34.0485,-118.258537,B5,30,One Way,Monthly Pass,standard",
    ]
    path = tmp_path / "metro-trips-2025-q1.csv"
    path.write_text(HEADER + "\n".join(rows) + "\n")
    report = profile_trips([path], FIXTURES / "stations_subset.csv")
    f = _findings(report)
    assert f["trip_id_duplicate_in_file"] == 2
    assert f["bike_overlapping_trips"] == 1
    assert f["end_before_start"] == 1
    assert len(report["timestamp_formats"]["start_time"]) == 2
    assert f["route_category_inconsistent"] == 1
    assert f["duration_mismatch"] >= 1
    assert f["start_virtual_station"] == 1
    assert f["start_coords_out_of_area"] == 1
    assert f["trip_outside_file_quarter"] == 1
    assert f["start_station_unknown"] == 1
    assert f["duration_out_of_range"] == 1
    assert f["plan_duration_inconsistent"] == 1  # Monthly Pass with 365 days


def test_file_quarter_from_names():
    from pathlib import Path
    assert file_quarter(Path("metro-trips-2024-q3.csv")) == "2024-Q3"
    assert file_quarter(Path("trips/year=2025/quarter=4/batch_id=x/a.csv")) == "2025-Q4"
    assert file_quarter(Path("metro-bike-share-trips-2019-q4.csv")) == "2019-Q4"
