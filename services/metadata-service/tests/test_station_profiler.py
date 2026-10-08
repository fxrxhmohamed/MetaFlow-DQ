from app.profiler.stations import profile_station_table

from conftest import FIXTURES


def test_station_table_findings():
    report = profile_station_table(FIXTURES / "stations_subset.csv")
    f = {x["check"]: x["count"] for x in report["findings"]}
    assert report["raw_header"][3] == "Region "  # trailing space in the published header
    assert f["header_whitespace"] == 1
    assert f["positive_longitude"] == 1  # 4649
    assert f["zero_coordinates"] == 1  # 3000
    assert f["name_reused_by_other_id"] == 2  # Venice & Lincoln 4205 / 4351
    assert f["kiosk_id_not_sorted"] >= 1
    assert f["non_physical_station"] == 2  # virtual + public bike rack
    assert f["far_from_own_region"] >= 1  # 4373 'Westside' rack in North Hollywood
