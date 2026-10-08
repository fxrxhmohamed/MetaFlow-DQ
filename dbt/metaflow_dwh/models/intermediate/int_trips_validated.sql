-- Trips with every hard DQ rule evaluated in SQL. Mirrors the rule ids in
-- config/rules/quality_rules.yaml so dbt and the Python DQ engine agree.
-- Rows the DQ service already sent to quarantine are excluded.
with t as (
    select * from {{ ref('stg_trips') }}
),

stations as (
    select station_id, go_live_date from {{ ref('stg_stations') }}
),

checked as (
    select
        t.*,
        array_construct_compact(
            iff(t.trip_id is null,                                   'trip_id_not_null', null),
            iff(t.started_at_local is null,                          'start_time_parseable', null),
            iff(t.ended_at_local is null,                            'end_time_parseable', null),
            iff(t.ended_at_local < t.started_at_local,               'end_after_start', null),
            iff(t.duration_min is null or t.duration_min not between 1 and 1440,               'duration_in_range', null),
            iff(t.start_station_id is null,                          'start_station_not_null', null),
            iff(t.end_station_id is null,                            'end_station_not_null', null),
            iff(ss.station_id is null and t.start_station_id is not null, 'start_station_exists', null),
            iff(es.station_id is null and t.end_station_id is not null,   'end_station_exists', null),
            iff((t.trip_route_category = 'Round Trip') != t.is_same_station, 'route_category_consistent', null)
        ) as failed_rules,
        array_construct_compact(
            iff(abs(t.duration_min - t.span_min) > 1,                'duration_matches_timestamps', null),
            iff(year(t.started_at_local) != t.source_year
                or quarter(t.started_at_local) != t.source_quarter,  'start_in_file_quarter', null),
            iff(t.start_lat is null or t.start_lon is null,          'start_coords_present', null),
            iff(t.started_at_local < ss.go_live_date,                'start_before_go_live', null)
        ) as warned_rules
    from t
    left join stations ss on ss.station_id = t.start_station_id
    left join stations es on es.station_id = t.end_station_id
)

select
    c.*,
    array_size(c.failed_rules) = 0 as is_valid
from checked c
where not exists (
    select 1 from {{ source('quarantine', 'trips_rejected') }} q
    where q.trip_key = c.trip_key and q.remediation_status in ('pending', 'discarded')
)
