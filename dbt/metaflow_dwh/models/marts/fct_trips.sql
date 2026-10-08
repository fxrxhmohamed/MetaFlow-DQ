-- Grain: one valid trip. Rows that failed a hard rule stay out (they are in
-- QUARANTINE or flagged in int_trips_validated); warnings ride along as an array.
{{ config(cluster_by=['start_date_key']) }}

select
    t.trip_key,
    t.trip_id,
    t.source_year,
    t.source_quarter,
    to_number(to_char(t.started_at_local, 'YYYYMMDD'))   as start_date_key,
    hour(t.started_at_local)                              as start_hour,
    t.started_at_local,
    t.ended_at_local,
    t.started_at_utc,
    {{ dbt_utils.generate_surrogate_key(['t.start_station_id']) }} as start_station_key,
    {{ dbt_utils.generate_surrogate_key(['t.end_station_id']) }}   as end_station_key,
    {{ dbt_utils.generate_surrogate_key(['t.passholder_type', 't.plan_duration_days']) }} as rider_plan_key,
    t.bike_id,
    t.bike_type,
    t.trip_route_category,
    t.duration_min,
    iff(t.is_same_station or t.touches_virtual_station, null,
        {{ haversine_km('t.start_lat', 't.start_lon', 't.end_lat', 't.end_lon') }}) as distance_km,
    t.touches_virtual_station,
    t.duration_min = 1440                                 as is_capped_duration,
    t.warned_rules
from {{ ref('int_trips_validated') }} t
where t.is_valid
