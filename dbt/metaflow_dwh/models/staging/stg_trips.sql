-- Typed trips. One row per published row; nothing is dropped here, problems
-- become flags that the DQ layer and marts filter on.
with src as (
    select * from {{ source('raw', 'trips') }}
),

typed as (
    select
        -- trip_id is only unique inside one quarterly file
        md5(concat_ws('|', _year, _quarter, trip_id))         as trip_key,
        try_to_number(trip_id)                                 as trip_id,
        _year                                                  as source_year,
        _quarter                                               as source_quarter,
        try_to_number(duration)                                as duration_min,
        {{ parse_trip_ts('start_time') }}                      as started_at_local,
        {{ parse_trip_ts('end_time') }}                        as ended_at_local,
        try_to_number(start_station)                           as start_station_id,
        try_to_double(start_lat)                               as start_lat,
        try_to_double(start_lon)                               as start_lon,
        try_to_number(end_station)                             as end_station_id,
        try_to_double(end_lat)                                 as end_lat,
        try_to_double(end_lon)                                 as end_lon,
        nullif(trim(bike_id), '')                              as bike_id,
        try_to_number(plan_duration)                           as plan_duration_days,
        initcap(trim(trip_route_category))                     as trip_route_category,
        trim(passholder_type)                                  as passholder_type,
        lower(trim(bike_type))                                 as bike_type,
        start_time                                             as start_time_raw,
        end_time                                               as end_time_raw,
        _batch_id,
        _source_file,
        _file_row_number,
        _loaded_at
    from src
),

deduped as (
    -- a quarter re-landed under a new batch replaces the older copy
    select *
    from typed
    qualify row_number() over (partition by trip_key order by _loaded_at desc, _file_row_number) = 1
)

select
    *,
    -- Metro times are local wall clock (America/Los_Angeles)
    convert_timezone('America/Los_Angeles', 'UTC', started_at_local)            as started_at_utc,
    datediff('minute', started_at_local, ended_at_local)                         as span_min,
    start_station_id = end_station_id                                            as is_same_station,
    start_station_id = {{ var('virtual_station_id') }}
        or end_station_id = {{ var('virtual_station_id') }}                      as touches_virtual_station
from deduped
