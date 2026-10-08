-- One row per station id. Name, region and status come from the latest station
-- table; trip-observed coordinates and first/last use come from the trips,
-- because stations are moved and the table only keeps the latest position.
with st as (
    select * from {{ ref('stg_stations') }}
),

usage as (
    select station_id,
           min(started_at_local) as first_trip_at,
           max(started_at_local) as last_trip_at,
           count(*)              as trip_endpoints,
           median(lat)           as observed_lat,
           median(lon)           as observed_lon
    from (
        select start_station_id as station_id, started_at_local, start_lat as lat, start_lon as lon
        from {{ ref('int_trips_validated') }}
        union all
        select end_station_id, started_at_local, end_lat, end_lon
        from {{ ref('int_trips_validated') }}
    )
    where station_id is not null
    group by station_id
)

select
    {{ dbt_utils.generate_surrogate_key(['coalesce(st.station_id, u.station_id)']) }} as station_key,
    coalesce(st.station_id, u.station_id)                     as station_id,
    coalesce(st.station_name, 'Unknown station ' || u.station_id) as station_name,
    st.region,
    st.status,
    coalesce(st.is_active, false)                             as is_active,
    st.go_live_date,
    -- flipped longitude in the source (e.g. 4649) is repaired here, flagged upstream
    st.latitude,
    iff(st.has_flipped_longitude, -st.longitude, st.longitude) as longitude,
    u.observed_lat,
    u.observed_lon,
    coalesce(st.is_non_physical, u.station_id = {{ var('virtual_station_id') }}) as is_non_physical,
    coalesce(st.is_temporary_event, false)                    as is_temporary_event,
    st.station_id is null                                     as is_missing_from_station_table,
    u.first_trip_at,
    u.last_trip_at,
    coalesce(u.trip_endpoints, 0)                             as trip_endpoints
from st
full outer join usage u on u.station_id = st.station_id
