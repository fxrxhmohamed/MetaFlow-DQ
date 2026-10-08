-- Grain: one station per status poll. History exists only from the day the
-- poller started; the public site does not publish past availability.
{{ config(materialized='incremental', unique_key=['snapshot_ts', 'station_id']) }}

select
    s.snapshot_ts,
    to_number(to_char(convert_timezone('America/Los_Angeles', s.snapshot_ts), 'YYYYMMDD')) as date_key,
    {{ dbt_utils.generate_surrogate_key(['s.station_id']) }} as station_key,
    s.station_id,
    s.public_status,
    s.is_reporting,
    s.total_docks,
    s.docks_available,
    s.docks_disabled,
    s.bikes_available,
    s.classic_bikes_available,
    s.smart_bikes_available,
    s.electric_bikes_available,
    s.bikes_available = 0 and s.is_reporting              as is_empty,
    s.docks_available = 0 and s.is_reporting              as is_full,
    div0(s.bikes_available, nullif(s.total_docks - s.docks_disabled, 0)) as fill_ratio
from {{ ref('stg_station_status') }} s
{% if is_incremental() %}
where s.snapshot_ts > (select max(snapshot_ts) from {{ this }})
{% endif %}
