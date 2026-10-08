-- Latest station table snapshot, typed and classified.
with latest as (
    select *
    from {{ source('raw', 'stations') }}
    qualify _snapshot_date = max(_snapshot_date) over ()
)

select
    try_to_number(kiosk_id)                                  as station_id,
    trim(kiosk_name)                                         as station_name,
    try_to_date(go_live_date, 'MM/DD/YYYY')                  as go_live_date,
    nullif(nullif(trim(region), ''), 'N/A')                  as region,
    trim(status)                                             as status,
    trim(status) = 'Active'                                  as is_active,
    nullif(try_to_double(latitude), 0)                       as latitude,
    nullif(try_to_double(longitude), 0)                      as longitude,
    try_to_double(longitude) > 0                             as has_flipped_longitude,
    regexp_like(kiosk_name, '.*(Virtual|Free Bikes|Out of Service|Public Bike Rack).*', 'i')
                                                             as is_non_physical,
    regexp_like(kiosk_name, '.*(cicla|pop-?up|hub|temp|pride ride|open streets).*', 'i')
                                                             as is_temporary_event,
    _snapshot_date                                           as snapshot_date
from latest
qualify row_number() over (partition by try_to_number(kiosk_id) order by _file_row_number desc) = 1
