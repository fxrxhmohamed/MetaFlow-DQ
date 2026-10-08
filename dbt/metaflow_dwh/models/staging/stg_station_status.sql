-- One row per station per poll of the live GeoJSON feed.
with flat as (
    select
        s.snapshot_ts,
        f.value:properties as p
    from {{ source('raw', 'station_status_geojson') }} as s,
         lateral flatten(input => s.payload:features) as f
)

select
    snapshot_ts,
    p:kioskId::number                         as station_id,
    p:kioskPublicStatus::varchar              as public_status,
    p:kioskStatus::varchar                    as kiosk_status,
    p:kioskConnectionStatus::varchar          as connection_status,
    p:kioskConnectionStatus::varchar = 'Active' as is_reporting,
    p:kioskUnresponsiveTime::timestamp_tz     as unresponsive_since,
    p:totalDocks::number                      as total_docks,
    p:docksAvailable::number                  as docks_available,
    p:bikesAvailable::number                  as bikes_available,
    p:classicBikesAvailable::number           as classic_bikes_available,
    p:smartBikesAvailable::number             as smart_bikes_available,
    p:electricBikesAvailable::number          as electric_bikes_available,
    -- capacity minus (bikes + free docks) = docks out of service or reserved
    greatest(p:totalDocks::number - p:bikesAvailable::number - p:docksAvailable::number, 0)
                                              as docks_disabled,
    p:isVirtual::boolean                      as is_virtual,
    p:isEventBased::boolean                   as is_event_based,
    p:addressCity::varchar                    as address_city,
    p:latitude::float                         as latitude,
    p:longitude::float                        as longitude
from flat
