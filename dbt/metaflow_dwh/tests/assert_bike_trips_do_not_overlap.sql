{{ config(severity="warn") }}
-- A bike cannot start a new trip before its previous trip ended.
select *
from (
    select
        trip_key, bike_id, started_at_local,
        lag(ended_at_local) over (partition by bike_id order by started_at_local) as prev_ended_at
    from {{ ref('fct_trips') }}
    where bike_id is not null
)
where started_at_local < prev_ended_at
