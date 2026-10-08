-- Junk dimension: the (passholder_type, plan_duration) pairs that actually occur.
select distinct
    {{ dbt_utils.generate_surrogate_key(['passholder_type', 'plan_duration_days']) }} as rider_plan_key,
    passholder_type,
    plan_duration_days,
    case
        when passholder_type = 'Walk-up' or plan_duration_days in (0, 1) then 'Casual'
        when plan_duration_days >= 30 then 'Subscriber'
        else 'Other'
    end as rider_segment
from {{ ref('int_trips_validated') }}
