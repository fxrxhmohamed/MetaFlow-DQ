with days as (
    select dateadd('day', row_number() over (order by seq4()) - 1, '2016-07-01'::date) as date_day
    from table(generator(rowcount => 4000))
)

select
    to_number(to_char(date_day, 'YYYYMMDD'))         as date_key,
    date_day,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter,
    year(date_day) || '-Q' || quarter(date_day)      as year_quarter,
    month(date_day)                                  as month,
    monthname(date_day)                              as month_name,
    dayofweekiso(date_day)                           as day_of_week,
    dayname(date_day)                                as day_name,
    dayofweekiso(date_day) >= 6                      as is_weekend
from days
where date_day <= dateadd('year', 1, current_date())
