with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" ~ var('date_spine_start') ~ "'::date",
        end_date="dateadd('year', 1, current_date())"
    ) }}

)

select
    date_day::date                      as date_key,
    date_day::date                      as date_day,
    year(date_day)                      as year,
    quarter(date_day)                   as quarter,
    month(date_day)                     as month,
    monthname(date_day)                 as month_name,
    date_trunc('month', date_day)::date as month_start,
    date_trunc('week', date_day)::date  as week_start,
    dayofweekiso(date_day)              as day_of_week,
    dayname(date_day)                   as day_name,
    dayofweekiso(date_day) in (6, 7)    as is_weekend
from spine
