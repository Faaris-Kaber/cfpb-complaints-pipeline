{{ config(severity='warn') }}

{#
  Catches a silently broken ingest: flags any day 15-21 days ago whose complaint
  count is under half the average for the same weekday over the 4 prior weeks.
  - Same-weekday baseline: Sunday volume is about a fifth of a weekday's.
  - 15+ days back: the CFPB publishes complaints with a lag of up to ~15 days, so
    newer days are always incomplete (about 60% at one week old).
  - Calendar spine: a day with no complaints at all counts as 0 rather than
    dropping out of the check.
  Returns the offending days (a passing test returns no rows).
#}
with spine as (

    select date_day
    from {{ ref('dim_date') }}
    where date_day between dateadd('day', -49, current_date()) and dateadd('day', -15, current_date())

),

daily as (

    select
        date_received,
        count(*) as complaints
    from {{ ref('stg_cfpb__complaints') }}
    where date_received between dateadd('day', -49, current_date()) and dateadd('day', -15, current_date())
    group by date_received

),

scored as (

    select
        spine.date_day,
        coalesce(daily.complaints, 0) as complaints,
        avg(coalesce(daily.complaints, 0)) over (
            partition by dayofweekiso(spine.date_day)
            order by spine.date_day
            rows between 4 preceding and 1 preceding
        )                             as same_weekday_avg
    from spine
    left join daily on spine.date_day = daily.date_received

)

select *
from scored
where
    date_day >= dateadd('day', -21, current_date())
    and complaints < 0.5 * same_weekday_avg
