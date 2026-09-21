{{ config(severity='warn') }}

{#
  Catches a silently broken ingest: flags any of the last 3 complete days whose
  complaint count fell more than 3 standard deviations below the prior 30-day
  mean. Returns the offending days (a passing test returns no rows).
#}
with daily as (

    select
        date_received,
        count(*) as complaints
    from {{ ref('stg_cfpb__complaints') }}
    where date_received >= dateadd('day', -40, current_date())
        and date_received < current_date()
    group by 1

),

scored as (

    select
        date_received,
        complaints,
        avg(complaints) over (order by date_received rows between 30 preceding and 1 preceding)    as mean_30d,
        stddev(complaints) over (order by date_received rows between 30 preceding and 1 preceding) as sd_30d
    from daily

)

select *
from scored
where date_received >= dateadd('day', -3, current_date())
    and complaints < mean_30d - 3 * sd_30d
