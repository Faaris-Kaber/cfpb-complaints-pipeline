{#
  Estimate when each complaint closed, using the status snapshot.

  A complaint gets an observed close date only if we saw it "In progress" first
  and later saw it closed. Complaints that were already closed the first time we
  snapshotted them have no observable close date (NULL).
#}
with history as (

    select
        complaint_id,
        company_response,
        dbt_valid_from::date as observed_on
    from {{ ref('snap_complaints_status') }}

),

first_seen_open as (

    select
        complaint_id,
        min(observed_on) as first_open_on
    from history
    where company_response = 'In progress'
    group by 1

),

first_closed_after_open as (

    select
        h.complaint_id,
        min(h.observed_on) as observed_closed_on
    from history as h
    inner join first_seen_open as o
        on h.complaint_id = o.complaint_id
            and h.observed_on > o.first_open_on
    where h.company_response <> 'In progress'
    group by 1

)

select
    o.complaint_id,
    o.first_open_on,
    c.observed_closed_on
from first_seen_open as o
left join first_closed_after_open as c
    on o.complaint_id = c.complaint_id
