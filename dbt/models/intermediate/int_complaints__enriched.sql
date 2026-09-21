with complaints as (

    select * from {{ ref('stg_cfpb__complaints') }}

),

companies as (

    select * from {{ ref('int_companies__normalized') }}

),

resolution as (

    select * from {{ ref('int_complaints__resolution_dates') }}

)

select
    c.complaint_id,
    c.date_received,
    c.date_sent_to_company,
    c.product,
    c.sub_product,
    c.issue,
    c.sub_issue,
    c.company,
    co.parent_company,
    co.company_type,
    co.is_bank,
    c.state,
    c.zip3,
    c.submitted_via,
    c.company_response,
    c.company_public_response,
    c.tags,
    c.is_timely_response,
    c.is_consumer_disputed,

    case
        when c.company_response in (
                'Closed with monetary relief',
                'Closed with non-monetary relief',
                'Closed with relief'
            ) then 'relief'
        when c.company_response = 'Closed with explanation' then 'explanation'
        when c.company_response in ('Closed without relief', 'Closed') then 'closed_no_relief'
        when c.company_response = 'Untimely response' then 'untimely'
        when c.company_response = 'In progress' then 'in_progress'
        else 'unknown'
    end                                                      as resolution_category,

    c.company_response = 'Closed with monetary relief'       as is_monetary_relief,
    coalesce(c.tags ilike '%Servicemember%', false)          as is_servicemember,
    coalesce(c.tags ilike '%Older American%', false)         as is_older_american,

    datediff('day', c.date_received, c.date_sent_to_company) as days_to_forward,

    r.observed_closed_on,
    datediff('day', c.date_received, r.observed_closed_on)   as days_to_close_observed,

    c.extracted_at,
    c.loaded_at

from complaints as c
left join companies as co
    on c.company = co.company
left join resolution as r
    on c.complaint_id = r.complaint_id
