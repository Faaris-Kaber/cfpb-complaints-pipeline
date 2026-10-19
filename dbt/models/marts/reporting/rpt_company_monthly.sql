-- Grain: parent company x month. Feeds the dashboard leaderboard and trend lines.
with f as (

    select * from {{ ref('fct_complaints') }}

),

d as (

    select
        date_key,
        month_start
    from {{ ref('dim_date') }}

),

c as (

    select
        company_key,
        parent_company
    from {{ ref('dim_company') }}

),

-- dim_company is per legal entity; roll its attributes up to the parent brand.
parents as (

    select
        parent_company,
        max(company_type)   as company_type,
        boolor_agg(is_bank) as is_bank
    from {{ ref('dim_company') }}
    group by parent_company

)

select
    p.parent_company,
    p.company_type,
    p.is_bank,
    d.month_start                                               as month,
    count(*)                                                    as complaints,
    count_if(f.is_timely_response)                              as timely_responses,
    count_if(f.is_relief)                                       as relief_count,
    count_if(f.is_monetary_relief)                              as monetary_relief_count,
    count_if(f.is_in_progress)                                  as in_progress_count,
    div0(count_if(f.is_timely_response), count_if(f.is_timely_response is not null))
        as timely_pct,
    div0(count_if(f.is_relief), count_if(not f.is_in_progress)) as relief_pct,
    median(f.days_to_forward)                                   as median_days_to_forward,
    median(f.days_to_close_observed)                            as median_days_to_close_observed,
    count(f.days_to_close_observed)                             as closes_observed
from f
inner join d on f.date_received_key = d.date_key
inner join c on f.company_key = c.company_key
inner join parents as p on c.parent_company = p.parent_company
group by p.parent_company, p.company_type, p.is_bank, d.month_start
