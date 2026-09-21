-- Grain: state x month. Population lets the dashboard show complaints per 100k.
select
    g.state,
    g.state_name,
    g.census_region,
    g.state_population_2020,
    d.month_start       as month,
    count(*)            as complaints,
    count_if(c.is_bank) as bank_complaints
from {{ ref('fct_complaints') }} as f
inner join {{ ref('dim_geography') }} as g on f.geography_key = g.geography_key
inner join {{ ref('dim_date') }} as d on f.date_received_key = d.date_key
inner join {{ ref('dim_company') }} as c on f.company_key = c.company_key
where g.is_known_state
group by all
