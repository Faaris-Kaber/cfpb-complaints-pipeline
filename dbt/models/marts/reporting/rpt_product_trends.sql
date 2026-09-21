-- Grain: product group x month, with year-over-year change.
with monthly as (

    select
        p.product_group,
        d.month_start as month,
        count(*)      as complaints
    from {{ ref('fct_complaints') }} as f
    inner join {{ ref('dim_product') }} as p on f.product_key = p.product_key
    inner join {{ ref('dim_date') }} as d on f.date_received_key = d.date_key
    group by all

)

select
    product_group,
    month,
    complaints,
    lag(complaints) over (partition by product_group order by month)     as complaints_prev_month,
    lag(complaints, 12) over (partition by product_group order by month) as complaints_prev_year,
    div0(complaints - complaints_prev_year, complaints_prev_year)        as yoy_change_pct
from monthly
