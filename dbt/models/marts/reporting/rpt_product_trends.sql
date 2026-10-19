-- Grain: product group x month, with month-over-month and year-over-year comparisons.
-- Prior periods are joined by calendar month rather than lag(), so a product group
-- with a missing month still compares against the right month.
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
    cur.product_group,
    cur.month,
    cur.complaints,
    prev_month.complaints                                             as complaints_prev_month,
    prev_year.complaints                                              as complaints_prev_year,
    div0(cur.complaints - prev_year.complaints, prev_year.complaints) as yoy_change_pct
from monthly as cur
left join monthly as prev_month
    on
        cur.product_group = prev_month.product_group
        and prev_month.month = dateadd('month', -1, cur.month)
left join monthly as prev_year
    on
        cur.product_group = prev_year.product_group
        and prev_year.month = dateadd('year', -1, cur.month)
