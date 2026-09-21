{{ config(severity='warn') }}

-- The company_reference seed should cover most complaint volume. Warn if more
-- than 10% of complaints come from companies it does not map.
with totals as (

    select
        count(*)                  as complaints,
        count_if(not c.is_mapped) as unmapped
    from {{ ref('fct_complaints') }} as f
    inner join {{ ref('dim_company') }} as c on f.company_key = c.company_key

)

select *
from totals
where div0(unmapped, complaints) > 0.10
