-- One row per raw company name, mapped to a parent brand.
with companies as (

    select distinct company
    from {{ ref('stg_cfpb__complaints') }}
    where company is not null

),

reference as (

    select * from {{ ref('company_reference') }}

)

select
    c.company,
    coalesce(r.parent_company, c.company)    as parent_company,
    coalesce(r.company_type, 'other')        as company_type,
    coalesce(r.company_type = 'bank', false) as is_bank,
    r.company is not null                    as is_mapped
from companies as c
left join reference as r
    on c.company = r.company
