-- Grain: parent company (several raw legal-entity names can roll up to one parent).
with companies as (

    select * from {{ ref('int_companies__normalized') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['parent_company']) }} as company_key,
    parent_company,
    -- A parent's type comes from its mapped entities when any exist.
    coalesce(
        max(case when is_mapped then company_type end),
        'other'
    )                                                          as company_type,
    boolor_agg(is_bank)                                        as is_bank,
    boolor_agg(is_mapped)                                      as is_mapped,
    count(*)                                                   as legal_entity_count,
    listagg(company, ' | ') within group (order by company)    as legal_entity_names
from companies
group by parent_company
