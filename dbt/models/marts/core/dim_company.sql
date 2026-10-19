-- Grain: one row per raw legal-entity name as it appears in CFPB data, with the
-- parent brand and company type as attributes. Keying on the legal name (not the
-- parent) means remapping a company in the company_reference seed only changes
-- attributes here; fct_complaints rows outside the incremental window keep valid keys.
with companies as (

    select * from {{ ref('int_companies__normalized') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['company']) }} as company_key,
    company                                             as legal_name,
    parent_company,
    company_type,
    is_bank,
    is_mapped
from companies
