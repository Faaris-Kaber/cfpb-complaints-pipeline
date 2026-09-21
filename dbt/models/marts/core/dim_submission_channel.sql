with channels as (

    select distinct submitted_via
    from {{ ref('int_complaints__enriched') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['submitted_via']) }} as channel_key,
    coalesce(submitted_via, 'Unknown')                        as channel,
    submitted_via in ('Web', 'Email')                         as is_digital
from channels
