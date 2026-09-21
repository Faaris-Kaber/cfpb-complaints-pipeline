with issues as (

    select distinct
        issue,
        sub_issue
    from {{ ref('int_complaints__enriched') }}

)

select
    {{ dbt_utils.generate_surrogate_key(['issue', 'sub_issue']) }} as issue_key,
    coalesce(issue, 'Unknown')                                     as issue,
    sub_issue
from issues
