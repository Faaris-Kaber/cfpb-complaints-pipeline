{#
  Grain: one row per complaint. Incremental merge: each run picks up complaints
  whose latest version was loaded since the last run (new complaints plus
  status changes caught by the extractor's lookback window).
#}
{{
    config(
        materialized='incremental',
        unique_key='complaint_id',
        incremental_strategy='merge',
        cluster_by=['date_received_key'],
        on_schema_change='append_new_columns',
    )
}}

with complaints as (

    select * from {{ ref('int_complaints__enriched') }}
    {% if is_incremental() %}
        -- A status change always arrives as a newly loaded raw row, so this also
        -- picks up close dates the snapshot records in the same run.
        where loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

)

select
    complaint_id,

    -- Foreign keys
    date_received                                                      as date_received_key,
    {{ dbt_utils.generate_surrogate_key(['parent_company']) }}         as company_key,
    {{ dbt_utils.generate_surrogate_key(['product', 'sub_product']) }} as product_key,
    {{ dbt_utils.generate_surrogate_key(['issue', 'sub_issue']) }}     as issue_key,
    {{ dbt_utils.generate_surrogate_key(['state', 'zip3']) }}          as geography_key,
    {{ dbt_utils.generate_surrogate_key(['submitted_via']) }}          as channel_key,

    -- Degenerate dimensions
    company                                                            as company_legal_name,
    company_response,
    company_public_response,
    resolution_category,

    -- Flags
    is_timely_response,
    is_monetary_relief,
    resolution_category = 'relief'                                     as is_relief,
    resolution_category = 'in_progress'                                as is_in_progress,
    is_servicemember,
    is_older_american,
    is_consumer_disputed,

    -- Measures
    date_sent_to_company,
    days_to_forward,
    observed_closed_on,
    days_to_close_observed,

    loaded_at
from complaints
