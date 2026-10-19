{#
  Incremental so each run only reads newly loaded raw rows instead of
  re-deduplicating the whole append-only raw table (which grows by ~30 days of
  re-pulled complaints every day). Load the bulk backfill before any API data.
#}
{{
    config(
        materialized='incremental',
        unique_key='complaint_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns',
    )
}}

with source as (

    select * from {{ source('cfpb', 'complaints') }}
    {% if is_incremental() %}
        where _loaded_at > (select coalesce(max(loaded_at), '1900-01-01'::timestamp_ntz) from {{ this }})
    {% endif %}

),

typed as (

    select
        try_to_number(record:complaint_id::string)                 as complaint_id,
        try_to_date(left(record:date_received::string, 10))        as date_received,
        try_to_date(left(record:date_sent_to_company::string, 10)) as date_sent_to_company,

        {{ clean_string("record:product") }}                       as product,
        {{ clean_string("record:sub_product") }}                   as sub_product,
        {{ clean_string("record:issue") }}                         as issue,
        {{ clean_string("record:sub_issue") }}                     as sub_issue,
        {{ clean_string("record:company") }}                       as company,
        -- CFPB spells out one territory instead of using its USPS code.
        case upper({{ clean_string("record:state") }})
            when 'UNITED STATES MINOR OUTLYING ISLANDS' then 'UM'
            else upper({{ clean_string("record:state") }})
        end                                                        as state,
        {{ clean_string("record:zip_code") }}                      as zip_code,
        {{ clean_string("record:submitted_via") }}                 as submitted_via,
        {{ clean_string("record:company_response") }}              as company_response,
        {{ clean_string("record:company_public_response") }}       as company_public_response,
        {{ clean_string("record:tags") }}                          as tags,
        {{ yes_no_to_bool("record:timely") }}                      as is_timely_response,

        -- Not in the current public API; kept so older bulk extracts still map.
        {{ yes_no_to_bool("record:consumer_disputed") }}           as is_consumer_disputed,

        try_to_timestamp_ntz(record:_extracted_at::string)         as extracted_at,
        record:_source::string                                     as extract_source,
        _file                                                      as source_file,
        _loaded_at                                                 as loaded_at

    from source

),

deduped as (

    -- Keep the most recently extracted version of each complaint in this batch.
    select *
    from typed
    where complaint_id is not null
    qualify row_number() over (
            partition by complaint_id
            order by extracted_at desc nulls last, loaded_at desc, source_file desc
        ) = 1

)

select
    complaint_id,
    date_received,
    date_sent_to_company,
    product,
    sub_product,
    issue,
    sub_issue,
    company,
    state,
    zip_code,
    left(zip_code, 3) as zip3,
    submitted_via,
    company_response,
    company_public_response,
    tags,
    is_timely_response,
    is_consumer_disputed,
    extracted_at,
    extract_source,
    source_file,
    loaded_at
from deduped
{% if is_incremental() %}
    -- A reloaded older partition must not replace a newer version already merged.
    where not exists (
            select 1
            from {{ this }} as existing
            where
                existing.complaint_id = deduped.complaint_id
                and existing.extracted_at > deduped.extracted_at
        )
{% endif %}
