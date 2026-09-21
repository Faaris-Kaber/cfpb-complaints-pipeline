{#
  Track every change to a complaint's resolution status. The public data has no
  "closed date", so the first snapshot row where the status leaves
  "In progress" is the best available estimate of when it closed.
  dbt_valid_from has daily resolution (the snapshot runs once a day).
#}
{% snapshot snap_complaints_status %}

{{
    config(
        unique_key='complaint_id',
        strategy='check',
        check_cols=['company_response', 'is_timely_response', 'company_public_response'],
        invalidate_hard_deletes=False,
    )
}}

select
    complaint_id,
    date_received,
    company_response,
    is_timely_response,
    company_public_response
from {{ ref('stg_cfpb__complaints') }}

{% endsnapshot %}
