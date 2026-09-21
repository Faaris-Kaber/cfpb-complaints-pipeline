{#
  prod: every model lands in the target schema (ANALYTICS.PROD), so the
        dashboard and REPORTER grants only need one schema.
  dev / CI: <target schema>_<custom schema>, e.g. DEV_FK_MARTS, PR_42_STAGING.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if target.name == 'prod' or custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- else -%}
        {{ target.schema | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
