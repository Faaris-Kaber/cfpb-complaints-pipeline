{#
  Drop every schema in the target database that starts with `prefix`
  (e.g. PR_42, PR_42_STAGING, PR_42_MARTS). Used by CI to clean up.
  dbt run-operation drop_pr_schemas --args "{prefix: 'PR_42'}"
#}
{% macro drop_pr_schemas(prefix) %}
    {% if not prefix or not prefix.upper().startswith('PR_') %}
        {{ exceptions.raise_compiler_error("drop_pr_schemas only drops PR_* schemas, got: " ~ prefix) }}
    {% endif %}

    {% set find_sql %}
        select schema_name
        from {{ target.database }}.information_schema.schemata
        where schema_name ilike '{{ prefix }}%'
    {% endset %}

    {% for schema in run_query(find_sql).columns[0].values() %}
        {% do log("dropping " ~ target.database ~ "." ~ schema, info=True) %}
        {% do run_query("drop schema if exists " ~ target.database ~ "." ~ schema ~ " cascade") %}
    {% endfor %}
{% endmacro %}
