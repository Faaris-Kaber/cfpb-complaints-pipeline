{# Trim a VARIANT/string field and turn blanks and placeholder values into NULL. #}
{% macro clean_string(expr) -%}
    nullif(nullif(nullif(trim({{ expr }}::string), ''), 'None'), 'N/A')
{%- endmacro %}

{# Yes/No text -> boolean. Anything else (including NULL) -> NULL. #}
{% macro yes_no_to_bool(expr) -%}
    case lower(trim({{ expr }}::string))
        when 'yes' then true
        when 'no' then false
    end
{%- endmacro %}
