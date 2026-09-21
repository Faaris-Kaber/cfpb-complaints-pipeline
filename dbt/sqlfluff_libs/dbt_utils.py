"""Stand-ins for dbt_utils macros so sqlfluff's jinja templater can render models
without a warehouse connection. Output only needs to be valid SQL, not exact."""


def generate_surrogate_key(field_list):
    return "md5(" + " || '-' || ".join(f"coalesce({f}::string, '')" for f in field_list) + ")"


def date_spine(datepart, start_date, end_date):
    return (
        f"select dateadd({datepart}, seq4(), {start_date}) as date_{datepart} "
        "from table(generator(rowcount => 10))"
    )
