"""CFPB bank complaints dashboard.

Reads the dbt marts in Snowflake (ANALYTICS.PROD) with a read-only role.
Run locally:  streamlit run dashboard/app.py
Credentials:  dashboard/.streamlit/secrets.toml (see secrets.toml.example)
"""

from __future__ import annotations

from datetime import date

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Bank Complaints Analytics", layout="wide")

conn = st.connection("snowflake")
CACHE_TTL = 3600


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Run a query (cached by Streamlit for CACHE_TTL seconds); lower-case column names."""
    df = conn.query(sql, params=params, ttl=CACHE_TTL)
    df.columns = [c.lower() for c in df.columns]
    return df


def in_list(values: list[str]) -> tuple[str, tuple]:
    """Build a `%s, %s, ...` placeholder string for an IN clause."""
    return ", ".join(["%s"] * len(values)), tuple(values)


# ---------------------------------------------------------------- filters
bounds = query("select min(month) as lo, max(month) as hi from rpt_company_monthly")
lo, hi = bounds.loc[0, "lo"], bounds.loc[0, "hi"]
default_start = max(lo, date(hi.year - 2, hi.month, 1))

st.sidebar.header("Filters")
start, end = st.sidebar.slider(
    "Months", min_value=lo, max_value=hi, value=(default_start, hi), format="MMM YYYY"
)
banks_only = st.sidebar.toggle("Banks only", value=True)

companies = query(
    """
    select parent_company, sum(complaints) as complaints
    from rpt_company_monthly
    where month between %s and %s and (is_bank or not %s)
    group by 1 order by 2 desc
    """,
    (start, end, banks_only),
)
selected = st.sidebar.multiselect(
    "Companies",
    options=companies["parent_company"].tolist(),
    default=companies["parent_company"].head(10).tolist(),
)
if not selected:
    st.info("Pick at least one company in the sidebar.")
    st.stop()

ph, sel = in_list(selected)
base_params = (start, end, *sel)

# ---------------------------------------------------------------- KPIs
st.title("Bank Complaints Analytics")
st.caption(
    "Source: CFPB Consumer Complaint Database. Refreshed daily by Airflow; modeled with dbt "
    "in Snowflake."
)

monthly = query(
    f"""
    select month, parent_company, complaints, timely_responses, relief_count,
           in_progress_count, median_days_to_close_observed, closes_observed
    from rpt_company_monthly
    where month between %s and %s and parent_company in ({ph})
    """,
    base_params,
)

total = int(monthly["complaints"].sum())
closed = total - int(monthly["in_progress_count"].sum())
timely_pct = monthly["timely_responses"].sum() / total if total else 0
relief_pct = monthly["relief_count"].sum() / closed if closed else 0
by_month = monthly.groupby("month", as_index=False)["complaints"].sum().sort_values("month")
mom = (
    (by_month["complaints"].iloc[-1] / by_month["complaints"].iloc[-2] - 1)
    if len(by_month) >= 2 and by_month["complaints"].iloc[-2]
    else None
)

k1, k2, k3, k4 = st.columns(4)
k1.metric("Complaints", f"{total:,}")
k2.metric("Latest month vs prior", f"{mom:+.1%}" if mom is not None else "n/a")
k3.metric("Timely response", f"{timely_pct:.1%}")
k4.metric("Closed with relief", f"{relief_pct:.1%}")

# ---------------------------------------------------------------- trends
st.subheader("Monthly complaints")
fig = px.line(
    monthly.sort_values("month"),
    x="month",
    y="complaints",
    color="parent_company",
    labels={"month": "", "complaints": "Complaints", "parent_company": "Company"},
)
fig.update_layout(legend_title_text="", hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- leaderboard
st.subheader("Company leaderboard")
board = (
    monthly.groupby("parent_company")
    .agg(
        complaints=("complaints", "sum"),
        timely=("timely_responses", "sum"),
        relief=("relief_count", "sum"),
        in_progress=("in_progress_count", "sum"),
        closes_observed=("closes_observed", "sum"),
    )
    .reset_index()
)
board["timely_pct"] = board["timely"] / board["complaints"]
board["relief_pct"] = board["relief"] / (board["complaints"] - board["in_progress"])
close_times = query(
    f"""
    select c.parent_company, median(f.days_to_close_observed) as median_days_to_close
    from fct_complaints f
    join dim_company c on f.company_key = c.company_key
    where f.date_received_key between %s and last_day(%s) and c.parent_company in ({ph})
    group by 1
    """,
    base_params,
)
board = board.merge(close_times, on="parent_company", how="left").sort_values(
    "complaints", ascending=False
)
st.dataframe(
    board[
        [
            "parent_company",
            "complaints",
            "timely_pct",
            "relief_pct",
            "median_days_to_close",
            "closes_observed",
        ]
    ],
    hide_index=True,
    use_container_width=True,
    column_config={
        "parent_company": "Company",
        "complaints": st.column_config.NumberColumn("Complaints", format="%d"),
        "timely_pct": st.column_config.ProgressColumn(
            "Timely response", format="percent", min_value=0, max_value=1
        ),
        "relief_pct": st.column_config.ProgressColumn(
            "Relief rate", format="percent", min_value=0, max_value=1
        ),
        "median_days_to_close": st.column_config.NumberColumn(
            "Median days to close*", format="%.0f"
        ),
        "closes_observed": st.column_config.NumberColumn("Closes observed*", format="%d"),
    },
)
st.caption(
    "*The public data has no close date. Days to close are measured from daily snapshots, "
    "so they only cover complaints the pipeline saw move from “In progress” to closed."
)

# ---------------------------------------------------------------- product x company
left, right = st.columns(2)

with left:
    st.subheader("Products by company")
    heat = query(
        f"""
        select c.parent_company, p.product_group, count(*) as complaints
        from fct_complaints f
        join dim_company c on f.company_key = c.company_key
        join dim_product p on f.product_key = p.product_key
        where f.date_received_key between %s and last_day(%s) and c.parent_company in ({ph})
        group by 1, 2
        """,
        base_params,
    )
    pivot = heat.pivot_table(
        index="parent_company", columns="product_group", values="complaints", fill_value=0
    )
    fig = px.imshow(
        pivot, aspect="auto", color_continuous_scale="Blues", labels={"color": "Complaints"}
    )
    fig.update_layout(xaxis_title="", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

with right:
    st.subheader("Top issues")
    focus = st.selectbox("Company", selected)
    issues = query(
        """
        select i.issue, count(*) as complaints
        from fct_complaints f
        join dim_company c on f.company_key = c.company_key
        join dim_issue i on f.issue_key = i.issue_key
        where f.date_received_key between %s and last_day(%s) and c.parent_company = %s
        group by 1 order by 2 desc limit 10
        """,
        (start, end, focus),
    )
    fig = px.bar(issues.sort_values("complaints"), x="complaints", y="issue", orientation="h")
    fig.update_layout(xaxis_title="Complaints", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------- map
st.subheader("Complaints per 100k residents")
states = query(
    """
    select state, state_name,
           sum(iff(%s, bank_complaints, complaints)) as complaints,
           max(state_population_2020) as population
    from rpt_state_monthly
    where month between %s and %s
    group by 1, 2
    """,
    (banks_only, start, end),
)
states["per_100k"] = states["complaints"] / states["population"] * 100_000
fig = px.choropleth(
    states,
    locations="state",
    locationmode="USA-states",
    color="per_100k",
    scope="usa",
    hover_name="state_name",
    hover_data={"complaints": ":,", "per_100k": ":.1f", "state": False},
    color_continuous_scale="Blues",
    labels={"per_100k": "per 100k"},
)
st.plotly_chart(fig, use_container_width=True)
st.caption("Map covers all companies (or all banks with the toggle on), not just the selection.")
