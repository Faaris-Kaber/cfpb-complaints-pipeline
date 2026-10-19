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
    """Build a `?, ?, ...` placeholder string for an IN clause."""
    return ", ".join(["?"] * len(values)), tuple(values)


def pct_change(new: float, old: float) -> str:
    """Signed percentage, without the confusing "-0.0%" for tiny changes."""
    if not old:
        return "n/a"
    change = new / old - 1
    return "0.0%" if abs(change) < 0.0005 else f"{change:+.1%}"


# ---------------------------------------------------------------- filters
months = query("select distinct month from rpt_company_monthly order by month")["month"].tolist()
# The current month is still filling up; trends and MoM use complete months only.
month_to_date = date.today().replace(day=1)
default_start = months[max(0, len(months) - 25)]

st.sidebar.header("Filters")
# Whole months only: every section filters on month boundaries, so they always agree.
start, end = st.sidebar.select_slider(
    "Months",
    options=months,
    value=(default_start, months[-1]),
    format_func=lambda m: f"{m:%b %Y}",
)
banks_only = st.sidebar.toggle("Banks only", value=True)

companies = query(
    """
    select parent_company, sum(complaints) as complaints
    from rpt_company_monthly
    where month between ? and ? and (is_bank or not ?)
    group by 1 order by 2 desc
    """,
    (start, end, banks_only),
)
options = companies["parent_company"].tolist()
# Keep the user's picks across filter changes (dropping any no longer available).
if "companies" in st.session_state:
    st.session_state["companies"] = [c for c in st.session_state["companies"] if c in options]
else:
    st.session_state["companies"] = options[:10]
selected = st.sidebar.multiselect("Companies", options=options, key="companies")
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
    where month between ? and ? and parent_company in ({ph})
    """,
    base_params,
)

total = int(monthly["complaints"].sum())
closed = total - int(monthly["in_progress_count"].sum())
timely_pct = monthly["timely_responses"].sum() / total if total else 0
relief_pct = monthly["relief_count"].sum() / closed if closed else 0

complete = monthly[monthly["month"] < month_to_date]
complete_months = [m for m in months if start <= m <= end and m < month_to_date]
# Months with no complaints have no rows in the mart; count them as zero.
by_month = complete.groupby("month")["complaints"].sum().reindex(complete_months, fill_value=0)
if len(by_month) >= 2:
    mom_label = f"{by_month.index[-1]:%b %Y} vs prior month"
    mom = pct_change(by_month.iloc[-1], by_month.iloc[-2])
else:
    mom_label, mom = "Month vs prior", "n/a"

k1, k2, k3, k4 = st.columns(4)
k1.metric("Complaints", f"{total:,}")
k2.metric(mom_label, mom)
k3.metric("Timely response", f"{timely_pct:.1%}")
k4.metric("Closed with relief", f"{relief_pct:.1%}")

# ---------------------------------------------------------------- trends
st.subheader("Monthly complaints")
fig = px.line(
    complete.sort_values("month"),
    x="month",
    y="complaints",
    color="parent_company",
    labels={"month": "", "complaints": "Complaints", "parent_company": "Company"},
)
fig.update_layout(legend_title_text="", hovermode="x unified")
st.plotly_chart(fig, use_container_width=True)
st.caption(f"Complete months only; {month_to_date:%B %Y} is still in progress.")

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
closed_counts = (board["complaints"] - board["in_progress"]).replace(0, pd.NA)
board["relief_pct"] = board["relief"] / closed_counts
close_times = query(
    f"""
    select c.parent_company, median(f.days_to_close_observed) as median_days_to_close
    from fct_complaints f
    join dim_company c on f.company_key = c.company_key
    where f.date_received_key between ? and last_day(?) and c.parent_company in ({ph})
    group by 1
    """,
    base_params,
)
board = board.merge(close_times, on="parent_company", how="left").sort_values(
    "complaints", ascending=False
)
# Days to close only exists once the snapshot has seen complaints close.
has_close_data = board["closes_observed"].sum() > 0
board_cols = ["parent_company", "complaints", "timely_pct", "relief_pct"]
if has_close_data:
    board_cols += ["median_days_to_close", "closes_observed"]
st.dataframe(
    board[board_cols],
    hide_index=True,
    use_container_width=True,
    column_config={
        "parent_company": "Company",
        "complaints": st.column_config.NumberColumn("Complaints", format="localized"),
        "timely_pct": st.column_config.ProgressColumn(
            "Timely response", format="percent", min_value=0, max_value=1
        ),
        "relief_pct": st.column_config.ProgressColumn(
            "Relief rate", format="percent", min_value=0, max_value=1
        ),
        "median_days_to_close": st.column_config.NumberColumn(
            "Median days to close*", format="%.0f"
        ),
        "closes_observed": st.column_config.NumberColumn("Closes observed*", format="localized"),
    },
)
if has_close_data:
    st.caption(
        "*The public data has no close date. Days to close are measured from daily snapshots, "
        "so they only cover complaints the pipeline saw move from “In progress” to closed."
    )
else:
    st.caption(
        "Days to close: the public data has no close date, so the pipeline snapshots each "
        "complaint's status daily. This column appears once complaints it saw “In progress” "
        "start closing."
    )

# ---------------------------------------------------------------- product x company
st.subheader("Products by company")
heat = query(
    f"""
    select c.parent_company, p.product_group, count(*) as complaints
    from fct_complaints f
    join dim_company c on f.company_key = c.company_key
    join dim_product p on f.product_key = p.product_key
    where f.date_received_key between ? and last_day(?) and c.parent_company in ({ph})
    group by 1, 2
    """,
    base_params,
)
pivot = heat.pivot_table(
    index="parent_company", columns="product_group", values="complaints", fill_value=0
)
fig = px.imshow(
    pivot,
    aspect="auto",
    color_continuous_scale="Blues",
    labels={"color": "Complaints", "x": "", "y": ""},
)
# Cell labels: "23.1k" for large counts, plain integers below 1,000.
labels = pivot.map(lambda v: f"{v / 1000:.1f}k" if v >= 1000 else f"{v:.0f}")
fig.update_traces(text=labels.values, texttemplate="%{text}")
fig.update_layout(height=120 + 36 * len(pivot), xaxis_tickangle=-30)
st.plotly_chart(fig, use_container_width=True)

left, right = st.columns(2)

with left:
    st.subheader("Top issues")
    focus = st.selectbox("Company", selected)
    issues = query(
        """
        select i.issue, count(*) as complaints
        from fct_complaints f
        join dim_company c on f.company_key = c.company_key
        join dim_issue i on f.issue_key = i.issue_key
        where f.date_received_key between ? and last_day(?) and c.parent_company = ?
        group by 1 order by 2 desc limit 10
        """,
        (start, end, focus),
    )
    fig = px.bar(issues.sort_values("complaints"), x="complaints", y="issue", orientation="h")
    fig.update_layout(xaxis_title="Complaints", yaxis_title="")
    st.plotly_chart(fig, use_container_width=True)

with right:
    yoy_month = max((m for m in complete_months), default=None)
    st.subheader("Year over year by product")
    if yoy_month is None:
        st.info("Pick a range that includes a complete month.")
    else:
        yoy = query(
            """
            select product_group, complaints, complaints_prev_year, yoy_change_pct
            from rpt_product_trends
            where month = ? and complaints_prev_year > 0
            order by complaints desc
            limit 10
            """,
            (yoy_month,),
        )
        fig = px.bar(
            yoy.sort_values("yoy_change_pct"),
            x="yoy_change_pct",
            y="product_group",
            orientation="h",
            labels={
                "yoy_change_pct": f"{yoy_month:%b %Y} vs {yoy_month:%b} {yoy_month.year - 1}",
                "product_group": "",
            },
            hover_data={"complaints": ":,", "complaints_prev_year": ":,"},
        )
        fig.update_layout(xaxis_tickformat="+.0%")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Top 10 product groups by volume, across all companies.")

# ---------------------------------------------------------------- map
st.subheader("Complaints per 100k residents")
states = query(
    """
    select state, state_name,
           sum(iff(?, bank_complaints, complaints)) as complaints,
           max(state_population_2020) as population
    from rpt_state_monthly
    where month between ? and ?
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
