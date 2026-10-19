"""CFPB bank complaints dashboard.

Reads the dbt marts in Snowflake (ANALYTICS.PROD) with a read-only role.
Run locally:  streamlit run dashboard/app.py   (from the repo root, so the theme in
              .streamlit/config.toml applies)
Credentials:  dashboard/.streamlit/secrets.toml (see secrets.toml.example)
"""

from __future__ import annotations

import textwrap
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

st.set_page_config(
    page_title="CFPB Bank Complaints",
    layout="wide",
    initial_sidebar_state="expanded",
)

conn = st.connection("snowflake")
CACHE_TTL = 3600
MAX_COMPANIES = 8  # one per categorical color; more would need generated hues


def query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    """Run a query (cached by Streamlit for CACHE_TTL seconds); lower-case column names."""
    df = conn.query(sql, params=params, ttl=CACHE_TTL)
    df.columns = [c.lower() for c in df.columns]
    return df


def in_list(values: list[str]) -> tuple[str, tuple]:
    """Build a `?, ?, ...` placeholder string for an IN clause."""
    return ", ".join(["?"] * len(values)), tuple(values)


# ---------------------------------------------------------------- design tokens
# Light and dark steps of the same validated palette (see .streamlit/config.toml).
# Chart chrome (grid, axes, text) uses theme-neutral values, so it reads correctly
# even when Streamlit reports the theme late or the user switches it.
DARK = getattr(getattr(st.context, "theme", None), "type", "light") == "dark"
SERIES = (
    ["#3987e5", "#d95926", "#199e70", "#c98500", "#d55181", "#008300", "#9085e9", "#e66767"]
    if DARK
    else ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
)
# Sequential blue; near-zero recedes into the surface in both themes.
SEQ = (
    ["#162a45", "#184f95", "#256abf", "#3987e5", "#6da7ec", "#b7d3f6"]
    if DARK
    else ["#eef4fc", "#cde2fb", "#9ec5f4", "#5598e7", "#256abf", "#0d366b"]
)
COLORSCALE = [[i / (len(SEQ) - 1), c] for i, c in enumerate(SEQ)]
# Year-over-year polarity: red = more complaints, neutral gray = fewer. Neither is a
# series color, so they can't be mistaken for a company.
UP, DOWN = ("#e05a6f", "#6f6e69") if DARK else ("#c4314b", "#8a8984")
GRID = "rgba(128,128,128,0.18)"
AXIS = "rgba(128,128,128,0.45)"
TICK = "#a3a29c" if DARK else "#5f5e59"  # >= 4.5:1 on the page in each theme
FONT = "Inter, system-ui, -apple-system, 'Segoe UI', sans-serif"
PLOTLY_CONFIG = {"displayModeBar": False, "responsive": True}

st.html(
    """
    <style>
    .stMainBlockContainer {padding-top: 2.25rem; padding-bottom: 3rem; max-width: 1440px;}
    [data-testid="stSidebarUserContent"] {padding-top: 1.25rem;}
    [data-testid="stMetricValue"] {font-size: 1.7rem; font-weight: 600; letter-spacing: -0.02em;}
    [data-testid="stMetricLabel"] p {font-size: 0.8rem; opacity: 0.75;}
    .st-key-company_picker button {justify-content: space-between; text-align: left;
        font-weight: 400;}
    .card-title {font-weight: 600; font-size: 15px; line-height: 1.3;}
    .card-note {font-size: 13px; opacity: 0.65; margin-top: 2px; line-height: 1.4;}
    .sb-label {font-size: 11px; font-weight: 600; letter-spacing: 0.06em;
        text-transform: uppercase; opacity: 0.6; margin: 0.75rem 0 0.25rem 0;}
    </style>
    """
)


def style(fig: go.Figure, height: int, legend: bool = False) -> go.Figure:
    """Shared chart chrome: transparent surface, hairline grid, no axis rules."""
    fig.update_layout(
        height=height,
        margin={"l": 4, "r": 12, "t": 36 if legend else 8, "b": 4},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font={"family": FONT, "size": 12},
        showlegend=legend,
        legend={"orientation": "h", "x": 0, "y": 1.02, "yanchor": "bottom", "title": None},
        hoverlabel={"font": {"family": FONT, "size": 12}},
    )
    axis = {"showline": False, "ticks": "", "tickfont": {"color": TICK}, "title": None}
    fig.update_xaxes(showgrid=False, zeroline=False, **axis)
    fig.update_yaxes(gridcolor=GRID, zeroline=False, **axis)
    return fig


def card_header(title: str, note: str | None = None, table: pd.DataFrame | None = None) -> None:
    """One-block card title + note, with an optional "Data" table view on the right."""
    left, right = st.columns([6, 1], vertical_alignment="top")
    note_html = f"<div class='card-note'>{note}</div>" if note else ""
    left.markdown(f"<div class='card-title'>{title}</div>{note_html}", unsafe_allow_html=True)
    if table is not None:
        with (
            right,
            st.container(horizontal=True, horizontal_alignment="right"),
            st.popover("Data", type="tertiary", icon=":material/table:"),
        ):
            st.dataframe(table, hide_index=True, width="stretch")


def fmt_day(d: date) -> str:
    return f"{d:%b} {d.day}, {d:%Y}"


def month_label(m: date) -> str:
    return f"{m:%b %Y}"


# ---------------------------------------------------------------- filters
months = query("select distinct month from rpt_company_monthly order by month")["month"].tolist()
if not months:
    st.warning("No data loaded yet.")
    st.stop()
latest_day = pd.Timestamp(
    query("select max(date_received_key) as d from fct_complaints")["d"].iloc[0]
).date()
# Months before the one holding the latest data are complete. Based on the data, not
# the server clock, so a late load can't make a partial month look complete.
month_to_date = latest_day.replace(day=1)

with st.sidebar:
    st.markdown("<div class='sb-label'>Period</div>", unsafe_allow_html=True)
    custom = st.toggle("Custom range", key="period_custom")
    preset = st.segmented_control(
        "Period",
        ["12M", "24M", "YTD", "All"],
        default="24M",
        required=True,
        key="period_preset",
        label_visibility="collapsed",
        width="stretch",
        disabled=custom,
    )
    end_month = months[-1]
    if custom:
        c1, c2 = st.columns(2)
        start = c1.selectbox(
            "From",
            months,
            index=max(0, len(months) - 25),
            format_func=month_label,
            key="period_from",
        )
        end = c2.selectbox(
            "To", months, index=len(months) - 1, format_func=month_label, key="period_to"
        )
        if end < start:
            start, end = end, start
    elif preset == "YTD":
        start, end = date(end_month.year, 1, 1), end_month
    else:
        span = {"12M": 12, "24M": 24}.get(preset, len(months))
        start, end = months[max(0, len(months) - span)], end_month

    st.markdown("<div class='sb-label'>Company type</div>", unsafe_allow_html=True)
    scope = st.segmented_control(
        "Company type",
        ["Banks", "All companies"],
        default="Banks",
        required=True,
        key="scope",
        label_visibility="collapsed",
        width="stretch",
        on_change=lambda: st.session_state.update(cleared=False),
    )
    banks_only = scope == "Banks"

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
    volume = dict(zip(companies["parent_company"], companies["complaints"], strict=True))

    def pick_key(name: str) -> str:
        return f"pick::{name}"

    def set_picks(names: list[str]) -> None:
        for name in options:
            st.session_state[pick_key(name)] = name in names
        st.session_state["cleared"] = not names

    def enforce_cap(key: str) -> None:
        # Fast clicks can land before the rerun disables boxes; undo any 9th tick.
        if sum(bool(st.session_state.get(pick_key(o))) for o in options) > MAX_COMPANIES:
            st.session_state[key] = False

    # Re-seed when filters leave nothing picked (unless the user cleared on purpose).
    nothing_picked = not any(st.session_state.get(pick_key(n)) for n in options)
    if nothing_picked and not st.session_state.get("cleared"):
        set_picks(options[:5])

    selected = [n for n in options if st.session_state.get(pick_key(n))][:MAX_COMPANIES]
    if not selected:
        summary = "Select companies"
    elif len(selected) == 1:
        summary = selected[0]
    else:
        summary = f"{len(selected)} companies"

    st.markdown("<div class='sb-label'>Companies</div>", unsafe_allow_html=True)
    with (
        st.container(key="company_picker"),
        st.popover(summary, width="stretch", icon=":material/apartment:"),
    ):
        b1, b2, b3 = st.columns(3, gap="small")
        b1.button("Top 5", on_click=set_picks, args=(options[:5],), type="tertiary")
        b2.button("Top 8", on_click=set_picks, args=(options[:MAX_COMPANIES],), type="tertiary")
        b3.button("Clear", on_click=set_picks, args=([],), type="tertiary")
        st.caption(f"Up to {MAX_COMPANIES}, ranked by complaints in the period.")
        with st.container(height=320, border=False):
            for name in options:
                st.checkbox(
                    f"{name} :gray[{volume[name]:,}]",
                    key=pick_key(name),
                    disabled=len(selected) >= MAX_COMPANIES and name not in selected,
                    on_change=enforce_cap,
                    args=(pick_key(name),),
                )
    if len(selected) > 1:
        st.caption(", ".join(selected))

    st.divider()
    st.caption(
        "Source: CFPB Consumer Complaint Database, loaded daily by Airflow into Snowflake "
        "and modeled with dbt."
    )

if not selected:
    st.info("Pick at least one company under **Companies** in the sidebar.")
    st.stop()

# A company keeps its color while it stays selected (color follows the entity, never
# its rank), so changing the selection never repaints the remaining lines.
slots: dict[str, int] = st.session_state.setdefault("color_slots", {})
for name in list(slots):
    if name not in selected:
        del slots[name]
for name in selected:
    free = sorted(set(range(MAX_COMPANIES)) - set(slots.values()))
    if name not in slots and free:
        slots[name] = free[0]
color_of = {name: SERIES[slots[name]] for name in selected if name in slots}

ph, sel = in_list(selected)
base_params = (start, end, *sel)

# ---------------------------------------------------------------- data
monthly = query(
    f"""
    select month, parent_company, complaints, timely_responses, relief_count,
           in_progress_count, median_days_to_close_observed, closes_observed
    from rpt_company_monthly
    where month between ? and ? and parent_company in ({ph})
    """,
    base_params,
)
complete_months = [m for m in months if start <= m <= end and m < month_to_date]
complete = monthly[monthly["month"] < month_to_date]
# The mart has no row for a company-month with zero complaints; fill those with 0 so
# lines don't bridge the gap and totals count the month.
wide = (
    complete.pivot_table(
        index="month", columns="parent_company", values="complaints", aggfunc="sum"
    )
    .reindex(index=complete_months, columns=selected)
    .fillna(0)
)
by_month = wide.sum(axis=1)

total = int(monthly["complaints"].sum())
closed = total - int(monthly["in_progress_count"].sum())
timely_pct = monthly["timely_responses"].sum() / total if total else 0
relief_pct = monthly["relief_count"].sum() / closed if closed else 0
rates = complete.groupby("month")[
    ["complaints", "timely_responses", "relief_count", "in_progress_count"]
].sum()
timely_trend = (100 * rates["timely_responses"] / rates["complaints"]).tail(12)
closed_by_month = (rates["complaints"] - rates["in_progress_count"]).where(lambda s: s > 0)
relief_trend = (100 * rates["relief_count"] / closed_by_month).dropna().tail(12)

# ---------------------------------------------------------------- header + KPIs
head_l, head_r = st.columns([3, 1], vertical_alignment="bottom")
head_l.title("Consumer complaints by " + ("bank" if banks_only else "company"))
head_l.caption(
    f"{month_label(start)} – {month_label(end)} · {len(selected)} "
    f"{'company' if len(selected) == 1 else 'companies'} · "
    + ("Banks" if banks_only else "All company types")
)
with head_r, st.container(horizontal=True, horizontal_alignment="right"):
    st.badge(f"Data through {fmt_day(latest_day)}", icon=":material/schedule:", color="gray")

k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "Complaints",
    f"{total:,}",
    border=True,
    height="stretch",
    chart_data=by_month.tail(12).tolist() or None,
    chart_type="line",
    help="Selected companies and period. Trend: last 12 complete months.",
)
if len(by_month) >= 1:
    last = by_month.iloc[-1]
    prev = by_month.iloc[-2] if len(by_month) >= 2 else None
    change = (last / prev - 1) if prev else None
    k2.metric(
        "Latest complete month",
        f"{int(last):,}",
        delta=None if change is None else f"{change:+.1%}",
        # Streamlit paints a metric's chart in its delta color; keep both neutral so the
        # bars don't read as an alarm. The arrow still shows the direction.
        delta_color="off",
        delta_description=None
        if change is None
        else f"{by_month.index[-1]:%b} vs {by_month.index[-2]:%b}",
        border=True,
        height="stretch",
        chart_data=by_month.tail(12).tolist(),
        chart_type="bar",
        help=f"Complaints received in {by_month.index[-1]:%B %Y}; bars are the last 12 months.",
    )
else:
    k2.metric("Latest complete month", "–", border=True, height="stretch")
k3.metric(
    "Timely response",
    f"{timely_pct:.1%}",
    border=True,
    height="stretch",
    chart_data=timely_trend.tolist() or None,
    chart_type="line",
    help="Share of complaints the company answered within 15 days.",
)
k4.metric(
    "Closed with relief",
    f"{relief_pct:.1%}",
    border=True,
    height="stretch",
    chart_data=relief_trend.tolist() or None,
    chart_type="line",
    help="Share of closed complaints resolved with monetary or non-monetary relief.",
)

overview, products, geography = st.tabs(["Overview", "Products & issues", "Geography"])

# ---------------------------------------------------------------- overview
with overview:
    with st.container(border=True, gap="small"):
        table = wide.copy()
        table.index = [month_label(m) for m in table.index]
        card_header(
            "Monthly complaints",
            f"Complete months only; {month_to_date:%B %Y} is still in progress.",
            table.reset_index(names="Month"),
        )
        if not complete_months:
            st.info("This period has no complete month yet. Widen the period to see the trend.")
        else:
            fig = go.Figure()
            for name in selected:
                fig.add_trace(
                    go.Scatter(
                        x=wide.index,
                        y=wide[name],
                        name=name,
                        mode="lines+markers" if len(complete_months) <= 3 else "lines",
                        line={"color": color_of.get(name), "width": 2},
                        marker={"size": 6},
                        hovertemplate="%{y:,}<extra>" + name + "</extra>",
                    )
                )
            style(fig, height=360, legend=True)
            fig.update_layout(hovermode="x unified")
            fig.update_yaxes(tickformat="~s", rangemode="tozero")
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

    with st.container(border=True, gap="small"):
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
        board["timely_pct"] = 100 * board["timely"] / board["complaints"]
        closed_counts = (board["complaints"] - board["in_progress"]).where(lambda s: s > 0)
        board["relief_pct"] = 100 * board["relief"] / closed_counts
        board["in_progress_pct"] = 100 * board["in_progress"] / board["complaints"]
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
        cols = ["parent_company", "complaints", "timely_pct", "relief_pct", "in_progress_pct"]
        if has_close_data:
            cols += ["median_days_to_close"]
        card_header(
            "Company scorecard",
            "Sorted by complaints. Relief rate counts closed complaints only."
            + ("" if has_close_data else " Days to close appears as tracked complaints close."),
        )
        # Company names wear their chart color, tying each row to its line.
        styled = board[cols].style.map(
            lambda v: f"color: {color_of.get(v, 'inherit')}; font-weight: 500",
            subset=["parent_company"],
        )
        st.dataframe(
            styled,
            hide_index=True,
            width="stretch",
            column_config={
                "parent_company": st.column_config.TextColumn("Company"),
                "complaints": st.column_config.NumberColumn("Complaints", format="localized"),
                "timely_pct": st.column_config.NumberColumn("Timely response", format="%.1f%%"),
                "relief_pct": st.column_config.ProgressColumn(
                    "Closed with relief", format="%.1f%%", min_value=0, max_value=100
                ),
                "in_progress_pct": st.column_config.NumberColumn(
                    "Still in progress", format="%.1f%%"
                ),
                "median_days_to_close": st.column_config.NumberColumn(
                    "Median days to close", format="%.0f"
                ),
            },
        )

# ---------------------------------------------------------------- products & issues
with products:
    with st.container(border=True, gap="small"):
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
        counts = heat.pivot_table(
            index="product_group", columns="parent_company", values="complaints", fill_value=0
        )
        counts = counts.reindex(columns=[c for c in selected if c in counts.columns])
        counts = counts.loc[counts.sum(axis=1).sort_values(ascending=False).index]
        view_l, view_r = st.columns([3, 1], vertical_alignment="top")
        with view_l:
            card_header(
                "Product mix by company",
                "Share of each company's complaints by product, so companies of different "
                "sizes compare directly.",
            )
        with view_r, st.container(horizontal=True, horizontal_alignment="right"):
            view = st.segmented_control(
                "View",
                ["Share", "Count"],
                default="Share",
                required=True,
                key="heat_view",
                label_visibility="collapsed",
            )
            with st.popover("Data", type="tertiary", icon=":material/table:"):
                st.dataframe(counts.reset_index(names="Product"), hide_index=True)
        if counts.empty:
            st.info("No complaints for these companies in this period.")
        else:
            matrix = counts / counts.sum(axis=0) if view == "Share" else counts
            fig = go.Figure(
                go.Heatmap(
                    z=matrix.values,
                    x=matrix.columns,
                    y=matrix.index,
                    zmin=0,
                    colorscale=COLORSCALE,
                    xgap=2,
                    ygap=2,
                    texttemplate="%{z:.0%}" if view == "Share" else "%{z:.2~s}",
                    textfont={"size": 11},
                    showscale=False,
                    customdata=counts.values,
                    hovertemplate="%{x}<br>%{y}: %{customdata:,} complaints<extra></extra>",
                )
            )
            style(fig, height=70 + 34 * len(matrix))
            fig.update_xaxes(side="top", tickangle=0)
            fig.update_yaxes(autorange="reversed", gridcolor="rgba(0,0,0,0)")
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

    left, right = st.columns(2)
    with left, st.container(border=True, gap="small", height="stretch"):
        issues = query(
            f"""
            select i.issue, count(*) as complaints
            from fct_complaints f
            join dim_company c on f.company_key = c.company_key
            join dim_issue i on f.issue_key = i.issue_key
            where f.date_received_key between ? and last_day(?) and c.parent_company in ({ph})
            group by 1 order by 2 desc limit 10
            """,
            base_params,
        )
        card_header(
            "Top issues",
            "Across the selected companies and period.",
            issues.rename(columns={"issue": "Issue", "complaints": "Complaints"}),
        )
        if issues.empty:
            st.info("No complaints for these companies in this period.")
        else:
            issues = issues.sort_values("complaints")
            fig = go.Figure(
                go.Bar(
                    x=issues["complaints"],
                    y=["<br>".join(textwrap.wrap(s, 34)) for s in issues["issue"]],
                    orientation="h",
                    marker={"color": SERIES[0], "cornerradius": 3},
                    text=issues["complaints"],
                    texttemplate="%{text:,}",
                    textposition="outside",
                    cliponaxis=False,
                    textfont={"color": TICK},
                    hovertemplate="%{x:,} complaints<extra></extra>",
                )
            )
            style(fig, height=430)
            fig.update_layout(bargap=0.4, margin={"r": 56})
            fig.update_xaxes(visible=False)
            fig.update_yaxes(showgrid=False)
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

    with right, st.container(border=True, gap="small", height="stretch"):
        yoy_month = max(complete_months, default=None)
        if yoy_month is None:
            card_header("Year-over-year change by product")
            st.info("Pick a period that includes a complete month.")
        else:
            prior = date(yoy_month.year - 1, yoy_month.month, 1)
            yoy = query(
                f"""
                select p.product_group,
                       count_if(f.date_received_key >= ?) as complaints,
                       count_if(f.date_received_key < ?) as complaints_prev_year
                from fct_complaints f
                join dim_company c on f.company_key = c.company_key
                join dim_product p on f.product_key = p.product_key
                where c.parent_company in ({ph})
                  and (f.date_received_key between ? and last_day(?)
                       or f.date_received_key between ? and last_day(?))
                group by 1
                having complaints_prev_year >= 50
                order by complaints desc
                limit 10
                """,
                (yoy_month, yoy_month, *sel, yoy_month, yoy_month, prior, prior),
            )
            yoy["yoy_change_pct"] = yoy["complaints"] / yoy["complaints_prev_year"] - 1
            card_header(
                "Year-over-year change by product",
                f"{month_label(yoy_month)} vs {month_label(prior)}, selected companies. "
                "Red: more complaints than a year earlier; gray: fewer.",
                yoy.rename(
                    columns={
                        "product_group": "Product",
                        "complaints": month_label(yoy_month),
                        "complaints_prev_year": month_label(prior),
                        "yoy_change_pct": "Change",
                    }
                ),
            )
            if yoy.empty:
                st.info(f"Not enough complaints in {month_label(prior)} to compare.")
            else:
                yoy = yoy.sort_values("yoy_change_pct")
                fig = go.Figure(
                    go.Bar(
                        x=yoy["yoy_change_pct"],
                        y=yoy["product_group"],
                        orientation="h",
                        marker={
                            "color": [UP if v > 0 else DOWN for v in yoy["yoy_change_pct"]],
                            "cornerradius": 3,
                        },
                        text=[f"{v:+.0%}" for v in yoy["yoy_change_pct"]],
                        textposition="outside",
                        textfont={"color": TICK},
                        cliponaxis=False,
                        customdata=yoy[["complaints", "complaints_prev_year"]],
                        hovertemplate=(
                            "%{x:+.1%}<br>%{customdata[0]:,} vs %{customdata[1]:,}<extra></extra>"
                        ),
                    )
                )
                # Keep bars the same thickness however many products qualify.
                style(fig, height=90 + 40 * len(yoy))
                fig.update_layout(bargap=0.4)
                # Pad both ends so bar-end labels stay clear of the category labels.
                lo = min(0.0, float(yoy["yoy_change_pct"].min()))
                hi = max(0.0, float(yoy["yoy_change_pct"].max()))
                pad = 0.22 * ((hi - lo) or 1.0)
                fig.update_xaxes(
                    range=[lo - pad if lo < 0 else -pad / 4, hi + pad if hi > 0 else pad / 4],
                    showgrid=True,
                    gridcolor=GRID,
                    tickformat=".0%",
                    zeroline=True,
                    zerolinecolor=AXIS,
                    zerolinewidth=1,
                )
                fig.update_yaxes(showgrid=False)
                st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)

# ---------------------------------------------------------------- geography
with geography, st.container(border=True, gap="small"):
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
    ranked = states.sort_values("per_100k", ascending=False)
    card_header(
        "Complaints per 100,000 residents",
        ("Banks" if banks_only else "All companies")
        + " in the selected period (not only the selected companies). 2020 census population.",
        ranked[["state_name", "complaints", "per_100k"]].rename(
            columns={"state_name": "State", "complaints": "Complaints", "per_100k": "Per 100k"}
        ),
    )
    if states.empty:
        st.info("No complaints in this period.")
    else:
        # Cap the color scale at the 95th percentile so one outlier (usually DC) doesn't
        # wash out every other state. Hover and the table still show true values.
        cap = float(states["per_100k"].quantile(0.95))
        map_col, list_col = st.columns([3, 1], vertical_alignment="center")
        with map_col:
            fig = go.Figure(
                go.Choropleth(
                    locations=states["state"],
                    z=states["per_100k"].clip(upper=cap),
                    zmin=0,
                    zmax=cap,
                    locationmode="USA-states",
                    colorscale=COLORSCALE,
                    marker_line_color="rgba(128,128,128,0.35)",
                    marker_line_width=0.5,
                    customdata=states[["state_name", "complaints", "per_100k"]],
                    hovertemplate=(
                        "%{customdata[0]}<br>%{customdata[2]:.1f} per 100k "
                        "(%{customdata[1]:,} complaints)<extra></extra>"
                    ),
                    colorbar={
                        "orientation": "h",
                        "x": 0,
                        "xanchor": "left",
                        "y": -0.02,
                        "yanchor": "top",
                        "len": 0.4,
                        "thickness": 8,
                        "outlinewidth": 0,
                        "tickfont": {"color": TICK},
                        "title": {
                            "text": f"per 100k (scale capped at {cap:.0f})",
                            "side": "top",
                            "font": {"color": TICK, "size": 11},
                        },
                    },
                )
            )
            style(fig, height=470)
            fig.update_geos(
                scope="usa",
                bgcolor="rgba(0,0,0,0)",
                showland=False,
                showlakes=False,
                showframe=False,
                showcoastlines=False,
            )
            st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG)
        with list_col:
            st.dataframe(
                ranked.head(10)[["state_name", "per_100k"]],
                hide_index=True,
                width="stretch",
                column_config={
                    "state_name": st.column_config.TextColumn("Highest per capita"),
                    "per_100k": st.column_config.NumberColumn("Per 100k", format="%.0f"),
                },
            )
