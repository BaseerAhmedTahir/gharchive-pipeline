"""GH Archive pipeline dashboard.

Reads the gold Parquet marts (and silver, for the ops page) via DuckDB.
Design follows the dataviz method: fixed categorical color slots assigned by
overall volume (tail folded into 'Other'), one hue for single-series charts,
legends for >= 2 series, tables as the accessible view of every chart.
"""

from __future__ import annotations

import altair as alt
import pandas as pd
import streamlit as st

from dashboard import queries

# validated reference palette (dataviz skill): fixed categorical slot order
SERIES = ["#2a78d6", "#1baf7a", "#eda100", "#008300", "#4a3aa7", "#e34948"]
OTHER_GRAY = "#898781"
BLUE = SERIES[0]  # single-series / sequential hue
AQUA = SERIES[1]

st.set_page_config(page_title="GH Archive Pipeline", page_icon=":package:", layout="wide")


@st.cache_data(ttl=300)
def _load(data_root: str, query_name: str, *args):
    # data_root is part of the cache key so a DATA_ROOT change (or tests
    # against fixture lakes) can never serve stale cross-lake results
    cfg = queries.dashboard_config()
    if query_name == "ops_snapshot":
        return queries.ops_snapshot(cfg)
    con = queries.open_connection(cfg)
    try:
        return getattr(queries, query_name)(con, cfg, *args)
    finally:
        con.close()


def load(query_name: str, *args):
    return _load(str(queries.dashboard_config().data_root), query_name, *args)


def type_color_scale(order: list[str]) -> alt.Scale:
    colors = [SERIES[i] if t != "Other" else OTHER_GRAY for i, t in enumerate(order)]
    return alt.Scale(domain=order, range=colors)


page = st.sidebar.radio(
    "Page", ["Overview", "Trending repos", "Pull requests", "Pipeline ops"]
)
st.sidebar.caption(
    "GH Archive -> DuckDB -> Parquet, orchestrated by Airflow. "
    "All numbers are measured from the live lake."
)

if page == "Overview":
    st.title("GitHub activity, from the firehose")
    s = load("summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Events processed", f"{s['total_events']:,}")
    c2.metric("Events / day", f"{s['events_per_day']:,.0f}")
    c3.metric("Bot share", f"{s['bot_share']:.0%}")
    c4.metric("Active repos", f"{s['distinct_repos']:,}")
    st.caption(f"Window: {s['first_day']} to {s['last_day']} ({s['days']} days retained)")

    mix = pd.DataFrame(load("daily_type_mix"))
    order = load("type_order")
    if not mix.empty:
        st.subheader("Daily events by type")
        chart = (
            alt.Chart(mix)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4, binSpacing=2)
            .encode(
                x=alt.X("event_date:T", title=None, axis=alt.Axis(format="%b %d")),
                y=alt.Y("events:Q", title=None),
                color=alt.Color("type:N", scale=type_color_scale(order), title="Event type"),
                order=alt.Order("events:Q", sort="descending"),
                tooltip=["event_date:T", "type:N", "events:Q", "bot_events:Q"],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, width="stretch")

        st.subheader("Human vs bot, by event type")
        split = (
            mix.groupby("type", as_index=False)[["human_events", "bot_events"]]
            .sum()
            .melt(id_vars="type", var_name="who", value_name="events")
            .replace({"human_events": "Human", "bot_events": "Bot"})
        )
        split_chart = (
            alt.Chart(split)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("type:N", sort=order, title=None),
                y=alt.Y("events:Q", title=None),
                color=alt.Color(
                    "who:N",
                    scale=alt.Scale(domain=["Human", "Bot"], range=[BLUE, OTHER_GRAY]),
                    title=None,
                ),
                xOffset="who:N",
                tooltip=["type:N", "who:N", "events:Q"],
            )
            .properties(height=280)
        )
        st.altair_chart(split_chart, width="stretch")
        with st.expander("Table view"):
            st.dataframe(mix, width="stretch")

elif page == "Trending repos":
    st.title("Trending repositories")
    st.caption(
        "Human (non-bot) daily activity vs a trailing 7-day baseline. "
        "Multi-actor repos only - single-actor firehoses are bots by behavior."
    )
    scored = load("trending")
    if scored:
        df = pd.DataFrame(scored)
        st.altair_chart(
            alt.Chart(df.head(12))
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4, color=BLUE)
            .encode(
                x=alt.X("trend_score:Q", title="x the 7-day baseline"),
                y=alt.Y("repo_name:N", sort="-x", title=None),
                tooltip=list(df.columns),
            )
            .properties(height=360),
            width="stretch",
        )
        with st.expander("Table view", expanded=True):
            st.dataframe(df, width="stretch")
    else:
        first = load("trending_first_scored_date")
        st.info(
            "No trend scores yet - the trailing 7-day baseline needs 7 full days "
            f"of retained history. First scores expected on **{first:%Y-%m-%d}**. "
            "Meanwhile, the most active multi-actor repos:"
        )
        st.dataframe(pd.DataFrame(load("top_repos")), width="stretch")

elif page == "Pull requests":
    st.title("Pull-request lifecycle")
    st.caption(
        "Opened/closed counts and hours-open latency, joined from open->close "
        "events. (2026 GH Archive payloads no longer carry merge status - "
        "this mart reports lifecycle, honestly.)"
    )
    pr = pd.DataFrame(load("pr_daily"))
    if pr.empty:
        st.info("No PR data in the retained window yet.")
    else:
        counts = pr.melt(
            id_vars="event_date",
            value_vars=["prs_opened", "prs_closed"],
            var_name="what",
            value_name="prs",
        ).replace({"prs_opened": "Opened", "prs_closed": "Closed"})
        st.altair_chart(
            alt.Chart(counts)
            .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=80))
            .encode(
                x=alt.X("event_date:T", title=None, axis=alt.Axis(format="%b %d")),
                y=alt.Y("prs:Q", title="PRs / day"),
                color=alt.Color(
                    "what:N",
                    scale=alt.Scale(domain=["Opened", "Closed"], range=[BLUE, AQUA]),
                    title=None,
                ),
                tooltip=["event_date:T", "what:N", "prs:Q"],
            )
            .properties(height=300),
            width="stretch",
        )
        c1, c2 = st.columns(2)
        latest = pr.iloc[-1]
        median = latest.get("median_hours_open")
        c1.metric(
            "Median hours open (latest day)",
            "n/a" if pd.isna(median) else f"{median:,.1f} h",
        )
        c2.metric("PRs closed with known open event", f"{int(latest['closed_with_known_open']):,}")
        with st.expander("Table view"):
            st.dataframe(pr, width="stretch")

elif page == "Pipeline ops":
    st.title("Pipeline ops")
    st.caption("The pipeline watching itself: freshness, volumes, and the lake on disk.")
    ops = load("ops_snapshot")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Freshness lag",
        "n/a" if ops["freshness_hours"] is None else f"{ops['freshness_hours']:.1f} h",
        help="Now minus the end of the newest silver hour. 1-2h is normal for hourly batch.",
    )
    c2.metric("Silver hours", f"{ops['silver_partitions']}")
    c3.metric("Bronze on disk", f"{ops['bronze_mb']:,.0f} MB")
    c4.metric("Silver on disk", f"{ops['silver_mb']:,.0f} MB")

    volume = pd.DataFrame(load("hourly_volume"))
    if not volume.empty:
        st.subheader("Events per hour (silver)")
        st.altair_chart(
            alt.Chart(volume)
            .mark_line(strokeWidth=2, color=BLUE)
            .encode(
                x=alt.X("hour:T", title=None),
                y=alt.Y("events:Q", title=None),
                tooltip=["hour:T", "events:Q"],
            )
            .properties(height=280),
            width="stretch",
        )
    st.subheader("Gold marts")
    st.dataframe(
        pd.DataFrame(ops["gold_marts"], columns=["mart", "size_mb"]).round(2),
        width="stretch",
    )

    st.subheader("Per-run task metrics")
    summary_row = load("throughput_summary")
    runs = pd.DataFrame(load("run_metrics"))
    if summary_row is None or runs.empty:
        st.info("No per-run metrics yet — they appear after the first instrumented DAG run.")
    else:
        m1, m2, m3 = st.columns(3)
        m1.metric(
            "Median transform throughput",
            f"{summary_row['median_events_per_sec']:,.0f} events/s",
        )
        m2.metric("Median transform time", f"{summary_row['median_transform_seconds']:.1f} s")
        m3.metric(
            "Raw data downloaded",
            f"{summary_row['gb_ingested']:.2f} GB",
            help=f"across {summary_row['download_runs']} non-skipped ingest runs",
        )
        st.altair_chart(
            alt.Chart(runs)
            .mark_line(strokeWidth=2, point=alt.OverlayMarkDef(size=40))
            .encode(
                x=alt.X("hour:T", title=None),
                y=alt.Y("duration_seconds:Q", title="task seconds"),
                color=alt.Color(
                    "kind:N",
                    scale=alt.Scale(
                        domain=["ingest", "transform", "quality_gate"],
                        range=[SERIES[0], SERIES[1], SERIES[2]],
                    ),
                    title=None,
                ),
                tooltip=["hour:T", "kind:N", "duration_seconds:Q", "rows_in:Q"],
            )
            .properties(height=260),
            width="stretch",
        )
