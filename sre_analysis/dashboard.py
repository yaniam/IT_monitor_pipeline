from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import List, Sequence

import pandas as pd
import streamlit as st

from sre_analysis.online_monitor import (
    DEFAULT_DATASET_PATH,
    DEFAULT_DB_PATH,
    DEFAULT_DROP_THRESHOLD,
    DEFAULT_HISTORY,
    OnlineMonitor,
)


def fetch_available_segments(db_path: Path) -> List[str]:
    if not db_path.exists():
        return []
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT DISTINCT segment FROM segment_timeseries ORDER BY segment"
        ).fetchall()
        return [row[0] for row in rows]
    except sqlite3.OperationalError:
        return []
    finally:
        conn.close()


def fetch_segment_timeseries(db_path: Path, segments: Sequence[str]) -> pd.DataFrame:
    if not segments or not db_path.exists():
        return pd.DataFrame()
    placeholders = ",".join("?" for _ in segments)
    query = f"""
        SELECT
            ts,
            segment,
            count,
            dim1_name,
            dim1_value,
            dim2_name,
            dim2_value,
            dim3_name,
            dim3_value
        FROM segment_timeseries
        WHERE segment IN ({placeholders})
        ORDER BY ts ASC
    """
    conn = sqlite3.connect(db_path)
    try:
        return pd.read_sql_query(query, conn, params=list(segments))
    except sqlite3.OperationalError:
        return pd.DataFrame()
    finally:
        conn.close()


def build_dimension_label(row: pd.Series) -> str:
    parts: List[str] = []
    for idx in range(1, 4):
        name = row.get(f"dim{idx}_name")
        value = row.get(f"dim{idx}_value")
        if pd.notna(name) and str(name).strip():
            value_str = "(blank)" if pd.isna(value) or str(value).strip() == "" else str(value)
            parts.append(f"{name}={value_str}")
    return " | ".join(parts) if parts else str(row.get("segment", "Unknown"))


def main() -> None:
    st.set_page_config(page_title="SRE Online Dashboard", layout="wide")
    st.title("SRE Online Percentage Dashboard")
    st.caption(
        "Visualize the percentage of machines that reported activity in the last hour "
        "and spot significant drops at a glance."
    )

    with st.sidebar:
        st.header("Configuration")
        db_path = Path(
            st.text_input("SQLite database", value=str(DEFAULT_DB_PATH.resolve()))
        )
        dataset_path = Path(
            st.text_input("Device inventory CSV", value=str(DEFAULT_DATASET_PATH.resolve()))
        )
        history = st.slider(
            "Baseline window (snapshots)",
            min_value=1,
            max_value=12,
            value=DEFAULT_HISTORY,
        )
        drop_threshold = st.slider(
            "Alert drop threshold (%)",
            min_value=1,
            max_value=50,
            value=int(DEFAULT_DROP_THRESHOLD * 100),
        ) / 100.0

    monitor = OnlineMonitor(
        db_path=db_path,
        dataset_path=dataset_path,
        history=history,
        drop_threshold=drop_threshold,
        enable_alert_log=False,
    )

    snapshots = monitor.load_history()
    if not snapshots:
        st.warning("No ingestion snapshots found. Run data_collection/fake_api_collector.py first.")
        return

    df = pd.DataFrame(
        {
            "timestamp": [s.timestamp for s in snapshots],
            "online_count": [s.total_online for s in snapshots],
            "percentage": [s.percentage for s in snapshots],
        }
    ).sort_values("timestamp")
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df.set_index("timestamp", inplace=True)
    df["baseline"] = (
        df["percentage"].shift(1).rolling(window=history, min_periods=1).mean()
    )
    df["drop_fraction"] = (
        (df["baseline"] - df["percentage"]) / df["baseline"]
    ).clip(lower=0).fillna(0)

    latest = df.iloc[-1]
    st.metric(
        "Latest Online Share",
        f"{latest['percentage']:.2f}%",
        delta=f"{(latest['percentage'] - latest['baseline']):+.2f} pts"
        if not pd.isna(latest["baseline"])
        else None,
    )

    chart_data = df[["percentage", "baseline"]]
    st.line_chart(chart_data, height=400)

    with st.expander("Snapshot details"):
        st.dataframe(
            df[["online_count", "percentage", "baseline", "drop_fraction"]]
            .rename(
                columns={
                    "online_count": "Online machines",
                    "percentage": "Online %",
                    "baseline": "Baseline %",
                    "drop_fraction": "Drop vs. baseline",
                }
            )
            .style.format(
                {
                    "Online machines": "{:.0f}",
                    "Online %": "{:.2f}",
                    "Baseline %": "{:.2f}",
                    "Drop vs. baseline": "{:.2%}",
                }
            )
        )

    alert_rows = df[df["drop_fraction"] >= drop_threshold]
    if not alert_rows.empty:
        st.error(
            f"{len(alert_rows)} snapshot(s) breached the {drop_threshold:.0%} drop threshold."
        )
        st.dataframe(
            alert_rows[["online_count", "percentage", "baseline", "drop_fraction"]]
            .rename(
                columns={
                    "online_count": "Online machines",
                    "percentage": "Online %",
                    "baseline": "Baseline %",
                    "drop_fraction": "Drop vs. baseline",
                }
            )
            .style.format(
                {
                    "Online machines": "{:.0f}",
                    "Online %": "{:.2f}",
                    "Baseline %": "{:.2f}",
                    "Drop vs. baseline": "{:.2%}",
                }
            )
        )
    else:
        st.success("No alerts triggered for the current drop threshold.")

    available_segments = fetch_available_segments(db_path)
    if not available_segments:
        st.info("No segment-level data available yet.")
        return

    st.header("Segment breakouts")
    default_segments = available_segments[: min(3, len(available_segments))]
    selected_segments = st.multiselect(
        "Segments to visualize",
        options=available_segments,
        default=default_segments,
    )
    top_n = st.slider("Top dimension values per segment", min_value=1, max_value=10, value=5)
    metric_choice = st.radio(
        "Metric type",
        options=("Percentage share", "Raw counts"),
        horizontal=True,
    )

    if not selected_segments:
        st.info("Select at least one segment to display trends.")
        return

    segments_df = fetch_segment_timeseries(db_path, selected_segments)
    if segments_df.empty:
        st.warning("No rows found for the selected segments yet.")
        return

    segments_df["timestamp"] = pd.to_datetime(segments_df["ts"])
    segments_df["label"] = segments_df.apply(build_dimension_label, axis=1)
    segments_df["segment_total"] = (
        segments_df.groupby(["segment", "timestamp"])["count"].transform("sum")
    )
    segments_df["percentage"] = (
        (segments_df["count"] / segments_df["segment_total"]) * 100
    ).fillna(0.0)

    value_column = "percentage" if metric_choice == "Percentage share" else "count"

    for segment_name in selected_segments:
        subset = segments_df[segments_df["segment"] == segment_name].copy()
        if subset.empty:
            st.info(f"No data yet for segment '{segment_name}'.")
            continue

        top_labels = (
            subset.groupby("label")["count"]
            .sum()
            .sort_values(ascending=False)
            .head(top_n)
            .index
        )
        pivot = (
            subset[subset["label"].isin(top_labels)]
            .pivot_table(
                index="timestamp",
                columns="label",
                values=value_column,
                aggfunc="sum",
            )
            .sort_index()
            .fillna(0)
        )

        if pivot.empty:
            st.info(f"Not enough data to plot segment '{segment_name}'.")
            continue

        st.subheader(segment_name)
        st.line_chart(pivot, height=320)
        st.caption(
            "Top categories: "
            + ", ".join(top_labels)
            + (
                " (share %)" if value_column == "percentage" else " (raw counts)"
            )
        )


if __name__ == "__main__":
    main()

