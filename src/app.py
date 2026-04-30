"""Streamlit dashboard for real-time match analytics.

The dashboard reads pipeline outputs when available and falls back to mock data so the
UI remains functional before Kafka/Spark jobs are running.
"""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh

    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False


PROJECT_ROOT = Path(__file__).resolve().parent.parent
PROCESSED_DIR = PROJECT_ROOT / "output" / "processed"
AGGREGATES_DIR = PROJECT_ROOT / "output" / "aggregates"

EVENT_COLUMNS = [
    "event_id",
    "timestamp",
    "match_id",
    "team",
    "player",
    "event_type",
    "zone",
    "minute",
    "value",
    "period",
]


# def _mock_events_df() -> pd.DataFrame:
#     """Generate a realistic mock events dataframe matching the pipeline schema."""
#     rows = [
#         {
#             "event_id": f"ev_{i:03d}",
#             "timestamp": f"00:{(i * 2) % 60:02d}:{(i * 7) % 60:02d}.000",
#             "match_id": "3946454",
#             "team": "Team A" if i % 2 == 0 else "Team B",
#             "player": f"Player {i % 11 + 1}",
#             "event_type": ["Pass", "Shot", "Foul Committed", "Ball Recovery", "Duel"][i % 5],
#             "zone": [
#                 "defensive_left",
#                 "middle_center",
#                 "attacking_right",
#                 "middle_left",
#                 "attacking_center",
#             ][i % 5],
#             "minute": 2 + i * 3,
#             "value": [1, 2, 0, 1, 1][i % 5],
#             "period": 1 if i < 6 else 2,
#         }
#         for i in range(10)
#     ]

#     # Ensure there is at least one goal in mock data.
#     rows[6]["event_type"] = "Shot"
#     rows[6]["value"] = 3

#     return pd.DataFrame(rows, columns=EVENT_COLUMNS)


# def _mock_team_metrics_df() -> pd.DataFrame:
#     """Generate fallback team-level aggregates for dashboard rendering."""
#     return pd.DataFrame(
#         [
#             {
#                 "team": "Team A",
#                 "total_events": 120,
#                 "shots": 14,
#                 "goals": 2,
#                 "fouls": 9,
#                 "recoveries": 21,
#             },
#             {
#                 "team": "Team B",
#                 "total_events": 110,
#                 "shots": 11,
#                 "goals": 1,
#                 "fouls": 12,
#                 "recoveries": 18,
#             },
#         ]
#     )


def _has_files(path: Path) -> bool:
    """Check whether a directory has at least one file."""
    return path.exists() and any(item.is_file() for item in path.rglob("*"))


def load_processed_events() -> pd.DataFrame:
    """Load processed events from parquet outputs or return mock events."""
    if not PROCESSED_DIR.exists():
        return _mock_events_df()

    parquet_files = list(PROCESSED_DIR.rglob("*.parquet"))
    if parquet_files:
        try:
            df = pd.concat([pd.read_parquet(path) for path in sorted(parquet_files)], ignore_index=True)
            for column in EVENT_COLUMNS:
                if column not in df.columns:
                    df[column] = None
            return df[EVENT_COLUMNS]
        except Exception:
            return _mock_events_df()

    csv_files = list(PROCESSED_DIR.rglob("*.csv"))
    if csv_files:
        try:
            df = pd.concat([pd.read_csv(path) for path in csv_files], ignore_index=True)
            for column in EVENT_COLUMNS:
                if column not in df.columns:
                    df[column] = None
            return df[EVENT_COLUMNS]
        except Exception:
            return _mock_events_df()

    return _mock_events_df()


def load_team_metrics() -> pd.DataFrame:
    """Load team aggregates from outputs or return mock team metrics."""
    if not AGGREGATES_DIR.exists():
        return _mock_team_metrics_df()

    parquet_files = list(AGGREGATES_DIR.rglob("*.parquet"))
    if parquet_files:
        try:
            return pd.concat([pd.read_parquet(path) for path in sorted(parquet_files)], ignore_index=True)
        except Exception:
            return _mock_team_metrics_df()

    csv_files = list(AGGREGATES_DIR.rglob("*.csv"))
    if csv_files:
        try:
            return pd.concat([pd.read_csv(path) for path in csv_files], ignore_index=True)
        except Exception:
            return _mock_team_metrics_df()

    return _mock_team_metrics_df()


def render_live_events_tab(events_df: pd.DataFrame) -> None:
    """Render live events table, key metrics, and event-type chart."""
    st.subheader("Recent Event Feed")

    display_cols = ["minute", "team", "player", "event_type", "zone", "value"]
    display_df = events_df[display_cols].tail(20).sort_values(by="minute", ascending=False)
    st.dataframe(display_df, use_container_width=True, hide_index=True)

    total_events = len(events_df)
    total_goals = int(
        ((events_df["event_type"] == "Shot") & (events_df["value"] == 3)).sum()
    )
    total_shots = int((events_df["event_type"] == "Shot").sum())
    total_fouls = int(
        events_df["event_type"].isin(["Foul Committed", "Foul Won"]).sum()
    )

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Total Events", total_events)
    m2.metric("Total Goals", total_goals)
    m3.metric("Total Shots", total_shots)
    m4.metric("Total Fouls", total_fouls)

    st.subheader("Event Type Distribution")
    event_counts = events_df["event_type"].value_counts().sort_values(ascending=False)
    st.bar_chart(event_counts)


def render_team_metrics_tab(team_df: pd.DataFrame) -> None:
    """Render team comparison data and side-by-side metric cards."""
    st.subheader("Team Comparison")

    required_cols = ["team", "total_events", "shots", "goals", "fouls", "recoveries"]
    for column in required_cols:
        if column not in team_df.columns:
            team_df[column] = 0 if column != "team" else "Unknown"

    st.dataframe(team_df[required_cols], use_container_width=True, hide_index=True)

    teams = team_df[required_cols].head(2).to_dict(orient="records")
    if len(teams) < 2:
        teams = teams + [{"team": "N/A", "total_events": 0, "shots": 0, "goals": 0, "fouls": 0, "recoveries": 0}]

    col_left, col_right = st.columns(2)

    with col_left:
        left = teams[0]
        st.markdown(f"### {left['team']}")
        st.metric("Total Events", int(left["total_events"]))
        st.metric("Shots", int(left["shots"]))
        st.metric("Goals", int(left["goals"]))
        st.metric("Fouls", int(left["fouls"]))
        st.metric("Recoveries", int(left["recoveries"]))

    with col_right:
        right = teams[1]
        st.markdown(f"### {right['team']}")
        st.metric("Total Events", int(right["total_events"]))
        st.metric("Shots", int(right["shots"]))
        st.metric("Goals", int(right["goals"]))
        st.metric("Fouls", int(right["fouls"]))
        st.metric("Recoveries", int(right["recoveries"]))

    st.info("📌 Advanced metrics will appear here after Spark pipeline runs.")


def render_pipeline_status_tab() -> None:
    """Render component readiness and configuration details."""
    processed_ready = _has_files(PROCESSED_DIR)

    st.subheader("Component Status")
    st.write(f"{'✅' if processed_ready else '❌'} Kafka Producer")
    st.write(f"{'✅' if processed_ready else '❌'} Spark Streaming")
    st.write("🔄 RAG Pipeline — Coming soon")
    st.write("🔄 LangGraph Agents — Coming soon")

    st.subheader("Kafka Configuration")
    st.json({"topics": ["match_events", "match_aggregates"], "broker": "localhost:9092"})

    if st.button("🔄 Refresh Data"):
        st.rerun()


def main() -> None:
    """Main Streamlit entrypoint."""
    st.set_page_config(page_title="Real-Time Match Analytics Dashboard", page_icon="⚽", layout="wide")
    st.title("⚽ Real-Time Match Analytics Dashboard")

    # Auto-refresh page every 30 seconds when extension is available.
    if AUTOREFRESH_AVAILABLE:
        st_autorefresh(interval=30_000, key="dashboard_autorefresh")
    else:
        st.caption("Auto-refresh package not available. Use the refresh button in Pipeline Status.")

    events_df = load_processed_events()
    team_df = load_team_metrics()

    tab_live, tab_team, tab_status = st.tabs(["📡 Live Events", "📊 Team Metrics", "🔍 Pipeline Status"])

    with tab_live:
        render_live_events_tab(events_df)

    with tab_team:
        render_team_metrics_tab(team_df)

    with tab_status:
        render_pipeline_status_tab()


if __name__ == "__main__":
    main()
