"""Tools used by the LangGraph report workflow."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from rag_pipeline import format_context_with_sources, retrieve_context_with_sources

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    from langchain_core.tools import tool
except ImportError:  # Allows the app to run before optional LangGraph deps are installed.
    def tool(func: Callable) -> Callable:
        func.name = func.__name__
        return func


PROJECT_ROOT = Path(__file__).resolve().parent.parent
EVENTS_FILE = PROJECT_ROOT / "output" / "processed" / "events.parquet"
METRICS_FILE = PROJECT_ROOT / "output" / "aggregates" / "team_metrics.parquet"


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _load_events() -> pd.DataFrame:
    df = _read_parquet(EVENTS_FILE)
    if df.empty:
        return df
    df["minute"] = pd.to_numeric(df.get("minute"), errors="coerce").fillna(0).astype(int)
    df["value"] = pd.to_numeric(df.get("value"), errors="coerce").fillna(0).astype(int)
    df["event_type_norm"] = df.get("event_type", "").astype(str).str.lower()
    return df


def _load_latest_metrics() -> pd.DataFrame:
    df = _read_parquet(METRICS_FILE)
    if df.empty:
        return df
    df["snapshot_time"] = pd.to_datetime(df.get("snapshot_time"), errors="coerce")
    return (
        df.sort_values(["snapshot_time", "batch_id"])
        .groupby("team", as_index=False)
        .tail(1)
        .sort_values("team")
    )


def _intensity(events_df: pd.DataFrame, interval_minutes: int = 5) -> dict[str, Any]:
    if events_df.empty:
        return {"interval": "sin datos", "events": 0}

    buckets = events_df.copy()
    buckets["interval_start"] = (buckets["minute"] // interval_minutes) * interval_minutes
    grouped = (
        buckets.groupby("interval_start")
        .agg(events=("event_id", "count"), shots=("event_type_norm", lambda values: (values == "shot").sum()))
        .reset_index()
        .sort_values(["events", "shots"], ascending=False)
    )
    top = grouped.iloc[0]
    start = int(top["interval_start"])
    return {
        "interval": f"minutos {start}-{start + interval_minutes}",
        "events": int(top["events"]),
        "shots": int(top["shots"]),
    }


def compute_match_insights() -> dict[str, Any]:
    """Compute structured insights from Spark parquet outputs."""
    events_df = _load_events()
    latest_metrics = _load_latest_metrics()

    if events_df.empty and latest_metrics.empty:
        return {
            "status": "empty",
            "message": "No hay datos procesados por Spark para generar el informe.",
        }

    event_type = events_df.get("event_type_norm", pd.Series(dtype=str))
    total_shots = int((event_type == "shot").sum()) if not events_df.empty else 0
    total_fouls = int(event_type.isin(["foul committed", "foul won"]).sum()) if not events_df.empty else 0
    total_goals = int(((event_type == "goal") | ((event_type == "shot") & (events_df["value"] == 3))).sum())

    highlighted_team = {}
    recovery_team = {}
    if not latest_metrics.empty:
        numeric_cols = ["total_events", "goals", "shots", "passes", "fouls", "recoveries"]
        for column in numeric_cols:
            latest_metrics[column] = pd.to_numeric(latest_metrics.get(column), errors="coerce").fillna(0)
        highlighted_team = latest_metrics.sort_values(
            ["goals", "shots", "total_events"], ascending=False
        ).iloc[0].to_dict()
        recovery_team = latest_metrics.sort_values("recoveries", ascending=False).iloc[0].to_dict()

    highlighted_player = {}
    if not events_df.empty and "player" in events_df.columns:
        player_df = (
            events_df.groupby(["player", "team"], dropna=False)
            .agg(
                participations=("event_id", "count"),
                shots=("event_type_norm", lambda values: (values == "shot").sum()),
                goals=("value", lambda values: int(((events_df.loc[values.index, "event_type_norm"] == "shot") & (values == 3)).sum())),
            )
            .reset_index()
            .sort_values(["goals", "shots", "participations"], ascending=False)
        )
        if not player_df.empty:
            highlighted_player = player_df.iloc[0].to_dict()

    return {
        "status": "ok",
        "source": "datos_partido_tiempo_real",
        "total_events": int(len(events_df)),
        "total_shots": total_shots,
        "total_goals": total_goals,
        "total_fouls": total_fouls,
        "teams": latest_metrics.to_dict(orient="records") if not latest_metrics.empty else [],
        "highlighted_team": highlighted_team,
        "team_with_most_recoveries": recovery_team,
        "highlighted_player": highlighted_player,
        "highest_intensity_interval": _intensity(events_df),
    }


@tool
def query_match_metrics(query: str) -> str:
    """Tool 1: Query computed match metrics stored by Spark."""
    insights = compute_match_insights()
    insights["tool_query"] = query
    return json.dumps(insights, ensure_ascii=False, default=str)


@tool
def retrieve_document_context(query: str) -> str:
    """Tool 2: Retrieve contextual snippets from the local semantic RAG index."""
    retrieved = retrieve_context_with_sources(
        query,
        k=4,
        docs_path=PROJECT_ROOT / os.getenv("RAG_DOCS_PATH", "data/docs/rag_corpus"),
        persist_dir=PROJECT_ROOT / os.getenv("RAG_CHROMA_DIR", "output/chroma_db"),
        embedding_model=os.getenv("RAG_EMBEDDING_MODEL", "sentence-transformers/all-mpnet-base-v2"),
    )
    return format_context_with_sources(retrieved)
