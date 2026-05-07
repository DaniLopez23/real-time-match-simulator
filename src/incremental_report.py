"""Incremental report segment generation for closed match-minute windows."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from graph_workflow import run_window_analysis_workflow
from settings import (
    EVENTS_FILE,
    INCREMENTAL_SEGMENTS_FILE,
    REPORT_WINDOW_MINUTES,
    WINDOW_METRICS_FILE,
)


SEGMENT_COLUMNS = [
    "segment_id",
    "match_id",
    "window_start_minute",
    "window_end_minute",
    "window_minutes",
    "text",
    "metrics_json",
    "rag_context",
    "llm_model",
    "trace_json",
    "generated_at",
]


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def load_incremental_segments() -> pd.DataFrame:
    """Load already generated incremental narrative segments."""
    df = _read_parquet(INCREMENTAL_SEGMENTS_FILE)
    if df.empty:
        return pd.DataFrame(columns=SEGMENT_COLUMNS)
    for column in SEGMENT_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["window_start_minute"] = pd.to_numeric(df["window_start_minute"], errors="coerce").fillna(0).astype(int)
    df["window_end_minute"] = pd.to_numeric(df["window_end_minute"], errors="coerce").fillna(0).astype(int)
    df["window_minutes"] = pd.to_numeric(df["window_minutes"], errors="coerce").fillna(REPORT_WINDOW_MINUTES).astype(int)
    df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df[SEGMENT_COLUMNS].sort_values(["match_id", "window_start_minute"])


def _write_segments(df: pd.DataFrame) -> None:
    INCREMENTAL_SEGMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    df = df[SEGMENT_COLUMNS].sort_values(["match_id", "window_start_minute"])
    df.to_parquet(INCREMENTAL_SEGMENTS_FILE, index=False)


def _normalize_events(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return events_df
    events_df = events_df.copy()
    events_df["minute"] = pd.to_numeric(events_df.get("minute"), errors="coerce").fillna(0).astype(int)
    events_df["value"] = pd.to_numeric(events_df.get("value"), errors="coerce").fillna(0).astype(int)
    events_df["event_type_norm"] = events_df.get("event_type", "").astype(str).str.lower()
    events_df["match_id"] = events_df.get("match_id", "").astype(str)
    return events_df


def _team_window_metrics(events_df: pd.DataFrame) -> list[dict[str, Any]]:
    if events_df.empty:
        return []
    grouped = []
    for team, team_df in events_df.groupby("team", dropna=False):
        event_type = team_df["event_type_norm"]
        grouped.append(
            {
                "team": team,
                "events": int(len(team_df)),
                "goals": int(((event_type == "goal") | ((event_type == "shot") & (team_df["value"] == 3))).sum()),
                "shots": int((event_type == "shot").sum()),
                "passes": int((event_type == "pass").sum()),
                "fouls": int(event_type.isin(["foul committed", "foul won"]).sum()),
                "recoveries": int((event_type == "ball recovery").sum()),
                "avg_value": float(team_df["value"].mean()) if not team_df.empty else 0.0,
            }
        )
    return sorted(grouped, key=lambda item: (-item["events"], str(item["team"])))


def _highlighted_player(events_df: pd.DataFrame) -> dict[str, Any]:
    if events_df.empty or "player" not in events_df.columns:
        return {}
    rows = []
    for (player, team), player_df in events_df.groupby(["player", "team"], dropna=False):
        event_type = player_df["event_type_norm"]
        rows.append(
            {
                "player": player,
                "team": team,
                "participations": int(len(player_df)),
                "shots": int((event_type == "shot").sum()),
                "goals": int(((event_type == "shot") & (player_df["value"] == 3)).sum()),
            }
        )
    return sorted(rows, key=lambda item: (-item["goals"], -item["shots"], -item["participations"]))[0] if rows else {}


def build_window_payload(
    events_df: pd.DataFrame,
    match_id: str,
    start_minute: int,
    end_minute: int,
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    """Build metrics, event samples, and cumulative context for one window."""
    match_events = events_df[events_df["match_id"] == match_id]
    window_events = match_events[
        (match_events["minute"] >= start_minute)
        & (match_events["minute"] < end_minute)
    ].copy()
    cumulative_events = match_events[match_events["minute"] < end_minute].copy()

    event_type = window_events.get("event_type_norm", pd.Series(dtype=str))
    window_metrics = {
        "match_id": match_id,
        "window_start_minute": start_minute,
        "window_end_minute": end_minute,
        "window_minutes": end_minute - start_minute,
        "total_events": int(len(window_events)),
        "total_shots": int((event_type == "shot").sum()) if not window_events.empty else 0,
        "total_goals": int(((event_type == "goal") | ((event_type == "shot") & (window_events["value"] == 3))).sum()) if not window_events.empty else 0,
        "total_fouls": int(event_type.isin(["foul committed", "foul won"]).sum()) if not window_events.empty else 0,
        "teams": _team_window_metrics(window_events),
        "highlighted_player": _highlighted_player(window_events),
    }

    cumulative_type = cumulative_events.get("event_type_norm", pd.Series(dtype=str))
    cumulative_metrics = {
        "match_id": match_id,
        "up_to_minute": end_minute,
        "total_events": int(len(cumulative_events)),
        "total_shots": int((cumulative_type == "shot").sum()) if not cumulative_events.empty else 0,
        "total_goals": int(((cumulative_type == "goal") | ((cumulative_type == "shot") & (cumulative_events["value"] == 3))).sum()) if not cumulative_events.empty else 0,
        "teams": _team_window_metrics(cumulative_events),
    }

    event_cols = ["minute", "team", "player", "event_type", "zone", "value"]
    available_cols = [col for col in event_cols if col in window_events.columns]
    event_records = (
        window_events[available_cols]
        .sort_values(["minute", "team", "player"], na_position="last")
        .head(30)
        .to_dict(orient="records")
    )
    return window_metrics, event_records, cumulative_metrics


def _closed_windows(events_df: pd.DataFrame, window_minutes: int) -> list[tuple[str, int, int]]:
    windows: list[tuple[str, int, int]] = []
    if events_df.empty:
        return windows

    for match_id, match_df in events_df.groupby("match_id"):
        max_minute = int(match_df["minute"].max())
        starts = sorted(((match_df["minute"] // window_minutes) * window_minutes).unique())
        for start in starts:
            start = int(start)
            end = start + window_minutes
            if end <= max_minute:
                windows.append((str(match_id), start, end))
    return windows


def generate_missing_report_segments(max_windows: int | None = None) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Generate and persist missing incremental segments for closed windows."""
    events_df = _normalize_events(_read_parquet(EVENTS_FILE))
    existing_df = load_incremental_segments()
    if events_df.empty:
        return existing_df, []

    existing_keys = {
        (
            str(row.match_id),
            int(row.window_start_minute),
            int(row.window_end_minute),
        )
        for row in existing_df.itertuples(index=False)
    }

    generated_rows: list[dict[str, Any]] = []
    for match_id, start, end in _closed_windows(events_df, REPORT_WINDOW_MINUTES):
        if (match_id, start, end) in existing_keys:
            continue
        if max_windows is not None and len(generated_rows) >= max_windows:
            break

        window_metrics, window_events, cumulative_metrics = build_window_payload(events_df, match_id, start, end)
        result = run_window_analysis_workflow(
            match_id=match_id,
            window_start_minute=start,
            window_end_minute=end,
            window_metrics=window_metrics,
            window_events=window_events,
            cumulative_metrics=cumulative_metrics,
        )
        generated_rows.append(
            {
                "segment_id": f"{match_id}_{start}_{end}",
                "match_id": match_id,
                "window_start_minute": start,
                "window_end_minute": end,
                "window_minutes": REPORT_WINDOW_MINUTES,
                "text": result.get("text", ""),
                "metrics_json": json.dumps(window_metrics, ensure_ascii=False, default=str),
                "rag_context": result.get("rag_context", ""),
                "llm_model": result.get("llm_model", ""),
                "trace_json": json.dumps(result.get("trace", []), ensure_ascii=False, default=str),
                "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
        )

    if generated_rows:
        new_df = pd.DataFrame(generated_rows, columns=SEGMENT_COLUMNS)
        output_df = pd.concat([existing_df, new_df], ignore_index=True)
        output_df = output_df.drop_duplicates(["match_id", "window_start_minute", "window_end_minute"], keep="last")
        _write_segments(output_df)
        return load_incremental_segments(), generated_rows

    return existing_df, []


def load_window_metrics() -> pd.DataFrame:
    """Load Spark-generated metrics by report window."""
    df = _read_parquet(WINDOW_METRICS_FILE)
    if df.empty:
        return pd.DataFrame()
    for column in ["window_start_minute", "window_end_minute", "events", "shots", "goals", "passes", "fouls", "recoveries"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    return df.sort_values(["window_start_minute", "team"])
