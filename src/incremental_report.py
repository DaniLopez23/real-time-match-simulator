"""Generate and store 5-minute incremental report fragments."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from graph_workflow import run_window_analysis_workflow
from settings import EVENTS_FILE, INCREMENTAL_SEGMENTS_FILE, REPORT_WINDOW_MINUTES, WINDOW_METRICS_FILE


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

ProgressCallback = Callable[[str, str, dict[str, Any] | None], None]


def _emit_progress(
    callback: ProgressCallback | None,
    stage: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    if callback is None:
        return
    try:
        callback(stage, message, details or {})
    except Exception:
        pass


def _read_parquet(path: Path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def load_incremental_segments() -> pd.DataFrame:
    """Load the narrative fragments already generated."""
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
    output_df = df.copy()
    for column in SEGMENT_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = None
    for column in ["window_start_minute", "window_end_minute", "window_minutes"]:
        output_df[column] = pd.to_numeric(output_df[column], errors="coerce").fillna(0).astype("int64")
    for column in ["segment_id", "match_id", "text", "metrics_json", "rag_context", "llm_model", "trace_json"]:
        output_df[column] = output_df[column].fillna("").astype(str)
    output_df["generated_at"] = pd.to_datetime(output_df["generated_at"], errors="coerce")
    output_df = output_df[SEGMENT_COLUMNS].sort_values(["match_id", "window_start_minute"])

    tmp_file = INCREMENTAL_SEGMENTS_FILE.with_name(
        f".{INCREMENTAL_SEGMENTS_FILE.stem}_{uuid.uuid4().hex}.tmp.parquet"
    )
    output_df.to_parquet(tmp_file, index=False)
    os.replace(tmp_file, INCREMENTAL_SEGMENTS_FILE)


def _normalize_events(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return events_df
    df = events_df.copy()

    def col_or_default(column: str, default: Any) -> pd.Series:
        if column in df.columns:
            return df[column]
        return pd.Series(default, index=df.index)

    df["minute"] = pd.to_numeric(col_or_default("minute", 0), errors="coerce").fillna(0).astype(int)
    df["second"] = pd.to_numeric(col_or_default("second", 0), errors="coerce").fillna(0).astype(int)
    df["value"] = pd.to_numeric(col_or_default("value", 0), errors="coerce").fillna(0).astype(int)
    df["event_type_norm"] = col_or_default("event_type", "").astype(str).str.lower()
    df["outcome_norm"] = col_or_default("outcome", "").astype(str).str.lower()
    df["match_id"] = col_or_default("match_id", "").astype(str)
    return df


def _safe_pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 2) if denominator else 0.0


def _team_metrics(events_df: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if events_df.empty:
        return rows

    for team, team_df in events_df.groupby("team", dropna=False):
        event_type = team_df["event_type_norm"]
        outcome = team_df["outcome_norm"]
        passes = int((event_type == "pass").sum())
        successful_passes = int(((event_type == "pass") & (outcome == "success")).sum())
        duels = int((event_type == "duel").sum())
        successful_duels = int(((event_type == "duel") & (outcome == "success")).sum())
        pressures = int((event_type == "pressure").sum())
        shots = int((event_type == "shot").sum())
        goals = int(((event_type == "shot") & ((outcome == "goal") | (team_df["value"] == 3))).sum())
        rows.append(
            {
                "team": str(team),
                "events": int(len(team_df)),
                "passes": passes,
                "successful_passes": successful_passes,
                "pass_success_pct": _safe_pct(successful_passes, passes),
                "shots": shots,
                "goals": goals,
                "pressures": pressures,
                "duels": duels,
                "successful_duels": successful_duels,
                "duel_success_pct": _safe_pct(successful_duels, duels),
                "fouls": int(event_type.isin(["foul committed", "foul won"]).sum()),
                "recoveries": int((event_type == "ball recovery").sum()),
            }
        )
    return sorted(rows, key=lambda item: (-item["events"], item["team"]))


def _highlighted_player(events_df: pd.DataFrame) -> dict[str, Any]:
    if events_df.empty or "player" not in events_df.columns:
        return {}
    rows: list[dict[str, Any]] = []
    for (player, team), player_df in events_df.groupby(["player", "team"], dropna=False):
        event_type = player_df["event_type_norm"]
        rows.append(
            {
                "player": str(player),
                "team": str(team),
                "events": int(len(player_df)),
                "shots": int((event_type == "shot").sum()),
                "goals": int(((event_type == "shot") & ((player_df["outcome_norm"] == "goal") | (player_df["value"] == 3))).sum()),
            }
        )
    return sorted(rows, key=lambda item: (-item["goals"], -item["shots"], -item["events"]))[0] if rows else {}


def _window_metrics(events_df: pd.DataFrame, match_id: str, start: int, end: int) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    match_df = events_df[events_df["match_id"] == str(match_id)]
    window_df = match_df[(match_df["minute"] >= start) & (match_df["minute"] < end)].copy()
    cumulative_df = match_df[match_df["minute"] < end].copy()

    event_type = window_df.get("event_type_norm", pd.Series(dtype=str))
    outcome = window_df.get("outcome_norm", pd.Series(dtype=str))
    teams = _team_metrics(window_df)
    metrics = {
        "match_id": str(match_id),
        "window_start_minute": start,
        "window_end_minute": end,
        "window_minutes": end - start,
        "total_events": int(len(window_df)),
        "total_shots": int((event_type == "shot").sum()) if not window_df.empty else 0,
        "total_goals": int(((event_type == "shot") & ((outcome == "goal") | (window_df["value"] == 3))).sum()) if not window_df.empty else 0,
        "total_pressures": int((event_type == "pressure").sum()) if not window_df.empty else 0,
        "teams": teams,
        "highlighted_player": _highlighted_player(window_df),
    }

    cumulative_type = cumulative_df.get("event_type_norm", pd.Series(dtype=str))
    cumulative_outcome = cumulative_df.get("outcome_norm", pd.Series(dtype=str))
    cumulative = {
        "match_id": str(match_id),
        "up_to_minute": end,
        "total_events": int(len(cumulative_df)),
        "total_shots": int((cumulative_type == "shot").sum()) if not cumulative_df.empty else 0,
        "total_goals": int(((cumulative_type == "shot") & ((cumulative_outcome == "goal") | (cumulative_df["value"] == 3))).sum()) if not cumulative_df.empty else 0,
        "teams": _team_metrics(cumulative_df),
    }

    event_cols = ["timestamp", "minute", "second", "team", "player", "event_type", "outcome", "zone", "value"]
    available_cols = [column for column in event_cols if column in window_df.columns]
    sort_cols = [column for column in ["minute", "second", "timestamp"] if column in window_df.columns]
    sorted_df = window_df.sort_values(sort_cols, na_position="last") if sort_cols else window_df
    event_records = sorted_df[available_cols].head(25).to_dict(orient="records") if available_cols else []
    return metrics, event_records, cumulative


def _match_clock_seconds(match_df: pd.DataFrame) -> int:
    if match_df.empty:
        return 0
    minute = pd.to_numeric(match_df["minute"], errors="coerce").fillna(0).astype(int)
    second = pd.to_numeric(match_df.get("second", 0), errors="coerce").fillna(0).astype(int)
    return int((minute * 60 + second.clip(lower=0, upper=59)).max())


def report_window_status(
    events_df: pd.DataFrame | None = None,
    window_minutes: int = REPORT_WINDOW_MINUTES,
) -> pd.DataFrame:
    """Return open, pending and generated status for match-minute windows."""
    normalized_events = _normalize_events(_read_parquet(EVENTS_FILE) if events_df is None else events_df)
    existing_df = load_incremental_segments()
    existing_keys = {
        (str(row.match_id), int(row.window_start_minute), int(row.window_end_minute))
        for row in existing_df.itertuples(index=False)
    }
    if normalized_events.empty:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for match_id, match_df in normalized_events.groupby("match_id"):
        current_seconds = _match_clock_seconds(match_df)
        starts = sorted(((match_df["minute"] // window_minutes) * window_minutes).unique())
        for start in starts:
            start = int(start)
            end = start + window_minutes
            window_events = match_df[(match_df["minute"] >= start) & (match_df["minute"] < end)]
            if window_events.empty:
                continue
            is_closed = current_seconds >= end * 60
            is_generated = (str(match_id), start, end) in existing_keys
            rows.append(
                {
                    "match_id": str(match_id),
                    "window_start_minute": start,
                    "window_end_minute": end,
                    "window_minutes": window_minutes,
                    "event_count": int(len(window_events)),
                    "current_match_minute": int(current_seconds // 60),
                    "current_match_second": int(current_seconds % 60),
                    "is_closed": bool(is_closed),
                    "is_generated": bool(is_generated),
                    "status": "generada" if is_generated else ("pendiente" if is_closed else "abierta"),
                }
            )
    return pd.DataFrame(rows).sort_values(["match_id", "window_start_minute"]) if rows else pd.DataFrame()


def _closed_windows(events_df: pd.DataFrame) -> list[tuple[str, int, int]]:
    status_df = report_window_status(events_df, REPORT_WINDOW_MINUTES)
    if status_df.empty:
        return []
    return [
        (str(row.match_id), int(row.window_start_minute), int(row.window_end_minute))
        for row in status_df[status_df["is_closed"]].itertuples(index=False)
    ]


def generate_missing_report_segments(
    max_windows: int | None = None,
    progress_callback: ProgressCallback | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Generate missing fragments for closed 5-minute windows."""
    _emit_progress(progress_callback, "inicio", "Buscando ventanas cerradas pendientes.")
    events_df = _normalize_events(_read_parquet(EVENTS_FILE))
    existing_df = load_incremental_segments()
    if events_df.empty:
        return existing_df, []

    existing_keys = {
        (str(row.match_id), int(row.window_start_minute), int(row.window_end_minute))
        for row in existing_df.itertuples(index=False)
    }
    pending_windows = [window for window in _closed_windows(events_df) if window not in existing_keys]
    generated_rows: list[dict[str, Any]] = []

    for match_id, start, end in pending_windows:
        if max_windows is not None and len(generated_rows) >= max_windows:
            break

        _emit_progress(progress_callback, "ventana", f"Generando fragmento minutos {start}-{end}.")
        metrics, event_records, cumulative = _window_metrics(events_df, match_id, start, end)
        result = run_window_analysis_workflow(
            match_id=match_id,
            window_start_minute=start,
            window_end_minute=end,
            window_metrics=metrics,
            window_events=event_records,
            cumulative_metrics=cumulative,
            progress_callback=progress_callback,
        )
        generated_rows.append(
            {
                "segment_id": f"{match_id}_{start}_{end}",
                "match_id": str(match_id),
                "window_start_minute": start,
                "window_end_minute": end,
                "window_minutes": REPORT_WINDOW_MINUTES,
                "text": result.get("text", ""),
                "metrics_json": json.dumps(metrics, ensure_ascii=False, default=str),
                "rag_context": result.get("rag_context", ""),
                "llm_model": result.get("llm_model", ""),
                "trace_json": json.dumps(result.get("trace", []), ensure_ascii=False, default=str),
                "generated_at": pd.Timestamp(datetime.now()),
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
    numeric_columns = [
        "window_start_minute",
        "window_end_minute",
        "events",
        "goals",
        "passes",
        "successful_passes",
        "pass_success_pct",
        "carries",
        "dribbles",
        "successful_dribbles",
        "shots",
        "pressures",
        "duels",
        "successful_duels",
        "duel_success_pct",
        "interceptions",
        "successful_interceptions",
        "blocks",
        "clearances",
        "fouls",
        "recoveries",
        "offensive_index",
        "defensive_index",
        "avg_value",
        "batch_id",
        "window_minutes",
    ]
    for column in numeric_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    return df.sort_values(["window_start_minute", "team"]) if "window_start_minute" in df.columns else df
