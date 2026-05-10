"""Incremental report segment generation for closed match-minute windows."""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from graph_workflow import run_window_analysis_workflow
from settings import (
    EVENTS_FILE,
    INCREMENTAL_RAG_FILE,
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

RAG_COLUMNS = [
    "doc_id",
    "match_id",
    "window_start_minute",
    "window_end_minute",
    "document_text",
    "generated_at",
]

ProgressCallback = Callable[[str, str, dict[str, Any] | None], None]


def _emit_progress(
    progress_callback: ProgressCallback | None,
    stage: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    if progress_callback is None:
        return
    try:
        progress_callback(stage, message, details or {})
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


def load_incremental_rag_docs() -> pd.DataFrame:
    """Load the simple incremental RAG store built from processed match windows."""
    df = _read_parquet(INCREMENTAL_RAG_FILE)
    if df.empty:
        return pd.DataFrame(columns=RAG_COLUMNS)
    for column in RAG_COLUMNS:
        if column not in df.columns:
            df[column] = None
    df["window_start_minute"] = pd.to_numeric(df["window_start_minute"], errors="coerce").fillna(0).astype(int)
    df["window_end_minute"] = pd.to_numeric(df["window_end_minute"], errors="coerce").fillna(0).astype(int)
    df["document_text"] = df["document_text"].fillna("").astype(str)
    df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df[RAG_COLUMNS].sort_values(["match_id", "window_start_minute"])


def _normalize_segments_for_write(df: pd.DataFrame) -> pd.DataFrame:
    """Keep parquet dtypes stable when appending newly generated segments."""
    output_df = df.copy()
    for column in SEGMENT_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = None

    for column in ["window_start_minute", "window_end_minute", "window_minutes"]:
        output_df[column] = pd.to_numeric(output_df[column], errors="coerce").fillna(0).astype("int64")

    text_columns = [
        "segment_id",
        "match_id",
        "text",
        "metrics_json",
        "rag_context",
        "llm_model",
        "trace_json",
    ]
    for column in text_columns:
        output_df[column] = output_df[column].fillna("").astype(str)

    output_df["generated_at"] = pd.to_datetime(output_df["generated_at"], errors="coerce")
    return output_df[SEGMENT_COLUMNS]


def _write_segments(df: pd.DataFrame) -> None:
    INCREMENTAL_SEGMENTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    df = _normalize_segments_for_write(df).sort_values(["match_id", "window_start_minute"])
    tmp_file = INCREMENTAL_SEGMENTS_FILE.with_name(
        f".{INCREMENTAL_SEGMENTS_FILE.stem}_{uuid.uuid4().hex}.tmp.parquet"
    )
    df.to_parquet(tmp_file, index=False)
    os.replace(tmp_file, INCREMENTAL_SEGMENTS_FILE)


def _write_incremental_rag_docs(df: pd.DataFrame) -> None:
    INCREMENTAL_RAG_FILE.parent.mkdir(parents=True, exist_ok=True)
    output_df = df.copy()
    for column in RAG_COLUMNS:
        if column not in output_df.columns:
            output_df[column] = None
    output_df["window_start_minute"] = pd.to_numeric(
        output_df["window_start_minute"], errors="coerce"
    ).fillna(0).astype("int64")
    output_df["window_end_minute"] = pd.to_numeric(
        output_df["window_end_minute"], errors="coerce"
    ).fillna(0).astype("int64")
    output_df["generated_at"] = pd.to_datetime(output_df["generated_at"], errors="coerce")
    for column in ["doc_id", "match_id", "document_text"]:
        output_df[column] = output_df[column].fillna("").astype(str)
    output_df = output_df[RAG_COLUMNS].sort_values(["match_id", "window_start_minute"])
    tmp_file = INCREMENTAL_RAG_FILE.with_name(f".{INCREMENTAL_RAG_FILE.stem}_{uuid.uuid4().hex}.tmp.parquet")
    output_df.to_parquet(tmp_file, index=False)
    os.replace(tmp_file, INCREMENTAL_RAG_FILE)


def _normalize_events(events_df: pd.DataFrame) -> pd.DataFrame:
    if events_df.empty:
        return events_df
    events_df = events_df.copy()

    def column_or_default(column: str, default: Any) -> pd.Series:
        if column in events_df.columns:
            return events_df[column]
        return pd.Series(default, index=events_df.index)

    events_df["minute"] = pd.to_numeric(column_or_default("minute", 0), errors="coerce").fillna(0).astype(int)
    events_df["second"] = pd.to_numeric(column_or_default("second", 0), errors="coerce").fillna(0).astype(int)
    events_df["value"] = pd.to_numeric(column_or_default("value", 0), errors="coerce").fillna(0).astype(int)
    events_df["event_type_norm"] = column_or_default("event_type", "").astype(str).str.lower()
    events_df["outcome_norm"] = column_or_default("outcome", "").astype(str).str.lower()
    events_df["match_id"] = column_or_default("match_id", "").astype(str)
    return events_df


def _safe_pct(numerator: int, denominator: int) -> float:
    return round(numerator / denominator * 100, 2) if denominator else 0.0


def _offensive_index(events_df: pd.DataFrame) -> float:
    if events_df.empty:
        return 0.0
    event_type = events_df["event_type_norm"]
    outcome = events_df["outcome_norm"]
    score = (
        (((event_type == "pass") & (outcome == "success")).sum() * 0.8)
        + ((event_type == "carry").sum() * 0.6)
        + (((event_type == "dribble") & (outcome == "success")).sum() * 1.2)
        + ((event_type == "shot").sum() * 2.0)
        + (((event_type == "shot") & (outcome == "goal")).sum() * 5.0)
    )
    return round(float(score), 2)


def _defensive_index(events_df: pd.DataFrame) -> float:
    if events_df.empty:
        return 0.0
    event_type = events_df["event_type_norm"]
    outcome = events_df["outcome_norm"]
    score = (
        ((event_type == "pressure").sum() * 0.5)
        + (((event_type == "duel") & (outcome == "success")).sum() * 1.2)
        + (((event_type == "interception") & (outcome == "success")).sum() * 1.5)
        + ((event_type == "block").sum() * 1.0)
        + ((event_type == "clearance").sum() * 0.8)
    )
    return round(float(score), 2)


def _team_window_metrics(events_df: pd.DataFrame) -> list[dict[str, Any]]:
    if events_df.empty:
        return []
    grouped = []
    for team, team_df in events_df.groupby("team", dropna=False):
        event_type = team_df["event_type_norm"]
        outcome = team_df["outcome_norm"]
        passes = int((event_type == "pass").sum())
        successful_passes = int(((event_type == "pass") & (outcome == "success")).sum())
        duels = int((event_type == "duel").sum())
        successful_duels = int(((event_type == "duel") & (outcome == "success")).sum())
        grouped.append(
            {
                "team": team,
                "events": int(len(team_df)),
                "goals": int(((event_type == "shot") & (outcome == "goal")).sum()),
                "shots": int((event_type == "shot").sum()),
                "passes": passes,
                "successful_passes": successful_passes,
                "pass_success_pct": _safe_pct(successful_passes, passes),
                "carries": int((event_type == "carry").sum()),
                "dribbles": int((event_type == "dribble").sum()),
                "successful_dribbles": int(((event_type == "dribble") & (outcome == "success")).sum()),
                "pressures": int((event_type == "pressure").sum()),
                "duels": duels,
                "successful_duels": successful_duels,
                "duel_success_pct": _safe_pct(successful_duels, duels),
                "interceptions": int((event_type == "interception").sum()),
                "successful_interceptions": int(((event_type == "interception") & (outcome == "success")).sum()),
                "blocks": int((event_type == "block").sum()),
                "clearances": int((event_type == "clearance").sum()),
                "fouls": int(event_type.isin(["foul committed", "foul won"]).sum()),
                "recoveries": int((event_type == "ball recovery").sum()),
                "offensive_index": _offensive_index(team_df),
                "defensive_index": _defensive_index(team_df),
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
                "goals": int(((event_type == "shot") & (player_df["outcome_norm"] == "goal")).sum()),
            }
        )
    return sorted(rows, key=lambda item: (-item["goals"], -item["shots"], -item["participations"]))[0] if rows else {}


def _activity_leader(teams: list[dict[str, Any]]) -> dict[str, Any]:
    if not teams:
        return {}
    return max(
        teams,
        key=lambda item: (
            item.get("events", 0),
            item.get("passes", 0),
            item.get("shots", 0),
            item.get("offensive_index", 0),
            item.get("defensive_index", 0),
            item.get("goals", 0),
        ),
    )


def _impact_leader(teams: list[dict[str, Any]]) -> dict[str, Any]:
    if not teams:
        return {}
    return max(
        teams,
        key=lambda item: (
            item.get("goals", 0),
            item.get("offensive_index", 0),
            item.get("shots", 0),
            item.get("defensive_index", 0),
            item.get("pressures", 0),
            item.get("events", 0),
        ),
    )


def _team_fact_line(teams: list[dict[str, Any]]) -> str:
    if not teams:
        return "Sin actividad de equipos en la ventana."
    return "; ".join(
        (
            f"{team.get('team', 'N/D')}: {team.get('events', 0)} eventos, "
            f"{team.get('passes', 0)} pases, {team.get('shots', 0)} tiros, "
            f"{team.get('pressures', 0)} presiones, "
            f"indice ofensivo {team.get('offensive_index', 0)}, "
            f"indice defensivo {team.get('defensive_index', 0)}"
        )
        for team in teams
    )


def _event_type_breakdown(events_df: pd.DataFrame) -> list[dict[str, Any]]:
    if events_df.empty:
        return []
    breakdown = (
        events_df.groupby(["team", "event_type"], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values(["count", "team", "event_type"], ascending=[False, True, True])
    )
    return [
        {"team": row["team"], "event_type": row["event_type"], "count": int(row["count"])}
        for row in breakdown.to_dict(orient="records")
    ]


def _representative_window_events(window_events: pd.DataFrame, limit: int = 30) -> list[dict[str, Any]]:
    event_cols = ["timestamp", "minute", "second", "team", "player", "event_type", "outcome", "zone", "value"]
    available_cols = [col for col in event_cols if col in window_events.columns]
    if window_events.empty or not available_cols:
        return []

    sort_cols = [col for col in ["minute", "second", "timestamp"] if col in window_events.columns]
    sorted_events = window_events.sort_values(sort_cols, na_position="last") if sort_cols else window_events

    impactful_types = {"shot", "pressure", "duel", "interception", "block", "clearance", "dribble"}
    impactful_events = sorted_events[sorted_events["event_type_norm"].isin(impactful_types)]
    head_count = max(limit - len(impactful_events.head(limit // 2)), 0)
    sample = pd.concat([impactful_events.head(limit // 2), sorted_events.head(head_count)], ignore_index=True)
    sample = sample.drop_duplicates(keep="first").head(limit)

    return sample[available_cols].to_dict(orient="records")


def _format_team_metrics_for_rag(teams: list[dict[str, Any]]) -> str:
    if not teams:
        return "- Sin actividad por equipos."
    lines = []
    for team in teams:
        lines.append(
            "- {team}: {events} eventos, {passes} pases ({pass_pct:.1f}% acierto), "
            "{carries} conducciones, {dribbles} regates, {shots} tiros, {goals} goles, "
            "{pressures} presiones, {duels} duelos ({duel_pct:.1f}% ganados), "
            "indice ofensivo {off:.1f}, indice defensivo {defn:.1f}.".format(
                team=team.get("team", "N/D"),
                events=int(team.get("events", 0)),
                passes=int(team.get("passes", 0)),
                pass_pct=float(team.get("pass_success_pct", 0)),
                carries=int(team.get("carries", 0)),
                dribbles=int(team.get("dribbles", 0)),
                shots=int(team.get("shots", 0)),
                goals=int(team.get("goals", 0)),
                pressures=int(team.get("pressures", 0)),
                duels=int(team.get("duels", 0)),
                duel_pct=float(team.get("duel_success_pct", 0)),
                off=float(team.get("offensive_index", 0)),
                defn=float(team.get("defensive_index", 0)),
            )
        )
    return "\n".join(lines)


def _format_event_breakdown_for_rag(window_metrics: dict[str, Any], limit: int = 8) -> str:
    breakdown = window_metrics.get("event_type_breakdown", [])
    if not breakdown:
        return "- Sin desglose de eventos."
    lines = []
    for item in breakdown[:limit]:
        lines.append(
            f"- {item.get('team', 'N/D')}: {item.get('count', 0)} eventos tipo {item.get('event_type', 'N/D')}."
        )
    return "\n".join(lines)


def _format_event_samples_for_rag(window_events: list[dict[str, Any]], limit: int = 8) -> str:
    if not window_events:
        return "- Sin ejemplos de eventos relevantes."
    lines = []
    for event in window_events[:limit]:
        minute = int(event.get("minute", 0) or 0)
        second = int(event.get("second", 0) or 0)
        lines.append(
            "- {minute}:{second:02d} {team} - {player}: {event_type} ({outcome}) en {zone}.".format(
                minute=minute,
                second=second,
                team=event.get("team", "N/D"),
                player=event.get("player", "N/D"),
                event_type=event.get("event_type", "N/D"),
                outcome=event.get("outcome", "N/D"),
                zone=event.get("zone", "N/D"),
            )
        )
    return "\n".join(lines)


def build_window_rag_document(
    window_metrics: dict[str, Any],
    window_events: list[dict[str, Any]],
    cumulative_metrics: dict[str, Any],
) -> str:
    """Build the textual document inserted into the incremental RAG store."""
    start = int(window_metrics.get("window_start_minute", 0))
    end = int(window_metrics.get("window_end_minute", 0))
    leader = window_metrics.get("activity_leader", {}) or {}
    impact = window_metrics.get("impact_leader", {}) or {}
    highlighted = window_metrics.get("highlighted_player", {}) or {}
    return (
        f"VENTANA {start}-{end} MINUTOS\n"
        f"Partido: {window_metrics.get('match_id', 'N/D')}.\n"
        f"Eventos totales de la ventana: {window_metrics.get('total_events', 0)}. "
        f"Tiros: {window_metrics.get('total_shots', 0)}. "
        f"Goles: {window_metrics.get('total_goals', 0)}. "
        f"Presiones: {window_metrics.get('total_pressures', 0)}. "
        f"Indice ofensivo total: {float(window_metrics.get('offensive_index', 0)):.1f}. "
        f"Indice defensivo total: {float(window_metrics.get('defensive_index', 0)):.1f}.\n"
        f"Equipo mas activo: {leader.get('team', 'N/D')} con {leader.get('events', 0)} eventos.\n"
        f"Equipo/impacto destacado: {impact.get('team', 'N/D')} "
        f"(ofensivo {float(impact.get('offensive_index', 0)):.1f}, "
        f"defensivo {float(impact.get('defensive_index', 0)):.1f}, "
        f"tiros {impact.get('shots', 0)}, goles {impact.get('goals', 0)}).\n"
        f"Jugador destacado de la ventana: {highlighted.get('player', 'N/D')} "
        f"({highlighted.get('team', 'N/D')}) con {highlighted.get('participations', 0)} participaciones, "
        f"{highlighted.get('shots', 0)} tiros y {highlighted.get('goals', 0)} goles.\n"
        "Metricas por equipo:\n"
        f"{_format_team_metrics_for_rag(window_metrics.get('teams', []))}\n"
        "Desglose principal de eventos:\n"
        f"{_format_event_breakdown_for_rag(window_metrics)}\n"
        "Eventos representativos:\n"
        f"{_format_event_samples_for_rag(window_events)}\n"
        f"Contexto acumulado hasta el minuto {cumulative_metrics.get('up_to_minute', end)}: "
        f"{cumulative_metrics.get('total_events', 0)} eventos, "
        f"{cumulative_metrics.get('total_shots', 0)} tiros, "
        f"{cumulative_metrics.get('total_goals', 0)} goles, "
        f"indice ofensivo acumulado {float(cumulative_metrics.get('offensive_index', 0)):.1f}, "
        f"indice defensivo acumulado {float(cumulative_metrics.get('defensive_index', 0)):.1f}."
    )


def upsert_incremental_rag_document(
    match_id: str,
    start_minute: int,
    end_minute: int,
    document_text: str,
) -> None:
    """Insert or replace one window document in the incremental RAG store."""
    docs_df = load_incremental_rag_docs()
    doc_id = f"{match_id}_{start_minute}_{end_minute}"
    new_row = pd.DataFrame(
        [
            {
                "doc_id": doc_id,
                "match_id": match_id,
                "window_start_minute": start_minute,
                "window_end_minute": end_minute,
                "document_text": document_text,
                "generated_at": pd.Timestamp(datetime.now()),
            }
        ],
        columns=RAG_COLUMNS,
    )
    output_df = pd.concat([docs_df, new_row], ignore_index=True)
    output_df = output_df.drop_duplicates(["match_id", "window_start_minute", "window_end_minute"], keep="last")
    _write_incremental_rag_docs(output_df)


def build_incremental_rag_context(
    match_id: str,
    start_minute: int,
    end_minute: int,
    current_document: str,
    previous_windows: int = 3,
) -> str:
    """Retrieve a compact incremental RAG context for the current window."""
    docs_df = load_incremental_rag_docs()
    context_parts = [f"DOCUMENTO RAG DE LA VENTANA ACTUAL ({start_minute}-{end_minute}):\n{current_document}"]
    if not docs_df.empty:
        previous_df = docs_df[
            (docs_df["match_id"] == str(match_id))
            & (docs_df["window_end_minute"] <= int(start_minute))
        ].sort_values("window_start_minute").tail(previous_windows)
        if not previous_df.empty:
            previous_text = "\n\n".join(
                f"DOCUMENTO RAG PREVIO ({int(row.window_start_minute)}-{int(row.window_end_minute)}):\n"
                f"{row.document_text}"
                for row in previous_df.itertuples(index=False)
            )
            context_parts.append(f"CONTEXTO RAG ACUMULADO RECIENTE:\n{previous_text}")
    return "\n\n".join(context_parts)


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
    team_metrics = _team_window_metrics(window_events)
    activity_leader = _activity_leader(team_metrics)
    impact_leader = _impact_leader(team_metrics)
    window_metrics = {
        "match_id": match_id,
        "window_start_minute": start_minute,
        "window_end_minute": end_minute,
        "window_minutes": end_minute - start_minute,
        "total_events": int(len(window_events)),
        "total_shots": int((event_type == "shot").sum()) if not window_events.empty else 0,
        "total_goals": int(((event_type == "shot") & (window_events["outcome_norm"] == "goal")).sum()) if not window_events.empty else 0,
        "total_fouls": int(event_type.isin(["foul committed", "foul won"]).sum()) if not window_events.empty else 0,
        "total_pressures": int((event_type == "pressure").sum()) if not window_events.empty else 0,
        "offensive_index": _offensive_index(window_events),
        "defensive_index": _defensive_index(window_events),
        "teams": team_metrics,
        "activity_leader": activity_leader,
        "impact_leader": impact_leader,
        "dominant_team": activity_leader,
        "team_fact_line": _team_fact_line(team_metrics),
        "event_type_breakdown": _event_type_breakdown(window_events),
        "highlighted_player": _highlighted_player(window_events),
    }

    cumulative_type = cumulative_events.get("event_type_norm", pd.Series(dtype=str))
    cumulative_team_metrics = _team_window_metrics(cumulative_events)
    cumulative_metrics = {
        "match_id": match_id,
        "up_to_minute": end_minute,
        "total_events": int(len(cumulative_events)),
        "total_shots": int((cumulative_type == "shot").sum()) if not cumulative_events.empty else 0,
        "total_goals": int(((cumulative_type == "shot") & (cumulative_events["outcome_norm"] == "goal")).sum()) if not cumulative_events.empty else 0,
        "offensive_index": _offensive_index(cumulative_events),
        "defensive_index": _defensive_index(cumulative_events),
        "teams": cumulative_team_metrics,
        "activity_leader": _activity_leader(cumulative_team_metrics),
        "impact_leader": _impact_leader(cumulative_team_metrics),
        "dominant_team": _activity_leader(cumulative_team_metrics),
        "team_fact_line": _team_fact_line(cumulative_team_metrics),
    }

    event_records = _representative_window_events(window_events)
    return window_metrics, event_records, cumulative_metrics


def _match_clock_seconds(match_df: pd.DataFrame) -> int:
    if match_df.empty:
        return 0
    minute_source = match_df["minute"] if "minute" in match_df.columns else pd.Series(0, index=match_df.index)
    second_source = match_df["second"] if "second" in match_df.columns else pd.Series(0, index=match_df.index)
    minute = pd.to_numeric(minute_source, errors="coerce").fillna(0).astype(int)
    second = pd.to_numeric(second_source, errors="coerce").fillna(0).astype(int)
    return int((minute * 60 + second.clip(lower=0, upper=59)).max())


def report_window_status(
    events_df: pd.DataFrame | None = None,
    window_minutes: int = REPORT_WINDOW_MINUTES,
) -> pd.DataFrame:
    """Return closed/open/generated status for observed match-minute windows."""
    normalized_events = _normalize_events(_read_parquet(EVENTS_FILE) if events_df is None else events_df)
    existing_df = load_incremental_segments()
    existing_keys = {
        (
            str(row.match_id),
            int(row.window_start_minute),
            int(row.window_end_minute),
        )
        for row in existing_df.itertuples(index=False)
    }
    if normalized_events.empty:
        return pd.DataFrame(
            columns=[
                "match_id",
                "window_start_minute",
                "window_end_minute",
                "window_minutes",
                "event_count",
                "current_match_minute",
                "current_match_second",
                "is_closed",
                "is_generated",
                "status",
            ]
        )

    rows: list[dict[str, Any]] = []
    for match_id, match_df in normalized_events.groupby("match_id"):
        current_seconds = _match_clock_seconds(match_df)
        current_minute = current_seconds // 60
        current_second = current_seconds % 60
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
                    "current_match_minute": int(current_minute),
                    "current_match_second": int(current_second),
                    "is_closed": bool(is_closed),
                    "is_generated": bool(is_generated),
                    "status": "generada" if is_generated else ("pendiente" if is_closed else "abierta"),
                }
            )

    return pd.DataFrame(rows).sort_values(["match_id", "window_start_minute"])


def _closed_windows(events_df: pd.DataFrame, window_minutes: int) -> list[tuple[str, int, int]]:
    windows: list[tuple[str, int, int]] = []
    if events_df.empty:
        return windows

    status_df = report_window_status(events_df, window_minutes)
    if status_df.empty:
        return windows

    closed_df = status_df[status_df["is_closed"]]
    for row in closed_df.itertuples(index=False):
        windows.append((str(row.match_id), int(row.window_start_minute), int(row.window_end_minute)))
    return windows


def generate_missing_report_segments(
    max_windows: int | None = None,
    progress_callback: ProgressCallback | None = None,
) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    """Generate and persist missing incremental segments for closed windows."""
    _emit_progress(progress_callback, "inicio", "Comprobando ventanas cerradas de partido.")
    events_df = _normalize_events(_read_parquet(EVENTS_FILE))
    existing_df = load_incremental_segments()
    if events_df.empty:
        _emit_progress(progress_callback, "sin_eventos", "Aun no hay eventos consolidados para narrar.")
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
    closed_windows = _closed_windows(events_df, REPORT_WINDOW_MINUTES)
    pending_windows = [window for window in closed_windows if window not in existing_keys]
    _emit_progress(
        progress_callback,
        "deteccion",
        f"Ventanas cerradas: {len(closed_windows)}; pendientes de texto: {len(pending_windows)}.",
        {"closed_windows": closed_windows, "pending_windows": pending_windows},
    )
    for match_id, start, end in pending_windows:
        if max_windows is not None and len(generated_rows) >= max_windows:
            break

        generation_trace = [
            f"Ventana cerrada detectada para minutos {start}-{end}.",
            "Inicio de construccion del payload de metricas y eventos.",
        ]
        _emit_progress(
            progress_callback,
            "ventana",
            f"Generando narrativa para minutos {start}-{end}.",
            {"match_id": match_id, "window_start_minute": start, "window_end_minute": end},
        )
        window_metrics, window_events, cumulative_metrics = build_window_payload(events_df, match_id, start, end)
        generation_trace.append(
            f"Payload preparado: {window_metrics.get('total_events', 0)} eventos, "
            f"{window_metrics.get('total_shots', 0)} tiros, "
            f"{len(window_metrics.get('teams', []))} equipos."
        )
        _emit_progress(
            progress_callback,
            "payload",
            f"Payload listo para {start}-{end}: {window_metrics.get('total_events', 0)} eventos.",
            {"window_metrics": window_metrics},
        )
        rag_document = build_window_rag_document(window_metrics, window_events, cumulative_metrics)
        upsert_incremental_rag_document(match_id, start, end, rag_document)
        incremental_rag_context = build_incremental_rag_context(match_id, start, end, rag_document)
        generation_trace.append("Documento RAG incremental de la ventana guardado y recuperado.")
        _emit_progress(
            progress_callback,
            "rag_incremental",
            f"Documento RAG incremental listo para minutos {start}-{end}.",
            {"rag_chars": len(incremental_rag_context)},
        )
        try:
            result = run_window_analysis_workflow(
                match_id=match_id,
                window_start_minute=start,
                window_end_minute=end,
                window_metrics=window_metrics,
                window_events=window_events,
                cumulative_metrics=cumulative_metrics,
                rag_context=incremental_rag_context,
                progress_callback=progress_callback,
            )
        except Exception as exc:  # noqa: BLE001
            fallback_text = (
                f"Minutos {start}-{end}: no se pudo completar el flujo RAG/LLM. "
                f"Se registraron {window_metrics.get('total_events', 0)} eventos, "
                f"{window_metrics.get('total_shots', 0)} tiros y "
                f"{window_metrics.get('total_goals', 0)} goles. "
                "El segmento queda guardado como fallback determinista para no bloquear el informe."
            )
            result = {
                "text": fallback_text,
                "rag_context": "",
                "llm_model": f"fallback:{exc}",
                "trace": [f"ERROR inesperado durante RAG/LLM: {exc}"],
            }
            _emit_progress(
                progress_callback,
                "error",
                f"Error generando minutos {start}-{end}; se guarda fallback determinista: {exc}",
                {"error": str(exc)},
            )
        result_trace = generation_trace + list(result.get("trace", []))
        generated_rows.append(
            {
                "segment_id": f"{match_id}_{start}_{end}",
                "match_id": match_id,
                "window_start_minute": start,
                "window_end_minute": end,
                "window_minutes": REPORT_WINDOW_MINUTES,
                "text": result.get("text", ""),
                "metrics_json": json.dumps(window_metrics, ensure_ascii=False, default=str),
                "rag_context": result.get("rag_context", incremental_rag_context),
                "llm_model": result.get("llm_model", ""),
                "trace_json": json.dumps(result_trace, ensure_ascii=False, default=str),
                "generated_at": pd.Timestamp(datetime.now()),
            }
        )
        _emit_progress(
            progress_callback,
            "segmento_listo",
            f"Segmento minutos {start}-{end} preparado para escritura.",
            {"match_id": match_id, "window_start_minute": start, "window_end_minute": end},
        )

    if generated_rows:
        new_df = pd.DataFrame(generated_rows, columns=SEGMENT_COLUMNS)
        output_df = pd.concat([existing_df, new_df], ignore_index=True)
        output_df = output_df.drop_duplicates(["match_id", "window_start_minute", "window_end_minute"], keep="last")
        _write_segments(output_df)
        _emit_progress(
            progress_callback,
            "guardado",
            f"Segmentos guardados en {INCREMENTAL_SEGMENTS_FILE}.",
            {"generated_count": len(generated_rows)},
        )
        return load_incremental_segments(), generated_rows

    _emit_progress(progress_callback, "sin_pendientes", "No hay ventanas cerradas pendientes de texto.")
    return existing_df, []


def load_window_metrics() -> pd.DataFrame:
    """Load Spark-generated metrics by report window."""
    df = _read_parquet(WINDOW_METRICS_FILE)
    if df.empty:
        return pd.DataFrame()
    int_columns = [
        "window_start_minute",
        "window_end_minute",
        "events",
        "shots",
        "goals",
        "passes",
        "successful_passes",
        "carries",
        "dribbles",
        "successful_dribbles",
        "pressures",
        "duels",
        "successful_duels",
        "interceptions",
        "successful_interceptions",
        "blocks",
        "clearances",
        "fouls",
        "recoveries",
    ]
    for column in int_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in ["pass_success_pct", "duel_success_pct", "offensive_index", "defensive_index", "avg_value"]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    return df.sort_values(["window_start_minute", "team"])
