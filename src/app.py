"""Streamlit dashboard for real-time match analytics.

The dashboard reads the consolidated parquet files produced by
`streaming_pipeline.py`:
- `output/processed/events.parquet`
- `output/aggregates/team_metrics.parquet`
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from incremental_report import report_window_status
from settings import (
    EVENTS_FILE,
    INCREMENTAL_SEGMENTS_FILE,
    METRICS_FILE,
    REPORT_PDF_FILE,
    REPORT_WINDOW_MINUTES,
    WINDOW_METRICS_FILE,
)

try:
    import altair as alt

    ALTAIR_AVAILABLE = True
except ImportError:
    alt = None
    ALTAIR_AVAILABLE = False

try:
    from streamlit_autorefresh import st_autorefresh

    AUTOREFRESH_AVAILABLE = True
except ImportError:
    AUTOREFRESH_AVAILABLE = False


PROJECT_ROOT = Path(__file__).resolve().parent.parent
MADRID_TZ = ZoneInfo("Europe/Madrid")
MATCH_FILE = PROJECT_ROOT / "data" / "static" / "match.json"
TEAMS_FILE = PROJECT_ROOT / "data" / "static" / "teams.json"

EVENT_COLUMNS = [
    "schema_version",
    "event_id",
    "timestamp",
    "match_id",
    "team",
    "player",
    "event_type",
    "zone",
    "minute",
    "second",
    "outcome",
    "period",
    "snapshot_time",
    "batch_id",
]

EVENT_TYPE_COLUMNS = [
    "passes",
    "carries",
    "dribbles",
    "shots",
    "pressures",
    "duels",
    "interceptions",
    "blocks",
    "clearances",
]

EVENT_TYPE_LABELS = {
    "pass": "Pase",
    "carry": "Conduccion",
    "dribble": "Regate",
    "shot": "Tiro",
    "pressure": "Presion",
    "duel": "Duelo",
    "interception": "Intercepcion",
    "block": "Bloqueo",
    "clearance": "Despeje",
}
EVENT_TYPE_ORDER = list(EVENT_TYPE_LABELS.keys())
EVENT_TYPE_LABEL_ORDER = list(EVENT_TYPE_LABELS.values())

OFFENSIVE_EVENT_TYPES = {"pass", "carry", "dribble", "shot"}
DEFENSIVE_EVENT_TYPES = {"pressure", "duel", "interception", "block", "clearance"}

METRIC_COLUMNS = [
    "team",
    "total_events",
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
    "snapshot_time",
    "batch_id",
    "snapshot_event_count",
]

WINDOW_METRIC_COLUMNS = [
    "match_id",
    "window_start_minute",
    "window_end_minute",
    "team",
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
    "snapshot_time",
    "batch_id",
    "window_minutes",
]

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


def _load_json_file(path: Path, fallback):
    """Read a JSON file and return fallback when it is not available."""
    if not path.exists() or not path.is_file():
        return fallback
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        st.warning(f"No se pudo leer {path.name}: {exc}")
        return fallback


def load_match_metadata() -> tuple[dict, list[dict]]:
    """Load static match and team metadata used in the dashboard header."""
    match_data = _load_json_file(MATCH_FILE, {})
    teams_data = _load_json_file(TEAMS_FILE, [])
    return match_data if isinstance(match_data, dict) else {}, teams_data if isinstance(teams_data, list) else []


def _norm_text(value: object) -> str:
    return str(value or "").strip().lower().replace(" cf", "").replace(" rcd", "")


def _find_team_profile(team_name: str, teams_data: list[dict]) -> dict:
    """Find the closest static team profile for a match team name."""
    wanted = _norm_text(team_name)
    for team in teams_data:
        candidates = [
            team.get("name"),
            team.get("short_name"),
            team.get("team_id", "").replace("_", " "),
        ]
        if any(wanted and (wanted == _norm_text(candidate) or wanted in _norm_text(candidate)) for candidate in candidates):
            return team
    return {}


def _format_capacity(value: object) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "N/D"


def _format_match_kickoff(match_data: dict) -> str:
    """Format the match kickoff as local Madrid time."""
    match_date = str(match_data.get("match_date") or "")
    kick_off = str(match_data.get("kick_off") or "").split(".")[0]
    if not match_date:
        return "N/D"
    try:
        dt = datetime.fromisoformat(f"{match_date}T{kick_off or '00:00:00'}").replace(tzinfo=MADRID_TZ)
        return dt.strftime("%d/%m/%Y %H:%M") + " Europe/Madrid"
    except ValueError:
        return f"{match_date} {kick_off}".strip()


def _format_madrid_datetime(value: object, assume_utc_when_naive: bool = True) -> str:
    """Format generated timestamps in Spain/Madrid time."""
    if value is None or pd.isna(value):
        return "N/D"
    ts = pd.to_datetime(value, errors="coerce")
    if pd.isna(ts):
        return str(value)
    if ts.tzinfo is None:
        if assume_utc_when_naive:
            ts = ts.tz_localize(timezone.utc)
        else:
            ts = ts.tz_localize(MADRID_TZ)
    return ts.tz_convert(MADRID_TZ).strftime("%d/%m/%Y %H:%M:%S Europe/Madrid")


def _render_team_profile_content(team_profile: dict, fallback_name: str) -> None:
    """Render static team information inside a modal or fallback container."""
    if team_profile.get("crest_url"):
        st.image(team_profile["crest_url"], width=92)
    st.write(f"Ciudad: {team_profile.get('city', 'N/D')}")
    st.write(f"Pais: {team_profile.get('country', 'N/D')}")
    st.write(f"Entrenador: {team_profile.get('coach', 'N/D')}")
    st.write(f"Capitan: {team_profile.get('captain', 'N/D')}")
    st.write(f"Sistema preferente: {team_profile.get('preferred_shape', 'N/D')}")
    stadium = team_profile.get("stadium") or {}
    st.write(f"Estadio habitual: {stadium.get('name', 'N/D')}")
    st.write(f"Capacidad: {_format_capacity(stadium.get('capacity'))}")
    st.write(f"Superficie: {stadium.get('surface', 'N/D')}")
    st.write(f"Ultimos 5 partidos: {team_profile.get('last_5_league_matches', 'N/D')}")
    st.write(f"Estilo: {team_profile.get('playing_style', 'N/D')}")

    strengths = team_profile.get("strengths") or []
    risks = team_profile.get("risks") or []
    if strengths:
        st.markdown("**Fortalezas**")
        for item in strengths:
            st.write(f"- {item}")
    if risks:
        st.markdown("**Riesgos**")
        for item in risks:
            st.write(f"- {item}")

    injured_count = team_profile.get("injured_players_count")
    injured_note = team_profile.get("injured_players_note")
    if injured_count is not None:
        st.write(f"Bajas registradas: {injured_count}")
    if injured_note:
        st.caption(injured_note)

    if not team_profile:
        st.info(f"No hay datos ampliados para {fallback_name}.")


def _render_team_dialog(team_profile: dict, fallback_name: str) -> None:
    """Open a modal with static team information."""
    dialog = getattr(st, "dialog", None) or getattr(st, "experimental_dialog", None)
    title = team_profile.get("name") or fallback_name or "Equipo"

    if dialog is None:
        st.warning("Tu version de Streamlit no soporta modales; muestro la informacion en la pagina.")
        _render_team_profile_content(team_profile, fallback_name)
        return

    @dialog(title)
    def _dialog() -> None:
        _render_team_profile_content(team_profile, fallback_name)

    _dialog()


def _team_goal_count(events_df: pd.DataFrame, team_name: str) -> int:
    """Count simulated goals for a team from the processed event stream."""
    if events_df.empty:
        return 0

    team_norm = events_df["team"].fillna("").astype(str).map(_norm_text)
    event_type = events_df["event_type"].fillna("").astype(str).str.lower()
    outcome = events_df["outcome"].fillna("").astype(str).str.lower()
    target = _norm_text(team_name)
    team_matches = team_norm.map(lambda name: bool(target) and (name == target or target in name or name in target))
    goals = team_matches & (event_type == "shot") & (outcome == "goal")
    return int(goals.sum())


def _latest_event_clock(events_df: pd.DataFrame) -> str:
    """Return the clock of the latest event received by the dashboard."""
    if events_df.empty:
        return "00:00"

    sorted_df = events_df.sort_values(["minute", "second", "timestamp"], na_position="last")
    latest = sorted_df.tail(1)
    if latest.empty:
        return "00:00"

    minute = pd.to_numeric(pd.Series([latest["minute"].iloc[0]]), errors="coerce").fillna(0).astype(int).iloc[0]
    second = pd.to_numeric(pd.Series([latest["second"].iloc[0]]), errors="coerce").fillna(0).astype(int).iloc[0]
    return f"{minute:02d}:{second:02d}"


def render_match_header(events_df: pd.DataFrame) -> None:
    """Render match metadata above the dashboard tabs."""
    match_data, teams_data = load_match_metadata()
    if not match_data:
        st.info("No hay metadatos de partido disponibles en data/static/match.json.")
        return

    home_name = ((match_data.get("home_team") or {}).get("home_team_name")) or "Local"
    away_name = ((match_data.get("away_team") or {}).get("away_team_name")) or "Visitante"
    home_profile = _find_team_profile(home_name, teams_data)
    away_profile = _find_team_profile(away_name, teams_data)
    stadium = match_data.get("stadium") or {}
    referee = match_data.get("referee") or {}
    capacity = (home_profile.get("stadium") or {}).get("capacity")
    kickoff = _format_match_kickoff(match_data)
    competition = (match_data.get("competition") or {}).get("competition_name", "N/D")
    stage = (match_data.get("competition_stage") or {}).get("name", "N/D")
    home_goals = _team_goal_count(events_df, home_name)
    away_goals = _team_goal_count(events_df, away_name)
    latest_clock = _latest_event_clock(events_df)

    with st.container(border=True):
        header_cols = st.columns([0.38, 0.24, 0.38], gap="small")

        home_cols = header_cols[0].columns([0.16, 0.72, 0.12], gap="small")
        if home_profile.get("crest_url"):
            home_cols[0].image(home_profile["crest_url"], width=54)
        home_cols[1].markdown(f"### {home_name}")
        if home_cols[2].button("?", key="home_team_info", help=f"Mas informacion de {home_name}"):
            _render_team_dialog(home_profile, home_name)

        header_cols[1].markdown(
            f"""
            <div style="text-align:center;">
              <div style="font-size:2rem;font-weight:800;line-height:1;">{home_goals} - {away_goals}</div>
              <div style="color:#667085;font-size:.88rem;margin-top:4px;">Ultimo evento {latest_clock}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        away_cols = header_cols[2].columns([0.12, 0.72, 0.16], gap="small")
        if away_cols[0].button("?", key="away_team_info", help=f"Mas informacion de {away_name}"):
            _render_team_dialog(away_profile, away_name)
        away_cols[1].markdown(f"<h3 style='text-align:right;'>{away_name}</h3>", unsafe_allow_html=True)
        if away_profile.get("crest_url"):
            away_cols[2].image(away_profile["crest_url"], width=54)

        st.divider()
        detail_cols = st.columns(3)
        detail_cols[0].markdown(f"**Lugar**  \n{stadium.get('name', 'N/D')}")
        detail_cols[1].markdown(f"**Hora del partido**  \n{kickoff}")
        detail_cols[2].markdown(f"**Capacidad del estadio**  \n{_format_capacity(capacity)}")

        detail_cols = st.columns(3)
        detail_cols[0].markdown(f"**Arbitro**  \n{referee.get('name', 'N/D')}")
        detail_cols[1].markdown(f"**Competicion**  \n{competition}")
        detail_cols[2].markdown(f"**Jornada y fase**  \nJornada {match_data.get('match_week', 'N/D')} - {stage}")


def _empty_events_df() -> pd.DataFrame:
    """Return an empty events dataframe with the expected schema."""
    return pd.DataFrame(columns=EVENT_COLUMNS)


def _empty_metrics_df() -> pd.DataFrame:
    """Return an empty metrics dataframe with the expected schema."""
    return pd.DataFrame(columns=METRIC_COLUMNS)


def _empty_window_metrics_df() -> pd.DataFrame:
    """Return an empty window metrics dataframe with the expected schema."""
    return pd.DataFrame(columns=WINDOW_METRIC_COLUMNS)


def _read_parquet_file(path: Path, columns: list[str]) -> pd.DataFrame:
    """Read one consolidated parquet file, returning an empty typed frame on failure."""
    if not path.exists() or not path.is_file():
        return pd.DataFrame(columns=columns)

    try:
        df = pd.read_parquet(path)
    except Exception as exc:
        st.warning(f"No se pudo leer {path.name}: {exc}")
        return pd.DataFrame(columns=columns)

    for column in columns:
        if column not in df.columns:
            df[column] = None
    return df[columns]


def load_incremental_segments() -> pd.DataFrame:
    """Load already materialized report segments."""
    df = _read_parquet_file(INCREMENTAL_SEGMENTS_FILE, SEGMENT_COLUMNS)
    if df.empty:
        return pd.DataFrame(columns=SEGMENT_COLUMNS)

    df["window_start_minute"] = pd.to_numeric(df["window_start_minute"], errors="coerce").fillna(0).astype(int)
    df["window_end_minute"] = pd.to_numeric(df["window_end_minute"], errors="coerce").fillna(0).astype(int)
    df["window_minutes"] = pd.to_numeric(df["window_minutes"], errors="coerce").fillna(REPORT_WINDOW_MINUTES).astype(int)
    df["generated_at"] = pd.to_datetime(df["generated_at"], errors="coerce")
    return df.sort_values(["match_id", "window_start_minute"])


def load_report_window_status() -> pd.DataFrame:
    """Load observed windows and their narrative-generation status."""
    try:
        return report_window_status()
    except Exception as exc:  # noqa: BLE001
        st.error(f"No se pudo calcular el estado de ventanas: {exc}")
        return pd.DataFrame()


def load_window_metrics() -> pd.DataFrame:
    """Load window metrics already generated by the streaming pipeline."""
    df = _read_parquet_file(WINDOW_METRICS_FILE, WINDOW_METRIC_COLUMNS)
    if df.empty:
        return _empty_window_metrics_df()

    int_columns = [
        "window_start_minute",
        "window_end_minute",
        "window_minutes",
        "events",
        "goals",
        *EVENT_TYPE_COLUMNS,
        "successful_passes",
        "successful_dribbles",
        "successful_duels",
        "successful_interceptions",
        "fouls",
        "recoveries",
        "batch_id",
    ]
    float_columns = ["pass_success_pct", "duel_success_pct", "offensive_index", "defensive_index"]
    for column in int_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0).astype(int)
    for column in float_columns:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
    return df.sort_values(["window_start_minute", "team"])


def load_processed_events() -> pd.DataFrame:
    """Load processed events from the single consolidated parquet file."""
    df = _read_parquet_file(EVENTS_FILE, EVENT_COLUMNS)
    if df.empty:
        return _empty_events_df()

    df["minute"] = pd.to_numeric(df["minute"], errors="coerce").fillna(0).astype(int)
    df["second"] = pd.to_numeric(df["second"], errors="coerce").fillna(0).astype(int)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
    df["schema_version"] = df["schema_version"].fillna("legacy").astype(str)
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
    df["match_time"] = df.apply(lambda row: f"{int(row['minute']):02d}:{int(row['second']):02d}", axis=1)
    return df


def load_team_metrics() -> pd.DataFrame:
    """Load team metric snapshots from the single consolidated parquet file."""
    df = _read_parquet_file(METRICS_FILE, METRIC_COLUMNS)
    if df.empty:
        return _empty_metrics_df()

    count_cols = [
        "total_events",
        "goals",
        "passes",
        "successful_passes",
        "carries",
        "dribbles",
        "successful_dribbles",
        "shots",
        "pressures",
        "duels",
        "successful_duels",
        "interceptions",
        "successful_interceptions",
        "blocks",
        "clearances",
        "fouls",
        "recoveries",
        "batch_id",
        "snapshot_event_count",
    ]
    float_cols = ["pass_success_pct", "duel_success_pct", "offensive_index", "defensive_index"]
    for column in count_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0)
    df[count_cols] = df[count_cols].astype(int)
    for column in float_cols:
        df[column] = pd.to_numeric(df[column], errors="coerce").fillna(0.0)
    df["snapshot_time"] = pd.to_datetime(df["snapshot_time"], errors="coerce")
    return df


def latest_team_snapshot(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """Return the latest cumulative metrics row for each team."""
    if metrics_df.empty:
        return _empty_metrics_df()

    return (
        metrics_df.sort_values(["snapshot_time", "batch_id"])
        .groupby("team", as_index=False)
        .tail(1)
        .sort_values("team")
    )


def render_empty_state() -> None:
    """Render a concise message while Spark has not produced parquet files yet."""
    st.info(
        "Aun no hay datos consolidados. Arranca Kafka, el productor y "
        "`python src/streaming_pipeline.py`; Streamlit se actualizara cada 30 segundos."
    )


def _percentage(part: int, total: int) -> float:
    return (part / total * 100) if total else 0.0


def _render_event_distribution_by_team(events_df: pd.DataFrame) -> None:
    """Render event-type distribution split by team below the live events table."""
    st.subheader("Distribucion por tipo de evento y equipo")
    teams = sorted(events_df["team"].dropna().astype(str).unique())
    if not teams:
        st.info("No hay equipos suficientes para calcular la distribucion.")
        return

    base_index = pd.MultiIndex.from_product(
        [EVENT_TYPE_ORDER, teams],
        names=["event_type_norm", "team"],
    )
    counts = (
        events_df.assign(
            event_type_norm=events_df["event_type"].fillna("").astype(str).str.lower(),
            team=events_df["team"].fillna("N/D").astype(str),
        )
        .groupby(["event_type_norm", "team"], dropna=False)
        .size()
        .reindex(base_index, fill_value=0)
        .reset_index(name="eventos")
    )
    counts["tipo_evento"] = counts["event_type_norm"].map(EVENT_TYPE_LABELS)
    counts["tipo_evento"] = pd.Categorical(
        counts["tipo_evento"],
        categories=EVENT_TYPE_LABEL_ORDER,
        ordered=True,
    )
    distribution_df = counts.sort_values(["tipo_evento", "team"])

    if ALTAIR_AVAILABLE:
        chart = (
            alt.Chart(distribution_df)
            .mark_bar(cornerRadiusEnd=3)
            .encode(
                x=alt.X("tipo_evento:N", sort=EVENT_TYPE_LABEL_ORDER, title="Tipo de evento"),
                xOffset=alt.XOffset("team:N", title="Equipo"),
                y=alt.Y("eventos:Q", title="Eventos"),
                color=alt.Color("team:N", title="Equipo"),
                tooltip=[
                    alt.Tooltip("team:N", title="Equipo"),
                    alt.Tooltip("tipo_evento:N", title="Tipo"),
                    alt.Tooltip("eventos:Q", title="Eventos"),
                ],
            )
            .properties(height=320)
        )
        st.altair_chart(chart, use_container_width=True)
        return

    fallback_df = distribution_df.pivot_table(
        index="tipo_evento",
        columns="team",
        values="eventos",
        fill_value=0,
        aggfunc="sum",
    ).reindex(EVENT_TYPE_LABEL_ORDER)
    st.bar_chart(fallback_df)


def _filter_events_for_table(events_df: pd.DataFrame) -> pd.DataFrame:
    """Render table filters and return the filtered event dataframe."""
    st.subheader("Filtros de eventos")
    latest_minute = int(pd.to_numeric(events_df["minute"], errors="coerce").fillna(0).max())
    max_minute = max(latest_minute, 0)

    filter_cols = st.columns([1.1, 1, 1])
    if max_minute > 0:
        minute_range = filter_cols[0].slider(
            "Rango de tiempo",
            min_value=0,
            max_value=max_minute,
            value=(0, max_minute),
            step=1,
            help="Filtra por minuto de partido desde 0 hasta el ultimo evento recibido.",
        )
    else:
        minute_range = (0, 0)
        filter_cols[0].caption("Rango de tiempo: minuto 0")

    event_options = sorted(events_df["event_type"].dropna().astype(str).unique())
    selected_event_types = filter_cols[1].multiselect(
        "Tipo de evento",
        options=event_options,
        default=event_options,
    )

    zone_options = sorted(events_df["zone"].dropna().astype(str).unique())
    selected_zones = filter_cols[2].multiselect(
        "Zona",
        options=zone_options,
        default=zone_options,
    )

    minute = pd.to_numeric(events_df["minute"], errors="coerce").fillna(0).astype(int)
    filtered_df = events_df[(minute >= minute_range[0]) & (minute <= minute_range[1])].copy()

    if selected_event_types:
        filtered_df = filtered_df[filtered_df["event_type"].astype(str).isin(selected_event_types)]
    else:
        filtered_df = filtered_df.iloc[0:0]

    if selected_zones:
        filtered_df = filtered_df[filtered_df["zone"].astype(str).isin(selected_zones)]
    else:
        filtered_df = filtered_df.iloc[0:0]

    return filtered_df


def _render_paginated_events_table(events_df: pd.DataFrame) -> None:
    """Render all filtered events with manual pagination of 50 rows."""
    st.subheader("Tabla de eventos")
    display_cols = ["match_time", "team", "player", "event_type", "outcome", "zone", "schema_version"]
    available_cols = [column for column in display_cols if column in events_df.columns]

    if events_df.empty:
        st.info("No hay eventos para los filtros seleccionados.")
        return

    sorted_df = events_df.sort_values(["minute", "second", "timestamp"], na_position="last").reset_index(drop=True)
    page_size = 50
    total_rows = len(sorted_df)
    total_pages = max((total_rows - 1) // page_size + 1, 1)

    pager_cols = st.columns([1, 2])
    page = pager_cols[0].number_input(
        "Pagina",
        min_value=1,
        max_value=total_pages,
        value=1,
        step=1,
    )
    start = (int(page) - 1) * page_size
    end = min(start + page_size, total_rows)
    pager_cols[1].caption(f"Mostrando {start + 1}-{end} de {total_rows} eventos filtrados")

    st.dataframe(sorted_df.iloc[start:end][available_cols], use_container_width=True, hide_index=True, height=520)


def _render_horizontal_metric_comparison(latest_df: pd.DataFrame) -> None:
    """Render a horizontal comparative bar chart for the main team metrics."""
    metric_specs = [
        ("total_events", "Eventos"),
        ("offensive_index", "Indice ofensivo"),
        ("defensive_index", "Indice defensivo"),
        ("pass_success_pct", "% pases exitosos"),
        ("duel_success_pct", "% duelos ganados"),
        ("goals", "Goles"),
        ("shots", "Tiros"),
        ("pressures", "Presiones"),
        ("interceptions", "Intercepciones"),
        ("blocks", "Bloqueos"),
        ("clearances", "Despejes"),
    ]
    available_specs = [(column, label) for column, label in metric_specs if column in latest_df.columns]
    if not available_specs:
        return

    metric_order = [label for _, label in available_specs]
    chart_df = latest_df[["team", *[column for column, _ in available_specs]]].melt(
        id_vars="team",
        var_name="metric_key",
        value_name="valor",
    )
    label_map = dict(available_specs)
    chart_df["metrica"] = chart_df["metric_key"].map(label_map)
    chart_df["valor"] = pd.to_numeric(chart_df["valor"], errors="coerce").fillna(0.0)

    st.subheader("Comparativa horizontal de metricas")
    if ALTAIR_AVAILABLE:
        midpoint = (len(metric_order) + 1) // 2
        chart_columns = st.columns(2)
        for column, metric_subset in zip(chart_columns, [metric_order[:midpoint], metric_order[midpoint:]]):
            if not metric_subset:
                continue
            subset_df = chart_df[chart_df["metrica"].isin(metric_subset)]
            base = (
                alt.Chart(subset_df)
                .mark_bar(cornerRadiusEnd=3)
                .encode(
                    y=alt.Y("team:N", title=None, sort="-x"),
                    x=alt.X("valor:Q", title="Valor"),
                    color=alt.Color("team:N", title="Equipo"),
                    tooltip=[
                        alt.Tooltip("team:N", title="Equipo"),
                        alt.Tooltip("metrica:N", title="Metrica"),
                        alt.Tooltip("valor:Q", title="Valor", format=".2f"),
                    ],
                )
                .properties(height=58)
            )
            chart = (
                base.facet(row=alt.Row("metrica:N", title=None, sort=metric_subset))
                .resolve_scale(x="independent")
            )
            column.altair_chart(chart, use_container_width=True)
        return

    fallback_df = chart_df.pivot_table(
        index="metrica",
        columns="team",
        values="valor",
        aggfunc="sum",
        fill_value=0,
    ).reindex(metric_order)
    midpoint = (len(metric_order) + 1) // 2
    left, right = st.columns(2)
    left.bar_chart(fallback_df.loc[metric_order[:midpoint]])
    right.bar_chart(fallback_df.loc[metric_order[midpoint:]])


def render_live_events_tab(events_df: pd.DataFrame) -> None:
    """Render live events table, key metrics, and event-type chart."""
    st.subheader("Eventos")
    if events_df.empty:
        render_empty_state()
        return

    event_type = events_df["event_type"].astype(str).str.lower()
    outcome = events_df["outcome"].astype(str).str.lower()
    total_events = len(events_df)
    total_goals = int(((event_type == "shot") & (outcome == "goal")).sum())
    total_shots = int((event_type == "shot").sum())
    offensive_events = int(event_type.isin(OFFENSIVE_EVENT_TYPES).sum())
    defensive_events = int(event_type.isin(DEFENSIVE_EVENT_TYPES).sum())
    latest_event = events_df.sort_values(["minute", "second", "timestamp"], na_position="last").tail(1)
    latest_clock = latest_event["match_time"].iloc[0] if not latest_event.empty else "00:00"

    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Eventos", total_events)
    m2.metric("Tiempo", latest_clock)
    m3.metric("Goles", total_goals)
    m4.metric("Tiros", total_shots)
    m5.metric(
        "Eventos ofensivos",
        offensive_events,
        f"{_percentage(offensive_events, total_events):.1f}% del total",
        delta_color="off",
    )
    m6.metric(
        "Eventos defensivos",
        defensive_events,
        f"{_percentage(defensive_events, total_events):.1f}% del total",
        delta_color="off",
    )

    filtered_events_df = _filter_events_for_table(events_df)
    _render_event_distribution_by_team(filtered_events_df)
    _render_paginated_events_table(filtered_events_df)


def _render_window_event_evolution(window_metrics_df: pd.DataFrame) -> None:
    if window_metrics_df.empty:
        return

    st.subheader("Evolucion por tipo de evento")
    for team in sorted(window_metrics_df["team"].dropna().unique()):
        team_df = window_metrics_df[window_metrics_df["team"] == team].sort_values("window_start_minute")
        chart_cols = ["events", *[col for col in EVENT_TYPE_COLUMNS if col in team_df.columns]]
        chart_df = team_df.set_index("window_start_minute")[chart_cols]
        st.markdown(f"#### {team}")
        st.line_chart(chart_df)


def _render_index_comparison(window_metrics_df: pd.DataFrame) -> None:
    if window_metrics_df.empty:
        return

    st.subheader("Indices ofensivo y defensivo")
    offensive_df = (
        window_metrics_df.pivot_table(
            index="window_start_minute",
            columns="team",
            values="offensive_index",
            aggfunc="last",
        )
        .sort_index()
        .fillna(0)
    )
    defensive_df = (
        window_metrics_df.pivot_table(
            index="window_start_minute",
            columns="team",
            values="defensive_index",
            aggfunc="last",
        )
        .sort_index()
        .fillna(0)
    )
    left, right = st.columns(2)
    with left:
        st.markdown("#### Indice ofensivo")
        st.line_chart(offensive_df)
    with right:
        st.markdown("#### Indice defensivo")
        st.line_chart(defensive_df)


def render_team_metrics_tab(metrics_df: pd.DataFrame, window_metrics_df: pd.DataFrame) -> None:
    """Render latest team comparison and metric history."""
    st.subheader("Metricas acumuladas por equipo")
    if metrics_df.empty:
        render_empty_state()
        return

    latest_df = latest_team_snapshot(metrics_df)
    _render_horizontal_metric_comparison(latest_df)

    display_cols = [
        "team",
        "total_events",
        "offensive_index",
        "defensive_index",
        "pass_success_pct",
        "duel_success_pct",
        "goals",
        "passes",
        "shots",
        "pressures",
        "duels",
        "interceptions",
        "blocks",
        "clearances",
    ]
    st.subheader("Detalle acumulado")
    st.dataframe(latest_df[display_cols], use_container_width=True, hide_index=True)

    if not window_metrics_df.empty:
        _render_index_comparison(window_metrics_df)
        _render_window_event_evolution(window_metrics_df)


def parse_trace(trace_json: str | None) -> list[str]:
    try:
        trace = json.loads(trace_json) if trace_json else []
    except Exception:
        trace = []
    return [str(step) for step in trace] if isinstance(trace, list) else []


def _split_rag_source(source: str) -> dict[str, str]:
    """Split a RAG source label into file and fragment fields for display."""
    clean_source = str(source or "").strip()
    if not clean_source:
        return {"archivo_fuente": "N/D", "fragmento": ""}

    if "::" in clean_source:
        file_part, detail_part = clean_source.split("::", 1)
        return {"archivo_fuente": file_part.strip(), "fragmento": detail_part.strip()}

    if "#" in clean_source:
        file_part, chunk_part = clean_source.split("#", 1)
        return {"archivo_fuente": file_part.strip(), "fragmento": chunk_part.strip()}

    return {"archivo_fuente": clean_source, "fragmento": ""}


def extract_rag_sources(rag_context: str | None) -> pd.DataFrame:
    """Extract explicit source files from the RAG context shown in Streamlit."""
    context = str(rag_context or "")
    sources: list[str] = []

    for match in re.finditer(r"\[Fuente:\s*([^|\]\n]+)", context):
        sources.append(match.group(1).strip())

    in_sources_block = False
    for raw_line in context.splitlines():
        line = raw_line.strip()
        if line.startswith("Fuentes RAG"):
            in_sources_block = True
            continue
        if in_sources_block and line.startswith("- "):
            sources.append(line[2:].strip())
        elif in_sources_block and not line:
            in_sources_block = False

    deduped: list[str] = []
    for source in sources:
        if source and source not in deduped and "sin fuentes recuperadas" not in source.lower():
            deduped.append(source)

    rows = [_split_rag_source(source) for source in deduped]
    return pd.DataFrame(rows, columns=["archivo_fuente", "fragmento"])


def render_rag_sources(rag_context: str | None) -> None:
    """Render the source files used by RAG for a generated segment."""
    sources_df = extract_rag_sources(rag_context)
    if sources_df.empty:
        st.info("No hay archivo fuente RAG identificado para este segmento.")
        return
    st.dataframe(sources_df, hide_index=True, use_container_width=True)


def render_report_window_status(status_df: pd.DataFrame, compact: bool = False) -> None:
    """Show which match-minute windows are open, pending, or already narrated."""
    if status_df.empty:
        st.info("Aun no hay ventanas de partido detectadas en los eventos consolidados.")
        return

    latest_clock = status_df.sort_values(["current_match_minute", "current_match_second"]).tail(1).iloc[0]
    st.caption(
        "Tiempo de partido detectado: "
        f"{int(latest_clock.current_match_minute)}:{int(latest_clock.current_match_second):02d}"
    )

    view_df = status_df.copy()
    view_df["ventana"] = (
        view_df["window_start_minute"].astype(int).astype(str)
        + "-"
        + view_df["window_end_minute"].astype(int).astype(str)
    )
    view_df = view_df.rename(
        columns={
            "match_id": "partido",
            "event_count": "eventos",
            "status": "estado",
            "is_closed": "cerrada",
            "is_generated": "texto_generado",
        }
    )
    cols = ["partido", "ventana", "eventos", "cerrada", "texto_generado", "estado"]
    dataframe_kwargs = {"height": 170} if compact else {}
    st.dataframe(view_df[cols], hide_index=True, use_container_width=True, **dataframe_kwargs)


def render_incremental_report_tab(events_df: pd.DataFrame) -> None:
    """Render already materialized incremental report segments."""
    st.subheader(f"Informe incremental cada {REPORT_WINDOW_MINUTES} minutos")
    if events_df.empty:
        render_empty_state()
        return

    window_status_df = load_report_window_status()
    render_report_window_status(window_status_df)

    pending_count = 0
    if not window_status_df.empty and "status" in window_status_df.columns:
        pending_count = int((window_status_df["status"] == "pendiente").sum())
    if pending_count:
        st.warning(
            f"{pending_count} ventana(s) de {REPORT_WINDOW_MINUTES} minutos ya estan cerradas "
            "y pendientes de texto. El pipeline de streaming las procesara y lo registrara por consola."
        )

    segments_df = load_incremental_segments()
    window_metrics_df = load_window_metrics()
    if not window_metrics_df.empty:
        chart_df = (
            window_metrics_df.groupby("window_start_minute", as_index=False)
            .agg(
                eventos=("events", "sum"),
                tiros=("shots", "sum"),
                goles=("goals", "sum"),
                presiones=("pressures", "sum"),
                indice_ofensivo=("offensive_index", "sum"),
                indice_defensivo=("defensive_index", "sum"),
            )
            .sort_values("window_start_minute")
            .set_index("window_start_minute")
        )
        st.subheader("Actividad por ventana")
        st.line_chart(chart_df)

    if segments_df.empty:
        st.info(
            "Todavia no hay ventanas completas para narrar. "
            f"Se generara texto cuando exista al menos una ventana cerrada de {REPORT_WINDOW_MINUTES} minutos."
        )
        return

    for row in segments_df.itertuples(index=False):
        title = f"Minutos {int(row.window_start_minute)}-{int(row.window_end_minute)}"
        with st.expander(title, expanded=True):
            st.write(row.text)
            model_label = "Fallback determinista" if str(row.llm_model).startswith("fallback:") else (row.llm_model or "no disponible")
            generated_at = _format_madrid_datetime(row.generated_at)
            st.caption(f"Generado por: {model_label} | Hora Madrid: {generated_at}")
            with st.popover("Contexto, fuentes y metricas"):
                st.markdown("**Archivos fuente usados por RAG**")
                render_rag_sources(row.rag_context)
                st.markdown("**Contexto usado**")
                st.code(row.rag_context or "Sin contexto RAG", language="text")
                st.markdown("**Metricas usadas**")
                st.code(row.metrics_json or "{}", language="json")


def render_export_tab(events_df: pd.DataFrame, metrics_df: pd.DataFrame) -> None:
    """Render only the PDF generated by the streaming pipeline."""
    st.subheader("Informe evolutivo PDF")

    disabled = events_df.empty and metrics_df.empty
    if disabled:
        render_empty_state()
        return

    segments_df = load_incremental_segments()
    window_status_df = load_report_window_status()

    if segments_df.empty:
        render_report_window_status(window_status_df, compact=True)
        st.info(
            "El PDF aparecera aqui cuando el pipeline cierre una ventana, "
            "genere su texto y refresque el documento automaticamente."
        )
        return

    if REPORT_PDF_FILE.exists() and REPORT_PDF_FILE.is_file():
        st.success("Informe PDF disponible.")
        st.download_button(
            "Descargar PDF",
            data=REPORT_PDF_FILE.read_bytes(),
            file_name="match_report.pdf",
            mime="application/pdf",
        )
    else:
        st.warning(
            "Ya hay textos generados, pero el PDF todavia no esta disponible. "
            "El pipeline de streaming lo refrescara en el siguiente ciclo de generacion."
        )


def render_pipeline_status_tab() -> None:
    """Render output readiness and configuration details."""
    events_ready = EVENTS_FILE.exists() and EVENTS_FILE.is_file()
    metrics_ready = METRICS_FILE.exists() and METRICS_FILE.is_file()
    window_metrics_ready = WINDOW_METRICS_FILE.exists() and WINDOW_METRICS_FILE.is_file()
    segments_ready = INCREMENTAL_SEGMENTS_FILE.exists() and INCREMENTAL_SEGMENTS_FILE.is_file()
    pdf_ready = REPORT_PDF_FILE.exists() and REPORT_PDF_FILE.is_file()

    st.subheader("Estado")
    st.write(f"Eventos consolidados: {'OK' if events_ready else 'pendiente'}")
    st.write(f"Metricas consolidadas: {'OK' if metrics_ready else 'pendiente'}")
    st.write(f"Metricas por ventana: {'OK' if window_metrics_ready else 'pendiente'}")
    st.write(f"Segmentos incrementales: {'OK' if segments_ready else 'pendiente'}")
    st.write(f"PDF generado: {'OK' if pdf_ready else 'pendiente'}")
    st.write(f"Tamano de ventana configurado: {REPORT_WINDOW_MINUTES} minutos")

    st.subheader("Ficheros leidos por Streamlit")
    st.code(
        f"{EVENTS_FILE}\n{METRICS_FILE}\n{WINDOW_METRICS_FILE}\n{INCREMENTAL_SEGMENTS_FILE}\n"
        f"{REPORT_PDF_FILE}",
        language="text",
    )

    if st.button("Actualizar ahora"):
        st.rerun()


def main() -> None:
    """Main Streamlit entrypoint."""
    st.set_page_config(page_title="Real-Time Match Analytics Dashboard", page_icon=":soccer:", layout="wide")
    st.title("Real-Time Match Analytics Dashboard")

    if AUTOREFRESH_AVAILABLE:
        st_autorefresh(interval=30_000, key="dashboard_autorefresh")
    else:
        st.caption("Auto-refresh no disponible. Usa el boton de estado para actualizar.")

    events_df = load_processed_events()
    metrics_df = load_team_metrics()
    window_metrics_df = load_window_metrics()

    render_match_header(events_df)

    tab_live, tab_team, tab_incremental, tab_export, tab_status = st.tabs(
        ["Eventos", "Metricas por equipo", "Informe incremental", "Exportar informe", "Estado"]
    )

    with tab_live:
        render_live_events_tab(events_df)

    with tab_team:
        render_team_metrics_tab(metrics_df, window_metrics_df)

    with tab_incremental:
        render_incremental_report_tab(events_df)

    with tab_export:
        render_export_tab(events_df, metrics_df)

    with tab_status:
        render_pipeline_status_tab()


if __name__ == "__main__":
    main()
