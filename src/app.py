"""Streamlit dashboard for real-time match analytics.

The dashboard reads the consolidated parquet files produced by
`streaming_pipeline.py`:
- `output/processed/events.parquet`
- `output/aggregates/team_metrics.parquet`
"""

from __future__ import annotations

import json
from pathlib import Path

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

EVENT_COLUMNS = [
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
    "value",
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
    "avg_value",
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
    "avg_value",
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
    float_columns = ["pass_success_pct", "duel_success_pct", "offensive_index", "defensive_index", "avg_value"]
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
    df["value"] = pd.to_numeric(df["value"], errors="coerce").fillna(0).astype(int)
    df["period"] = pd.to_numeric(df["period"], errors="coerce").fillna(0).astype(int)
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
    float_cols = ["pass_success_pct", "duel_success_pct", "offensive_index", "defensive_index", "avg_value"]
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

    display_cols = ["match_time", "team", "player", "event_type", "outcome", "zone", "value"]
    display_df = (
        events_df.sort_values(["minute", "second", "timestamp"], na_position="last")
        .tail(35)
        .sort_values(by=["minute", "second"], ascending=False)
    )
    st.dataframe(display_df[display_cols], use_container_width=True, hide_index=True)

    _render_event_distribution_by_team(events_df)


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
            st.caption(f"Modelo: {model_label} | Generado: {row.generated_at}")
            trace = parse_trace(row.trace_json)
            problem_trace = [
                step
                for step in trace
                if any(token in step.lower() for token in ["error", "fallback", "problema", "no se pudo"])
            ]
            if problem_trace:
                st.warning("\n".join(f"- {step}" for step in problem_trace))
            if trace:
                st.markdown("**Etapas de generacion**")
                for step in trace:
                    st.write(f"- {step}")
            with st.popover("RAG y metricas"):
                st.code(row.rag_context or "Sin contexto RAG", language="text")
                st.code(row.metrics_json or "{}", language="json")


def render_export_tab(events_df: pd.DataFrame, metrics_df: pd.DataFrame) -> None:
    """Render the last PDF generated from processed report segments."""
    st.subheader("Informe evolutivo PDF")

    disabled = events_df.empty and metrics_df.empty
    if disabled:
        render_empty_state()
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
        st.info("Aun no hay PDF generado con los ultimos segmentos procesados.")


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

    tab_live, tab_team, tab_incremental, tab_export, tab_status = st.tabs(
        ["⚽ Eventos", "📊 Metricas por equipo", "📝 Informe incremental", "📄 Exportar informe", "⚙️ Estado"]
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
