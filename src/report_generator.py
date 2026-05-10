"""PDF report generation for the final match-analysis practice."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any

import pandas as pd

from incremental_report import load_incremental_segments, load_window_metrics
from settings import EVENTS_FILE, METRICS_FILE, REPORT_WINDOW_MINUTES

try:
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.charts.linecharts import HorizontalLineChart
    from reportlab.graphics.shapes import Drawing, String
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


EVENT_TYPE_METRICS = [
    ("passes", "Pase"),
    ("carries", "Conduccion"),
    ("dribbles", "Regate"),
    ("shots", "Tiro"),
    ("pressures", "Presion"),
    ("duels", "Duelo"),
    ("interceptions", "Intercepcion"),
    ("blocks", "Bloqueo"),
    ("clearances", "Despeje"),
]
CHART_COLORS = [
    "#1F4E79",
    "#D95D39",
    "#58A4B0",
    "#7A5195",
    "#2F855A",
    "#C0841A",
    "#6B7280",
    "#B83280",
    "#2B6CB0",
]


def _clean_text(value: Any) -> str:
    """Convert values to PDF-safe plain text."""
    return str(value).replace("\n", "<br/>")


def generate_report(metrics: dict, rag_context: str, workflow_output: dict) -> str:
    """Generate the natural-language body used in the PDF report."""
    sections = workflow_output.get("report_sections", {})
    return "\n\n".join(
        [
            "RESUMEN GENERAL DEL PARTIDO\n" + sections.get("resumen_general", ""),
            "EQUIPO O JUGADOR DESTACADO\n" + sections.get("destacado", ""),
            "MOMENTO O TRAMO DE MAYOR INTENSIDAD\n" + sections.get("momento_intensidad", ""),
            "INTERPRETACION BASICA DEL RENDIMIENTO\n" + sections.get("interpretacion", ""),
            "CONTEXTO RECUPERADO MEDIANTE RAG\n" + rag_context,
            "CONCLUSION PARA STAKEHOLDER NO TECNICO\n" + sections.get("conclusion", ""),
        ]
    )


def _add_section(story: list, styles: dict, title: str, body: str) -> None:
    story.append(Paragraph(title, styles["SectionTitle"]))
    story.append(Paragraph(_clean_text(body), styles["Body"]))
    story.append(Spacer(1, 0.28 * cm))


def _add_segment_section(story: list, styles: dict, row: Any) -> None:
    start = int(row.window_start_minute)
    end = int(row.window_end_minute)
    story.append(Paragraph(f"Minutos {start}-{end}", styles["SectionTitle"]))
    story.append(Paragraph(_clean_text(row.text), styles["Body"]))
    source_text = (
        f"Fuente: datos de eventos y metricas Spark de la ventana {start}-{end}, "
        "mas contexto RAG incremental construido con las ventanas procesadas. "
        "El texto procede del flujo de agentes y LLM que resume los datos de esa ventana."
    )
    story.append(Paragraph(_clean_text(source_text), styles["Trace"]))
    story.append(Spacer(1, 0.28 * cm))


def _build_metrics_table(metrics: dict) -> Table:
    teams = metrics.get("teams", [])
    rows = [["Equipo", "Eventos", "Ind. Of.", "Ind. Def.", "% Pase", "% Duelo", "Goles"]]
    for team in teams:
        rows.append(
            [
                team.get("team", "N/D"),
                int(team.get("total_events", 0)),
                float(team.get("offensive_index", 0)),
                float(team.get("defensive_index", 0)),
                float(team.get("pass_success_pct", 0)),
                float(team.get("duel_success_pct", 0)),
                int(team.get("goals", 0)),
            ]
        )

    if len(rows) == 1:
        rows.append(["Sin datos", 0, 0, 0, 0, 0, 0])

    table = Table(rows, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E79")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#C7D0D9")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 8),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4F7FA")]),
            ]
        )
    )
    return table


def _build_window_metrics_table(window_metrics_df) -> Table:
    rows = [["Min.", "Equipo", "Eventos", "Ind. Of.", "Ind. Def.", "% Pase", "% Duelo", "Tiros"]]
    if window_metrics_df.empty:
        rows.append(["Sin datos", "N/D", 0, 0, 0, 0, 0, 0])
    else:
        for row in window_metrics_df.itertuples(index=False):
            rows.append(
                [
                    f"{int(row.window_start_minute)}-{int(row.window_end_minute)}",
                    getattr(row, "team", "N/D"),
                    int(getattr(row, "events", 0)),
                    float(getattr(row, "offensive_index", 0)),
                    float(getattr(row, "defensive_index", 0)),
                    float(getattr(row, "pass_success_pct", 0)),
                    float(getattr(row, "duel_success_pct", 0)),
                    int(getattr(row, "shots", 0)),
                ]
            )

    table = Table(rows, repeatRows=1, hAlign="LEFT")
    table.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1F4E79")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#C7D0D9")),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 7),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#F4F7FA")]),
            ]
        )
    )
    return table


def _chart_color(index: int):
    return colors.HexColor(CHART_COLORS[index % len(CHART_COLORS)])


def _build_event_distribution_chart(metrics: dict[str, Any]) -> Drawing | None:
    teams = metrics.get("teams", [])
    if not teams:
        return None

    drawing = Drawing(500, 230)
    chart = VerticalBarChart()
    chart.x = 42
    chart.y = 42
    chart.height = 130
    chart.width = 420
    chart.data = [
        [float(team.get(column, 0)) for column, _ in EVENT_TYPE_METRICS]
        for team in teams
    ]
    chart.categoryAxis.categoryNames = [label for _, label in EVENT_TYPE_METRICS]
    chart.categoryAxis.labels.angle = 28
    chart.categoryAxis.labels.fontSize = 6
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(max(series) for series in chart.data) + 2
    chart.valueAxis.valueStep = max(int(chart.valueAxis.valueMax // 4), 1)
    for index, _team in enumerate(teams):
        chart.bars[index].fillColor = _chart_color(index)
    drawing.add(chart)
    drawing.add(String(42, 200, "Distribucion de eventos por equipo", fontName="Helvetica-Bold", fontSize=10))
    for index, team in enumerate(teams):
        drawing.add(
            String(
                42 + (index * 160),
                184,
                f"{team.get('team', 'N/D')}",
                fontSize=8,
                fillColor=_chart_color(index),
            )
        )
    return drawing


def _line_chart(
    data: list[list[float]],
    categories: list[str],
    x: int,
    y: int,
    width: int,
    height: int,
) -> HorizontalLineChart:
    chart = HorizontalLineChart()
    chart.x = x
    chart.y = y
    chart.height = height
    chart.width = width
    chart.data = data
    chart.categoryAxis.categoryNames = categories
    chart.categoryAxis.labels.fontSize = 6
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(max(series) for series in data) + 2 if data else 2
    chart.valueAxis.valueStep = max(int(chart.valueAxis.valueMax // 4), 1)
    for index in range(len(data)):
        chart.lines[index].strokeColor = _chart_color(index)
        chart.lines[index].strokeWidth = 1
    return chart


def _build_event_type_evolution_chart(window_metrics_df) -> Drawing | None:
    if window_metrics_df.empty:
        return None

    teams = sorted(window_metrics_df["team"].dropna().astype(str).unique())
    if not teams:
        return None

    team_count = min(len(teams), 2)
    drawing_height = 155 * team_count + 50
    drawing = Drawing(500, drawing_height)
    drawing.add(
        String(
            42,
            drawing_height - 18,
            "Evolucion por tipo de evento en ventanas de partido",
            fontName="Helvetica-Bold",
            fontSize=10,
        )
    )

    for team_index, team in enumerate(teams[:2]):
        team_df = window_metrics_df[window_metrics_df["team"].astype(str) == team].sort_values("window_start_minute")
        if team_df.empty:
            continue
        categories = [str(int(value)) for value in team_df["window_start_minute"]]
        series = [
            team_df[column].fillna(0).astype(float).tolist()
            for column, _label in EVENT_TYPE_METRICS
            if column in team_df.columns
        ]
        if not series:
            continue
        y = drawing_height - 150 - (team_index * 155)
        drawing.add(String(42, y + 118, team, fontName="Helvetica-Bold", fontSize=9))
        drawing.add(_line_chart(series, categories, 42, y, 410, 95))

    legend_y = 12
    for index, (_column, label) in enumerate(EVENT_TYPE_METRICS):
        drawing.add(String(42 + (index % 3) * 145, legend_y + (index // 3) * 10, label, fontSize=6, fillColor=_chart_color(index)))
    return drawing


def _build_offensive_index_evolution_chart(window_metrics_df) -> Drawing | None:
    if window_metrics_df.empty or "offensive_index" not in window_metrics_df.columns:
        return None

    teams = sorted(window_metrics_df["team"].dropna().astype(str).unique())
    categories = sorted(window_metrics_df["window_start_minute"].dropna().astype(int).unique())
    if not teams or not categories:
        return None

    data = []
    for team in teams:
        team_df = (
            window_metrics_df[window_metrics_df["team"].astype(str) == team]
            .set_index("window_start_minute")
            .sort_index()
        )
        data.append([float(team_df["offensive_index"].get(category, 0.0)) for category in categories])

    drawing = Drawing(500, 210)
    drawing.add(String(42, 190, "Evolucion del indice ofensivo por equipo", fontName="Helvetica-Bold", fontSize=10))
    chart = _line_chart(data, [str(value) for value in categories], 42, 45, 410, 120)
    drawing.add(chart)
    for index, team in enumerate(teams):
        drawing.add(String(42 + (index * 160), 20, team, fontSize=8, fillColor=_chart_color(index)))
    return drawing


def _read_parquet(path) -> pd.DataFrame:
    if not path.exists() or not path.is_file():
        return pd.DataFrame()
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _latest_team_metrics() -> pd.DataFrame:
    df = _read_parquet(METRICS_FILE)
    if df.empty:
        return pd.DataFrame()
    df["snapshot_time"] = pd.to_datetime(df.get("snapshot_time"), errors="coerce")
    return (
        df.sort_values(["snapshot_time", "batch_id"])
        .groupby("team", as_index=False)
        .tail(1)
        .sort_values("team")
    )


def _build_artifact_metrics() -> dict[str, Any]:
    latest_df = _latest_team_metrics()
    if latest_df.empty:
        return {"teams": [], "total_events": 0}
    return {
        "teams": latest_df.to_dict(orient="records"),
        "total_events": int(latest_df["total_events"].sum()) if "total_events" in latest_df.columns else 0,
        "total_goals": int(latest_df["goals"].sum()) if "goals" in latest_df.columns else 0,
        "total_shots": int(latest_df["shots"].sum()) if "shots" in latest_df.columns else 0,
    }


def _load_events() -> pd.DataFrame:
    df = _read_parquet(EVENTS_FILE)
    if df.empty:
        return df
    df["event_type_norm"] = df["event_type"].astype(str).str.lower() if "event_type" in df.columns else ""
    df["outcome_norm"] = df["outcome"].astype(str).str.lower() if "outcome" in df.columns else ""
    return df


def _general_metrics_summary(metrics: dict[str, Any]) -> str:
    teams = metrics.get("teams", [])
    if not teams:
        return "No hay metricas generales suficientes para resumir el partido."
    top_offensive = max(teams, key=lambda item: item.get("offensive_index", 0), default={})
    top_defensive = max(teams, key=lambda item: item.get("defensive_index", 0), default={})
    return (
        f"El partido acumula {metrics.get('total_events', 0)} eventos, "
        f"{metrics.get('total_shots', 0)} tiros y {metrics.get('total_goals', 0)} goles. "
        f"El mayor indice ofensivo corresponde a {top_offensive.get('team', 'N/D')} "
        f"({float(top_offensive.get('offensive_index', 0)):.1f}) y el mayor indice defensivo a "
        f"{top_defensive.get('team', 'N/D')} ({float(top_defensive.get('defensive_index', 0)):.1f})."
    )


def _highlighted_player_summary() -> str:
    events_df = _load_events()
    if events_df.empty or "player" not in events_df.columns:
        return "No hay eventos suficientes para identificar un jugador destacado."

    rows: list[dict[str, Any]] = []
    for (player, team), player_df in events_df.groupby(["player", "team"], dropna=False):
        event_type = player_df["event_type_norm"]
        outcome = player_df["outcome_norm"]
        events = int(len(player_df))
        shots = int((event_type == "shot").sum())
        goals = int(((event_type == "shot") & (outcome == "goal")).sum())
        successful_passes = int(((event_type == "pass") & (outcome == "success")).sum())
        carries = int((event_type == "carry").sum())
        successful_dribbles = int(((event_type == "dribble") & (outcome == "success")).sum())
        pressures = int((event_type == "pressure").sum())
        duels_won = int(((event_type == "duel") & (outcome == "success")).sum())
        score = (
            goals * 8.0
            + shots * 3.0
            + successful_dribbles * 1.4
            + duels_won * 1.2
            + successful_passes * 0.5
            + carries * 0.35
            + pressures * 0.3
            + events * 0.1
        )
        rows.append(
            {
                "player": player,
                "team": team,
                "events": events,
                "shots": shots,
                "goals": goals,
                "successful_passes": successful_passes,
                "carries": carries,
                "successful_dribbles": successful_dribbles,
                "pressures": pressures,
                "duels_won": duels_won,
                "score": score,
            }
        )

    if not rows:
        return "No hay eventos suficientes para identificar un jugador destacado."

    player = max(rows, key=lambda item: item["score"])
    return (
        f"Jugador destacado: {player.get('player', 'N/D')} ({player.get('team', 'N/D')}). "
        f"Se destaca por su volumen e impacto: {player.get('events', 0)} intervenciones, "
        f"{player.get('successful_passes', 0)} pases exitosos, {player.get('carries', 0)} conducciones, "
        f"{player.get('successful_dribbles', 0)} regates exitosos, {player.get('shots', 0)} tiros, "
        f"{player.get('goals', 0)} goles, {player.get('pressures', 0)} presiones y "
        f"{player.get('duels_won', 0)} duelos ganados."
    )


def _evolution_summary(metrics: dict[str, Any], segments_df: pd.DataFrame) -> str:
    teams = metrics.get("teams", [])
    if not teams:
        return "No hay metricas consolidadas suficientes para resumir la evolucion del partido."

    top_offensive = max(teams, key=lambda item: item.get("offensive_index", 0), default={})
    top_defensive = max(teams, key=lambda item: item.get("defensive_index", 0), default={})
    segment_count = 0 if segments_df.empty else len(segments_df)
    return (
        f"El informe evolutivo integra {segment_count} ventanas narradas de "
        f"{REPORT_WINDOW_MINUTES} minutos de partido. El equipo con mayor indice ofensivo "
        f"acumulado es {top_offensive.get('team', 'N/D')} "
        f"({float(top_offensive.get('offensive_index', 0)):.1f}), mientras que el mayor "
        f"indice defensivo corresponde a {top_defensive.get('team', 'N/D')} "
        f"({float(top_defensive.get('defensive_index', 0)):.1f})."
    )


def generate_pdf_report(include_incremental: bool = True) -> tuple[bytes, dict]:
    """Build a PDF from already materialized metrics and narrative artifacts."""
    if not REPORTLAB_AVAILABLE:
        raise RuntimeError("La generacion PDF requiere instalar `reportlab`.")

    metrics = _build_artifact_metrics()
    segments_df = load_incremental_segments()
    window_metrics_df = load_window_metrics()
    workflow_output = {
        "source": "existing_artifacts",
        "llm_model": "no ejecutado",
        "used_langgraph": False,
        "trace": [
            "El PDF no lo genera el agente: compone metricas, graficas y textos ya existentes.",
            "Los textos de cada intervalo si proceden del flujo previo de agentes, RAG incremental y LLM.",
        ],
    }

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=1.6 * cm,
        leftMargin=1.6 * cm,
        topMargin=1.5 * cm,
        bottomMargin=1.5 * cm,
        title="Informe automatico del partido",
    )

    sample = getSampleStyleSheet()
    styles = {
        "Title": ParagraphStyle(
            "ReportTitle",
            parent=sample["Title"],
            fontName="Helvetica-Bold",
            fontSize=18,
            leading=22,
            textColor=colors.HexColor("#16324F"),
            spaceAfter=10,
        ),
        "Subtitle": ParagraphStyle(
            "Subtitle",
            parent=sample["BodyText"],
            fontSize=9,
            leading=12,
            textColor=colors.HexColor("#53616F"),
            spaceAfter=12,
        ),
        "SectionTitle": ParagraphStyle(
            "SectionTitle",
            parent=sample["Heading2"],
            fontName="Helvetica-Bold",
            fontSize=12,
            leading=15,
            textColor=colors.HexColor("#1F4E79"),
            spaceBefore=8,
            spaceAfter=5,
        ),
        "Body": ParagraphStyle(
            "Body",
            parent=sample["BodyText"],
            fontSize=9.5,
            leading=13,
            spaceAfter=4,
        ),
        "Trace": ParagraphStyle(
            "Trace",
            parent=sample["BodyText"],
            fontSize=8,
            leading=10,
            textColor=colors.HexColor("#53616F"),
        ),
    }

    story = [
        Paragraph("Informe automatico del partido", styles["Title"]),
        Paragraph(
            "Generado a partir de metricas Spark y narrativas incrementales ya persistidas. "
            f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            styles["Subtitle"],
        ),
    ]

    _add_section(story, styles, "1. Metricas generales del partido", _general_metrics_summary(metrics))
    _add_section(story, styles, "2. Lectura evolutiva", _evolution_summary(metrics, segments_df))
    story.append(_build_metrics_table(metrics))
    story.append(Spacer(1, 0.3 * cm))
    _add_section(story, styles, "3. Jugador destacado", _highlighted_player_summary())

    if include_incremental:
        _add_section(
            story,
            styles,
            f"4. Textos por ventanas de {REPORT_WINDOW_MINUTES} minutos",
            "Bloques generados previamente a partir de ventanas cerradas de partido.",
        )
        if not segments_df.empty:
            for row in segments_df.itertuples(index=False):
                _add_segment_section(story, styles, row)
        else:
            _add_section(story, styles, "Segmentos incrementales", "No hay segmentos incrementales generados.")

        distribution_chart = _build_event_distribution_chart(metrics)
        if distribution_chart is not None:
            story.append(Paragraph("5. Distribucion de eventos por equipo", styles["SectionTitle"]))
            story.append(distribution_chart)
            story.append(Spacer(1, 0.3 * cm))

        event_evolution_chart = _build_event_type_evolution_chart(window_metrics_df)
        if event_evolution_chart is not None:
            story.append(Paragraph("6. Evolucion de tipos de evento por equipo", styles["SectionTitle"]))
            story.append(event_evolution_chart)
            story.append(Spacer(1, 0.3 * cm))

        offensive_chart = _build_offensive_index_evolution_chart(window_metrics_df)
        if offensive_chart is not None:
            story.append(Paragraph("7. Evolucion del indice ofensivo", styles["SectionTitle"]))
            story.append(offensive_chart)
            story.append(Spacer(1, 0.3 * cm))

    _add_section(story, styles, "8. Justificacion", "<br/>".join(workflow_output.get("trace", [])))

    doc.build(story)
    return buffer.getvalue(), workflow_output
