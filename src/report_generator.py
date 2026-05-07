"""PDF report generation for the final match-analysis practice."""

from __future__ import annotations

from datetime import datetime
from io import BytesIO
from typing import Any

from graph_workflow import run_analysis_workflow
from incremental_report import generate_missing_report_segments, load_incremental_segments, load_window_metrics
from settings import REPORT_WINDOW_MINUTES

try:
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.shapes import Drawing
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import cm
    from reportlab.platypus import Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

    REPORTLAB_AVAILABLE = True
except ImportError:
    REPORTLAB_AVAILABLE = False


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


def _build_metrics_table(metrics: dict) -> Table:
    teams = metrics.get("teams", [])
    rows = [["Equipo", "Eventos", "Goles", "Tiros", "Pases", "Faltas", "Recup."]]
    for team in teams:
        rows.append(
            [
                team.get("team", "N/D"),
                int(team.get("total_events", 0)),
                int(team.get("goals", 0)),
                int(team.get("shots", 0)),
                int(team.get("passes", 0)),
                int(team.get("fouls", 0)),
                int(team.get("recoveries", 0)),
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
    rows = [["Min.", "Equipo", "Eventos", "Goles", "Tiros", "Pases", "Faltas", "Recup."]]
    if window_metrics_df.empty:
        rows.append(["Sin datos", "N/D", 0, 0, 0, 0, 0, 0])
    else:
        for row in window_metrics_df.itertuples(index=False):
            rows.append(
                [
                    f"{int(row.window_start_minute)}-{int(row.window_end_minute)}",
                    getattr(row, "team", "N/D"),
                    int(getattr(row, "events", 0)),
                    int(getattr(row, "goals", 0)),
                    int(getattr(row, "shots", 0)),
                    int(getattr(row, "passes", 0)),
                    int(getattr(row, "fouls", 0)),
                    int(getattr(row, "recoveries", 0)),
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


def _build_window_activity_chart(window_metrics_df) -> Drawing | None:
    if window_metrics_df.empty:
        return None

    grouped = (
        window_metrics_df.groupby("window_start_minute", as_index=False)
        .agg(events=("events", "sum"), shots=("shots", "sum"), recoveries=("recoveries", "sum"))
        .sort_values("window_start_minute")
    )
    if grouped.empty:
        return None

    drawing = Drawing(460, 190)
    chart = VerticalBarChart()
    chart.x = 45
    chart.y = 35
    chart.height = 120
    chart.width = 380
    chart.data = [
        grouped["events"].astype(int).tolist(),
        grouped["shots"].astype(int).tolist(),
        grouped["recoveries"].astype(int).tolist(),
    ]
    chart.categoryAxis.categoryNames = [str(int(value)) for value in grouped["window_start_minute"]]
    chart.valueAxis.valueMin = 0
    chart.valueAxis.valueMax = max(max(series) for series in chart.data) + 2
    chart.valueAxis.valueStep = max(int(chart.valueAxis.valueMax // 4), 1)
    chart.bars[0].fillColor = colors.HexColor("#1F4E79")
    chart.bars[1].fillColor = colors.HexColor("#58A4B0")
    chart.bars[2].fillColor = colors.HexColor("#D95D39")
    drawing.add(chart)
    return drawing


def generate_pdf_report(include_incremental: bool = False) -> tuple[bytes, dict]:
    """Run the LangGraph workflow and return the final report as PDF bytes."""
    if not REPORTLAB_AVAILABLE:
        raise RuntimeError("La generacion PDF requiere instalar `reportlab`.")

    workflow_output = run_analysis_workflow()
    metrics = workflow_output.get("metrics", {})
    sections = workflow_output.get("report_sections", {})
    rag_context = workflow_output.get("rag_context", "")
    segments_df = load_incremental_segments()
    window_metrics_df = load_window_metrics()
    if include_incremental:
        segments_df, _ = generate_missing_report_segments()
        window_metrics_df = load_window_metrics()

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
            "Generado exclusivamente en PDF a partir de metricas Spark, contexto RAG y "
            f"orquestacion {'LangGraph' if workflow_output.get('used_langgraph') else 'local compatible'} "
            f"con agents/tools. LLM: {workflow_output.get('llm_model', 'no disponible')}. "
            f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            styles["Subtitle"],
        ),
    ]

    _add_section(story, styles, "1. Informacion derivada de datos del partido", sections.get("resumen_general", ""))
    story.append(_build_metrics_table(metrics))
    story.append(Spacer(1, 0.3 * cm))
    _add_section(story, styles, "2. Equipo o jugador destacado", sections.get("destacado", ""))
    _add_section(story, styles, "3. Momento o tramo de mayor intensidad", sections.get("momento_intensidad", ""))
    _add_section(story, styles, "4. Interpretacion basica del rendimiento", sections.get("interpretacion", ""))
    _add_section(
        story,
        styles,
        "5. Informacion contextual aportada por documentos recuperados",
        rag_context,
    )
    _add_section(story, styles, "6. Conclusion final para stakeholder no tecnico", sections.get("conclusion", ""))
    if include_incremental:
        _add_section(
            story,
            styles,
            f"7. Informe incremental por ventanas de {REPORT_WINDOW_MINUTES} minutos",
            "Bloques generados de forma incremental a partir de eventos, metricas de ventana y contexto RAG.",
        )
        if not segments_df.empty:
            for row in segments_df.itertuples(index=False):
                _add_section(
                    story,
                    styles,
                    f"Minutos {int(row.window_start_minute)}-{int(row.window_end_minute)}",
                    row.text,
                )
        else:
            _add_section(story, styles, "Segmentos incrementales", "No hay segmentos incrementales generados.")

        chart = _build_window_activity_chart(window_metrics_df)
        if chart is not None:
            story.append(Paragraph("Grafica de actividad por ventana", styles["SectionTitle"]))
            story.append(chart)
            story.append(Spacer(1, 0.3 * cm))
        story.append(Paragraph("Metricas por ventana", styles["SectionTitle"]))
        story.append(_build_window_metrics_table(window_metrics_df))
        story.append(Spacer(1, 0.3 * cm))

    trace_title = "8. Trazabilidad LangGraph y tools" if include_incremental else "7. Trazabilidad LangGraph y tools"
    _add_section(story, styles, trace_title, "<br/>".join(workflow_output.get("trace", [])))

    doc.build(story)
    return buffer.getvalue(), workflow_output
