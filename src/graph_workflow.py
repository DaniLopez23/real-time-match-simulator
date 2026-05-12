"""Simple agent workflow for incremental match reports.

Only one workflow exists here:
1. Metrics agent prepares the window facts and a RAG query.
2. RAG agent retrieves contextual information with the RAG tool.
3. Writer agent generates one short narrative fragment for that window.
"""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Callable, TypedDict

from tools import retrieve_document_context

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

try:
    from langgraph.graph import END, StateGraph

    LANGGRAPH_AVAILABLE = True
except ImportError:
    END = "END"
    StateGraph = None
    LANGGRAPH_AVAILABLE = False


ProgressCallback = Callable[[str, str, dict[str, Any] | None], None]


class WindowReportState(TypedDict, total=False):
    """State for one incremental match-minute report segment."""

    match_id: str
    window_start_minute: int
    window_end_minute: int
    window_metrics: dict[str, Any]
    window_events: list[dict[str, Any]]
    cumulative_metrics: dict[str, Any]
    rag_query: str
    rag_context: str
    final_prompt: str
    text: str
    llm_model: str
    trace: list[str]
    used_langgraph: bool
    progress_callback: ProgressCallback


def _append_trace(state: WindowReportState, message: str) -> list[str]:
    return [*state.get("trace", []), message]


def _emit_progress(
    state: WindowReportState,
    stage: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    callback = state.get("progress_callback")
    if callback is None:
        return
    try:
        callback(stage, message, details or {})
    except Exception:
        pass


def metrics_agent_node(state: WindowReportState) -> WindowReportState:
    """Use window metrics to prepare the query for RAG."""
    metrics = state.get("window_metrics", {})
    teams = metrics.get("teams", [])
    activity_team = max(teams, key=lambda item: item.get("events", 0), default={})
    pressure_team = max(teams, key=lambda item: item.get("pressures", 0), default={})

    query = (
        "interpretacion futbolistica de ventana de partido: "
        f"{metrics.get('total_events', 0)} eventos, "
        f"{metrics.get('total_shots', 0)} tiros, "
        f"{metrics.get('total_goals', 0)} goles, "
        f"{metrics.get('total_pressures', 0)} presiones, "
        f"equipo mas activo {activity_team.get('team', 'N/D')}, "
        f"equipo con mas presion {pressure_team.get('team', 'N/D')}, "
        "intensidad, posesion, presion, rendimiento y redaccion para stakeholder"
    )

    _emit_progress(state, "metricas", "Metricas de la ventana preparadas para RAG.", {"query": query})
    return {
        **state,
        "rag_query": query,
        "trace": _append_trace(state, "Agente de metricas preparo la consulta RAG de la ventana."),
    }


def rag_agent_node(state: WindowReportState) -> WindowReportState:
    """Use the RAG tool to retrieve document context."""
    query = state.get("rag_query", "intensidad rendimiento futbolistico")
    _emit_progress(state, "rag", "Recuperando contexto documental con la tool RAG.", {"query": query})

    try:
        context = (
            retrieve_document_context.invoke(query)
            if hasattr(retrieve_document_context, "invoke")
            else retrieve_document_context(query)
        )
        trace_message = "Agente RAG recupero contexto documental para la ventana."
    except Exception as exc:  # noqa: BLE001
        context = f"Contexto RAG no disponible. Motivo: {exc}"
        trace_message = f"Agente RAG no pudo recuperar contexto: {exc}"

    _emit_progress(state, "rag_ok", "Contexto RAG listo.", {"chars": len(str(context))})
    return {
        **state,
        "rag_context": str(context),
        "trace": _append_trace(state, trace_message),
    }


def writer_agent_node(state: WindowReportState) -> WindowReportState:
    """Generate one short narrative fragment for the closed window."""
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    prompt = (
        f"Redacta un fragmento de informe para los minutos {start}-{end}.\n"
        "Debe tener entre 80 y 130 palabras, empezar con el rango de minutos, "
        "usar solo las metricas/eventos dados y apoyarse en el RAG sin inventar.\n\n"
        f"METRICAS DE LA VENTANA:\n{json.dumps(state.get('window_metrics', {}), ensure_ascii=False, default=str)}\n\n"
        f"EVENTOS REPRESENTATIVOS:\n{json.dumps(state.get('window_events', []), ensure_ascii=False, default=str)}\n\n"
        f"METRICAS ACUMULADAS:\n{json.dumps(state.get('cumulative_metrics', {}), ensure_ascii=False, default=str)}\n\n"
        f"CONTEXTO RAG:\n{state.get('rag_context', '')}"
    )

    _emit_progress(state, "llm", f"Generando texto para minutos {start}-{end}.")
    try:
        text, model = _invoke_text_llm(prompt)
    except Exception as exc:  # noqa: BLE001
        text, model = _fallback_text(state, str(exc))

    return {
        **state,
        "final_prompt": prompt,
        "text": text,
        "llm_model": model,
        "trace": _append_trace(state, f"Agente redactor genero el fragmento ({model})."),
    }


def _invoke_text_llm(prompt: str) -> tuple[str, str]:
    """Call Ollama directly and return plain text."""
    if os.getenv("LLM_PROVIDER", "ollama").strip().lower() != "ollama":
        raise RuntimeError("Solo se soporta LLM_PROVIDER=ollama en este proyecto.")

    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    timeout_seconds = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "45"))
    max_retries = max(1, int(os.getenv("LLM_MAX_RETRIES", "2")))
    backoff_seconds = float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "2"))

    messages = [
        {
            "role": "system",
            "content": (
                "Eres un analista deportivo. Escribe texto natural en espanol. "
                "No devuelvas JSON ni listas de datos crudos."
            ),
        },
        {"role": "user", "content": prompt},
    ]

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            text = _call_ollama(base_url, model_name, messages, timeout_seconds).strip()
            if not text:
                raise ValueError("Ollama devolvio texto vacio.")
            if text.lstrip().startswith(("{", "[")):
                raise ValueError("El LLM devolvio datos estructurados en lugar de texto.")
            return text, f"ollama:{model_name}"
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)
    raise RuntimeError(f"No se pudo generar texto con Ollama: {last_error}")


def _call_ollama(
    base_url: str,
    model_name: str,
    messages: list[dict[str, str]],
    timeout_seconds: float,
) -> str:
    """Call the local Ollama chat endpoint."""
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/chat",
        data=json.dumps(
            {
                "model": model_name,
                "messages": messages,
                "stream": False,
                "options": {"temperature": 0.2},
            }
        ).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"Ollama no respondio correctamente. Revisa `ollama serve` y `ollama pull {model_name}`."
        ) from exc

    content = (payload.get("message") or {}).get("content")
    if not content:
        raise ValueError("Ollama respondio sin contenido.")
    return str(content)


def _fallback_text(state: WindowReportState, reason: str) -> tuple[str, str]:
    """Deterministic text used when the LLM is not available."""
    metrics = state.get("window_metrics", {})
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    teams = metrics.get("teams", [])
    leader = max(teams, key=lambda item: item.get("events", 0), default={})
    text = (
        f"Minutos {start}-{end}: en esta ventana se registraron "
        f"{metrics.get('total_events', 0)} eventos, {metrics.get('total_shots', 0)} tiros, "
        f"{metrics.get('total_goals', 0)} goles y {metrics.get('total_pressures', 0)} presiones. "
        f"El equipo con mas actividad fue {leader.get('team', 'N/D')}. "
        "El resumen procede de las metricas calculadas para la ventana y del contexto RAG disponible. "
        f"No se pudo usar el LLM principal, por lo que se genero este texto de respaldo. Motivo: {reason}"
    )
    return text, f"fallback:{reason}"


class _LocalWindowGraph:
    """Fallback runner with the same node order as the LangGraph graph."""

    def invoke(self, state: WindowReportState) -> WindowReportState:
        state = metrics_agent_node(state)
        state = rag_agent_node(state)
        state = writer_agent_node(state)
        return {**state, "used_langgraph": False}


def build_window_graph():
    """Build the only agent graph used by the project."""
    if not LANGGRAPH_AVAILABLE:
        return _LocalWindowGraph()

    graph = StateGraph(WindowReportState)
    graph.add_node("metrics_agent", metrics_agent_node)
    graph.add_node("rag_agent", rag_agent_node)
    graph.add_node("writer_agent", writer_agent_node)
    graph.set_entry_point("metrics_agent")
    graph.add_edge("metrics_agent", "rag_agent")
    graph.add_edge("rag_agent", "writer_agent")
    graph.add_edge("writer_agent", END)
    return graph.compile()


def run_window_analysis_workflow(
    match_id: str,
    window_start_minute: int,
    window_end_minute: int,
    window_metrics: dict[str, Any],
    window_events: list[dict[str, Any]],
    cumulative_metrics: dict[str, Any],
    progress_callback: ProgressCallback | None = None,
) -> WindowReportState:
    """Generate one incremental report fragment for one closed 5-minute window."""
    initial_state: WindowReportState = {
        "match_id": match_id,
        "window_start_minute": window_start_minute,
        "window_end_minute": window_end_minute,
        "window_metrics": window_metrics,
        "window_events": window_events,
        "cumulative_metrics": cumulative_metrics,
        "trace": [f"Inicio de fragmento minutos {window_start_minute}-{window_end_minute}."],
        "used_langgraph": LANGGRAPH_AVAILABLE,
        "progress_callback": progress_callback,
    }
    result = build_window_graph().invoke(initial_state)
    output = {**result, "used_langgraph": LANGGRAPH_AVAILABLE}
    output.pop("progress_callback", None)
    return output
