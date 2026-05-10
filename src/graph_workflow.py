"""LangGraph workflow for orchestrating metric tools, RAG tools, and report drafting."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from typing import Any, Callable, TypedDict

from tools import query_match_metrics, retrieve_document_context

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


class ReportState(TypedDict, total=False):
    """State shared by the report-generation graph."""

    user_request: str
    metrics_query: str
    rag_query: str
    metrics_json: str
    metrics: dict[str, Any]
    rag_context: str
    final_prompt: str
    report_sections: dict[str, str]
    llm_model: str
    trace: list[str]
    used_langgraph: bool


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
    progress_callback: Callable[[str, str, dict[str, Any] | None], None]


def _append_trace(state: ReportState, message: str) -> list[str]:
    return [*state.get("trace", []), message]


def _emit_window_progress(
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


def metrics_agent_node(state: ReportState) -> ReportState:
    """Agent node that explicitly uses the match metrics tool."""
    query = state.get(
        "metrics_query",
        "resumen, destacado, indices ofensivos y defensivos, intensidad, tiros y pases",
    )
    metrics_json = query_match_metrics.invoke(query) if hasattr(query_match_metrics, "invoke") else query_match_metrics(query)
    try:
        metrics = json.loads(metrics_json)
    except json.JSONDecodeError:
        metrics = {"status": "error", "raw": metrics_json}

    return {
        **state,
        "metrics_query": query,
        "metrics_json": metrics_json,
        "metrics": metrics,
        "trace": _append_trace(state, f"Tool 1 query_match_metrics ejecutada: {query}"),
    }


def rag_agent_node(state: ReportState) -> ReportState:
    """Agent node that explicitly uses the RAG retrieval tool."""
    metrics = state.get("metrics", {})
    team = metrics.get("highlighted_team", {}).get("team", "equipos del partido")
    player = metrics.get("highlighted_player", {}).get("player", "jugadores destacados")
    query = state.get(
        "rag_query",
        f"estilo tactico {team}, perfil {player}, indices ofensivos y defensivos, intensidad y rendimiento",
    )
    context = (
        retrieve_document_context.invoke(query)
        if hasattr(retrieve_document_context, "invoke")
        else retrieve_document_context(query)
    )
    return {
        **state,
        "rag_query": query,
        "rag_context": context,
        "trace": _append_trace(state, f"Tool 2 retrieve_document_context ejecutada: {query}"),
    }


def _llm_messages(final_prompt: str) -> list[tuple[str, str]]:
    """Build provider-agnostic chat messages for the report LLM."""
    system_prompt = (
        "Eres un analista deportivo senior. Debes redactar en espanol un informe "
        "automatico, claro y verificable para un stakeholder no tecnico. "
        "No inventes datos: distingue hechos derivados del parquet Spark y contexto "
        "documental recuperado mediante RAG. Devuelve exclusivamente JSON valido con "
        "estas claves: resumen_general, destacado, momento_intensidad, interpretacion, "
        "contexto_rag, conclusion."
    )
    return [("system", system_prompt), ("user", final_prompt)]


def _extract_json_object(raw_text: str) -> dict[str, str]:
    """Parse the JSON object returned by the LLM."""
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError:
        start = raw_text.find("{")
        end = raw_text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("El LLM no devolvio un objeto JSON valido.") from None
        parsed = json.loads(raw_text[start : end + 1])

    required = [
        "resumen_general",
        "destacado",
        "momento_intensidad",
        "interpretacion",
        "contexto_rag",
        "conclusion",
    ]
    missing = [key for key in required if key not in parsed]
    if missing:
        raise ValueError(f"El JSON del LLM no contiene las secciones requeridas: {missing}")

    return {key: str(parsed[key]) for key in required}


def _invoke_json_llm(final_prompt: str, required: list[str]) -> tuple[dict[str, str], str]:
    """Invoke Ollama and parse a JSON object with the requested keys."""
    provider = os.getenv("LLM_PROVIDER", "ollama").strip().lower()
    if provider != "ollama":
        raise RuntimeError("Este proyecto esta configurado para usar LLM_PROVIDER=ollama.")

    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    timeout_seconds = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "45"))
    max_retries = max(1, int(os.getenv("LLM_MAX_RETRIES", "2")))
    backoff_seconds = float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "2"))
    messages = [
        (
            "system",
            "Eres un analista deportivo senior. Responde exclusivamente con JSON valido. "
            "No inventes datos: separa datos observados y contexto documental RAG. "
            "Usa tono profesional, sin vulgaridades ni expresiones coloquiales. "
            "Cuando haya metricas agregadas y ejemplos de eventos, las metricas agregadas "
            "son la fuente de verdad; los ejemplos solo ilustran.",
        ),
        ("user", final_prompt),
    ]

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            raw_content = _call_ollama_chat(
                base_url=base_url,
                model_name=model_name,
                messages=messages,
                timeout_seconds=timeout_seconds,
            )
            parsed = _parse_json_payload(raw_content)
            normalized = _normalize_required_payload(parsed, required)
            return normalized, f"ollama:{model_name}"
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)

    raise RuntimeError(
        "No se pudo obtener una respuesta JSON valida de Ollama tras "
        f"{max_retries} intento(s): {last_error}"
    )


def _invoke_text_llm(final_prompt: str) -> tuple[str, str]:
    """Invoke Ollama for a plain Spanish narrative text."""
    provider = os.getenv("LLM_PROVIDER", "ollama").strip().lower()
    if provider != "ollama":
        raise RuntimeError("Este proyecto esta configurado para usar LLM_PROVIDER=ollama.")

    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    timeout_seconds = float(os.getenv("OLLAMA_TIMEOUT_SECONDS", "45"))
    max_retries = max(1, int(os.getenv("LLM_MAX_RETRIES", "2")))
    backoff_seconds = float(os.getenv("LLM_RETRY_BACKOFF_SECONDS", "2"))
    messages = [
        (
            "system",
            "Eres un analista deportivo senior. Redacta texto natural en espanol. "
            "No devuelvas JSON, diccionarios, listas de Python, tablas ni nombres de campos. "
            "No copies datos crudos: interpreta las metricas del contexto RAG incremental.",
        ),
        ("user", final_prompt),
    ]

    last_error: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            text = _call_ollama_chat(
                base_url=base_url,
                model_name=model_name,
                messages=messages,
                timeout_seconds=timeout_seconds,
                json_format=False,
            ).strip()
            if not text:
                raise ValueError("Ollama devolvio texto vacio.")
            lowered = text.lower()
            if text.lstrip().startswith(("{", "[")) or any(
                token in lowered for token in ("{'team'", '"teams"', "offensive_index", "defensive_index")
            ):
                raise ValueError("El LLM copio datos crudos en lugar de redactar un resumen.")
            return text, f"ollama:{model_name}"
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)

    raise RuntimeError(f"No se pudo obtener texto narrativo valido de Ollama: {last_error}")


def _call_ollama_chat(
    base_url: str,
    model_name: str,
    messages: list[tuple[str, str]],
    timeout_seconds: float,
    json_format: bool = True,
) -> str:
    """Call Ollama directly with an HTTP timeout."""
    url = f"{base_url.rstrip('/')}/api/chat"
    payload = {
        "model": model_name,
        "messages": [{"role": role, "content": content} for role, content in messages],
        "stream": False,
        "options": {"temperature": 0.2},
    }
    if json_format:
        payload["format"] = "json"
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
            response_payload = json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            "Ollama no respondio correctamente. Comprueba `ollama serve`, "
            f"el modelo `{model_name}` y OLLAMA_BASE_URL={base_url}."
        ) from exc

    content = (response_payload.get("message") or {}).get("content")
    if not content:
        raise ValueError("Ollama respondio sin contenido de mensaje.")
    return str(content)


def _parse_json_payload(raw_content: str) -> dict[str, Any]:
    """Parse a JSON object from a direct LLM response."""
    try:
        parsed = json.loads(raw_content)
    except json.JSONDecodeError:
        start = raw_content.find("{")
        end = raw_content.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("El LLM no devolvio un objeto JSON valido.") from None
        parsed = json.loads(raw_content[start : end + 1])

    if not isinstance(parsed, dict):
        raise ValueError("El JSON del LLM no es un objeto.")
    return parsed


def _normalize_required_payload(parsed: dict[str, Any], required: list[str]) -> dict[str, str]:
    """Normalize common LLM key variants before enforcing required keys."""
    if required == ["texto"] and "texto" not in parsed:
        for alias in ("text", "respuesta", "resumen", "analisis", "narrativa"):
            if alias in parsed:
                parsed["texto"] = parsed[alias]
                break
        if "texto" not in parsed and len(parsed) == 1:
            parsed["texto"] = next(iter(parsed.values()))

    missing = [key for key in required if key not in parsed]
    if missing:
        raise ValueError(f"El JSON del LLM no contiene las claves requeridas: {missing}")
    return {key: str(parsed[key]) for key in required}


def _invoke_report_llm(final_prompt: str) -> tuple[dict[str, str], str]:
    """Invoke the configured LLM and return report sections plus model id."""
    required = [
        "resumen_general",
        "destacado",
        "momento_intensidad",
        "interpretacion",
        "contexto_rag",
        "conclusion",
    ]
    return _invoke_json_llm(final_prompt, required)


def drafting_agent_node(state: ReportState) -> ReportState:
    """Agent node that calls an LLM to turn metrics and RAG context into report sections."""
    metrics = state.get("metrics", {})
    final_prompt = (
        "Genera el informe automatico del partido siguiendo estas directrices obligatorias:\n"
        "1. resumen general del partido;\n"
        "2. equipo o jugador destacado;\n"
        "3. momento o tramo de mayor intensidad;\n"
        "4. interpretacion basica del rendimiento observado;\n"
        "5. conclusion final orientada a un stakeholder no tecnico.\n\n"
        "Separa explicitamente lo que procede de datos del partido procesados en tiempo real "
        "de lo que procede del contexto documental recuperado.\n\n"
        f"DATOS DEL PARTIDO DESDE SPARK PARQUET:\n{json.dumps(metrics, ensure_ascii=False, default=str)}\n\n"
        f"CONTEXTO DOCUMENTAL RAG RECUPERADO:\n{state.get('rag_context', '')}"
    )
    sections, llm_model = _invoke_report_llm(final_prompt)

    return {
        **state,
        "final_prompt": final_prompt,
        "report_sections": sections,
        "llm_model": llm_model,
        "trace": _append_trace(
            state,
            f"Nodo de redaccion final invoco LLM real ({llm_model}) con prompt enriquecido.",
        ),
    }


def window_metrics_agent_node(state: WindowReportState) -> WindowReportState:
    """Prepare a compact query from one closed match-minute window."""
    _emit_window_progress(state, "metricas", "Preparando consulta RAG desde metricas de ventana.")
    window_metrics = state.get("window_metrics", {})
    teams = window_metrics.get("teams", [])
    top_team = (
        window_metrics.get("activity_leader")
        or window_metrics.get("dominant_team")
        or max(teams, key=lambda item: item.get("events", 0), default={})
    )
    defensive_team = max(teams, key=lambda item: item.get("defensive_index", 0), default={})
    query = (
        "interpretacion de tramo de partido con "
        f"eventos={window_metrics.get('total_events', 0)}, "
        f"tiros={window_metrics.get('total_shots', 0)}, "
        f"goles={window_metrics.get('total_goals', 0)}, "
        f"indice ofensivo={window_metrics.get('offensive_index', 0)}, "
        f"indice defensivo={window_metrics.get('defensive_index', 0)}, "
        f"equipo defensivo destacado {defensive_team.get('team', 'sin equipo')}, "
        f"equipo mas activo {top_team.get('team', 'sin equipo')}, "
        "intensidad, presion, pases, duelos, rendimiento y redaccion para stakeholder"
    )
    next_state = {
        **state,
        "rag_query": state.get("rag_query", query),
        "trace": _append_trace(state, "Agente de metricas preparo resumen de ventana cerrada."),
    }
    _emit_window_progress(
        next_state,
        "metricas_ok",
        "Metricas de ventana listas para alimentar RAG y LLM.",
        {"rag_query": next_state.get("rag_query", query)},
    )
    return next_state


def window_rag_agent_node(state: WindowReportState) -> WindowReportState:
    """Retrieve RAG context for one incremental window."""
    query = state.get("rag_query", "intensidad presion tiros pases duelos rendimiento")
    existing_context = str(state.get("rag_context", "")).strip()
    if existing_context:
        _emit_window_progress(
            state,
            "rag_ok",
            "Contexto RAG incremental recuperado desde ventanas procesadas.",
            {"rag_context_chars": len(existing_context)},
        )
        return {
            **state,
            "rag_context": existing_context,
            "trace": _append_trace(state, "Agente RAG uso contexto incremental de ventanas procesadas."),
        }

    _emit_window_progress(state, "rag_inicio", f"Recuperando contexto RAG: {query}")
    try:
        context = (
            retrieve_document_context.invoke(query)
            if hasattr(retrieve_document_context, "invoke")
            else retrieve_document_context(query)
        )
        trace_message = f"Agente RAG recupero contexto para ventana: {query}"
        _emit_window_progress(
            state,
            "rag_ok",
            "Contexto RAG recuperado.",
            {"rag_context_chars": len(str(context))},
        )
    except Exception as exc:  # noqa: BLE001
        context = f"Contexto RAG no disponible para esta ventana. Motivo: {exc}"
        trace_message = f"ERROR RAG en ventana: {exc}"
        _emit_window_progress(
            state,
            "rag_error",
            f"Problema recuperando RAG; se continua con contexto de fallback: {exc}",
            {"error": str(exc)},
        )
    return {
        **state,
        "rag_context": context,
        "trace": _append_trace(state, trace_message),
    }


def _fallback_window_text(state: WindowReportState, reason: str) -> tuple[str, str]:
    """Create a deterministic segment if the LLM is unavailable."""
    metrics = state.get("window_metrics", {})
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    total_events = metrics.get("total_events", 0)
    total_shots = metrics.get("total_shots", 0)
    total_goals = metrics.get("total_goals", 0)
    offensive_index = metrics.get("offensive_index", 0)
    defensive_index = metrics.get("defensive_index", 0)
    teams = metrics.get("teams", [])
    top_team = (
        metrics.get("activity_leader")
        or metrics.get("dominant_team")
        or max(teams, key=lambda item: item.get("events", 0), default={})
    )
    team_fact_line = metrics.get("team_fact_line", "")
    text = (
        f"Minutos {start}-{end}: en esta ventana se registraron {total_events} eventos, "
        f"{total_shots} tiros y {total_goals} goles. "
        f"Los indices agregados fueron {offensive_index} en ataque y {defensive_index} en defensa. "
        f"El equipo con mas actividad fue {top_team.get('team', 'N/D')} "
        f"({top_team.get('events', 0)} eventos, {top_team.get('passes', 0)} pases). "
        f"Detalle por equipos: {team_fact_line} "
        "La lectura debe considerarse descriptiva: procede de los eventos y metricas "
        "calculadas para este tramo, con apoyo contextual del RAG cuando esta disponible."
    )
    return text, f"fallback:{reason}"


def window_drafting_agent_node(state: WindowReportState) -> WindowReportState:
    """Generate the short incremental narrative for one closed window."""
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    final_prompt = (
        "Redacta un resumen incremental del partido usando SOLO el CONTEXTO RAG INCREMENTAL.\n"
        "Debe estar en espanol, tener entre 90 y 150 palabras, y empezar indicando "
        f"claramente 'Minutos {start}-{end}'.\n"
        "Reglas obligatorias:\n"
        "- Escribe texto natural, no JSON.\n"
        "- No copies diccionarios, listas, claves internas ni datos crudos.\n"
        "- Interpreta que equipo domina, que equipo resiste, si hubo tiro/gol/presion y por que.\n"
        "- Si no hubo tiros o goles, dilo claramente y no inventes ocasiones.\n"
        "- Puedes mencionar 2 o 3 metricas concretas, pero integradas en frases.\n"
        "- Mantén tono profesional, sin vulgaridades ni expresiones coloquiales.\n\n"
        f"CONTEXTO RAG INCREMENTAL:\n{state.get('rag_context', '')}"
    )
    _emit_window_progress(state, "llm_inicio", f"Invocando LLM para minutos {start}-{end}.")
    try:
        text, llm_model = _invoke_text_llm(final_prompt)
        _emit_window_progress(
            state,
            "llm_ok",
            f"LLM genero resumen narrativo con modelo {llm_model}.",
            {"model": llm_model},
        )
    except Exception as exc:
        text, llm_model = _fallback_window_text(state, str(exc))
        _emit_window_progress(
            state,
            "llm_error",
            f"Problema con LLM; se genera fallback determinista: {exc}",
            {"error": str(exc)},
        )

    next_state = {
        **state,
        "final_prompt": final_prompt,
        "text": text,
        "llm_model": llm_model,
        "trace": _append_trace(state, f"Agente redactor genero segmento incremental ({llm_model})."),
    }
    _emit_window_progress(
        next_state,
        "texto_ok",
        f"Texto incremental finalizado para minutos {start}-{end}.",
        {"model": llm_model},
    )
    return next_state


class _FallbackGraph:
    """Small local runner used only when LangGraph is not installed."""

    def invoke(self, state: ReportState) -> ReportState:
        state = metrics_agent_node(state)
        state = rag_agent_node(state)
        state = drafting_agent_node(state)
        return {**state, "used_langgraph": False}


class _FallbackWindowGraph:
    """Local runner for incremental window reports when LangGraph is unavailable."""

    def invoke(self, state: WindowReportState) -> WindowReportState:
        state = window_metrics_agent_node(state)
        state = window_rag_agent_node(state)
        state = window_drafting_agent_node(state)
        return {**state, "used_langgraph": False}


def build_graph():
    """Build and compile the LangGraph analysis workflow."""
    if not LANGGRAPH_AVAILABLE:
        return _FallbackGraph()

    graph = StateGraph(ReportState)
    graph.add_node("metrics_agent", metrics_agent_node)
    graph.add_node("rag_agent", rag_agent_node)
    graph.add_node("drafting_agent", drafting_agent_node)
    graph.set_entry_point("metrics_agent")
    graph.add_edge("metrics_agent", "rag_agent")
    graph.add_edge("rag_agent", "drafting_agent")
    graph.add_edge("drafting_agent", END)
    return graph.compile()


def build_window_graph():
    """Build and compile the incremental window report workflow."""
    if not LANGGRAPH_AVAILABLE:
        return _FallbackWindowGraph()

    graph = StateGraph(WindowReportState)
    graph.add_node("window_metrics_agent", window_metrics_agent_node)
    graph.add_node("window_rag_agent", window_rag_agent_node)
    graph.add_node("window_drafting_agent", window_drafting_agent_node)
    graph.set_entry_point("window_metrics_agent")
    graph.add_edge("window_metrics_agent", "window_rag_agent")
    graph.add_edge("window_rag_agent", "window_drafting_agent")
    graph.add_edge("window_drafting_agent", END)
    return graph.compile()


def run_analysis_workflow(user_request: str = "Generar informe automatico del partido") -> ReportState:
    """Execute the report workflow and return the full traceable state."""
    initial_state: ReportState = {
        "user_request": user_request,
        "trace": ["Inicio del flujo de analisis para informe PDF."],
        "used_langgraph": LANGGRAPH_AVAILABLE,
    }
    result = build_graph().invoke(initial_state)
    return {**result, "used_langgraph": LANGGRAPH_AVAILABLE}


def run_window_analysis_workflow(
    match_id: str,
    window_start_minute: int,
    window_end_minute: int,
    window_metrics: dict[str, Any],
    window_events: list[dict[str, Any]],
    cumulative_metrics: dict[str, Any],
    rag_context: str = "",
    progress_callback: Callable[[str, str, dict[str, Any] | None], None] | None = None,
) -> WindowReportState:
    """Generate one incremental report segment for a closed match-minute window."""
    initial_state: WindowReportState = {
        "match_id": match_id,
        "window_start_minute": window_start_minute,
        "window_end_minute": window_end_minute,
        "window_metrics": window_metrics,
        "window_events": window_events,
        "cumulative_metrics": cumulative_metrics,
        "rag_context": rag_context,
        "trace": [f"Inicio de segmento incremental minutos {window_start_minute}-{window_end_minute}."],
        "used_langgraph": LANGGRAPH_AVAILABLE,
        "progress_callback": progress_callback,
    }
    _emit_window_progress(
        initial_state,
        "workflow_inicio",
        f"Arranca workflow de texto para minutos {window_start_minute}-{window_end_minute}.",
    )
    result = build_window_graph().invoke(initial_state)
    output = {**result, "used_langgraph": LANGGRAPH_AVAILABLE}
    output.pop("progress_callback", None)
    _emit_window_progress(
        initial_state,
        "workflow_fin",
        f"Workflow de texto terminado para minutos {window_start_minute}-{window_end_minute}.",
    )
    return output
