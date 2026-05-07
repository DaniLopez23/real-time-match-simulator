"""LangGraph workflow for orchestrating metric tools, RAG tools, and report drafting."""

from __future__ import annotations

import json
import os
from typing import Any, TypedDict

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


def _append_trace(state: ReportState, message: str) -> list[str]:
    return [*state.get("trace", []), message]


def metrics_agent_node(state: ReportState) -> ReportState:
    """Agent node that explicitly uses the match metrics tool."""
    query = state.get(
        "metrics_query",
        "resumen, destacado, recuperaciones, intensidad, tiros y faltas",
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
        f"estilo tactico {team}, perfil {player}, recuperaciones, intensidad y rendimiento",
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

    try:
        from langchain_ollama import ChatOllama
    except ImportError as exc:
        raise RuntimeError("Instala `langchain-ollama` para generar texto con Ollama.") from exc

    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    llm = ChatOllama(model=model_name, base_url=base_url, temperature=0.2, format="json")
    messages = [
        (
            "system",
            "Eres un analista deportivo senior. Responde exclusivamente con JSON valido. "
            "No inventes datos: separa datos observados y contexto documental RAG.",
        ),
        ("user", final_prompt),
    ]

    try:
        response = llm.invoke(messages)
    except Exception as exc:
        raise RuntimeError(
            "No se pudo invocar Ollama. Comprueba que `ollama serve` esta activo "
            f"y que el modelo `{model_name}` esta descargado con `ollama pull {model_name}`."
        ) from exc

    try:
        parsed = json.loads(response.content)
    except json.JSONDecodeError:
        start = response.content.find("{")
        end = response.content.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError("El LLM no devolvio un objeto JSON valido.") from None
        parsed = json.loads(response.content[start : end + 1])

    missing = [key for key in required if key not in parsed]
    if missing:
        raise ValueError(f"El JSON del LLM no contiene las claves requeridas: {missing}")
    return {key: str(parsed[key]) for key in required}, f"ollama:{model_name}"


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
    provider = os.getenv("LLM_PROVIDER", "ollama").strip().lower()
    if provider != "ollama":
        raise RuntimeError("Este proyecto esta configurado para usar LLM_PROVIDER=ollama.")

    try:
        from langchain_ollama import ChatOllama
    except ImportError as exc:
        raise RuntimeError("Instala `langchain-ollama` para generar el informe con Ollama.") from exc

    model_name = os.getenv("OLLAMA_MODEL", "llama3.2")
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    llm = ChatOllama(model=model_name, base_url=base_url, temperature=0.2, format="json")

    try:
        response = llm.invoke(_llm_messages(final_prompt))
    except Exception as exc:
        raise RuntimeError(
            "No se pudo invocar Ollama. Comprueba que `ollama serve` esta activo "
            f"y que el modelo `{model_name}` esta descargado con `ollama pull {model_name}`."
        ) from exc
    parsed = _extract_json_object(response.content)
    return {key: parsed[key] for key in required}, f"ollama:{model_name}"


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
    window_metrics = state.get("window_metrics", {})
    teams = window_metrics.get("teams", [])
    top_team = max(teams, key=lambda item: item.get("events", 0), default={})
    recovery_team = max(teams, key=lambda item: item.get("recoveries", 0), default={})
    query = (
        "interpretacion de tramo de partido con "
        f"eventos={window_metrics.get('total_events', 0)}, "
        f"tiros={window_metrics.get('total_shots', 0)}, "
        f"goles={window_metrics.get('total_goals', 0)}, "
        f"recuperaciones destacadas de {recovery_team.get('team', 'sin equipo')}, "
        f"equipo mas activo {top_team.get('team', 'sin equipo')}, "
        "intensidad, presion, rendimiento y redaccion para stakeholder"
    )
    return {
        **state,
        "rag_query": state.get("rag_query", query),
        "trace": _append_trace(state, "Agente de metricas preparo resumen de ventana cerrada."),
    }


def window_rag_agent_node(state: WindowReportState) -> WindowReportState:
    """Retrieve RAG context for one incremental window."""
    query = state.get("rag_query", "intensidad recuperaciones tiros rendimiento")
    context = (
        retrieve_document_context.invoke(query)
        if hasattr(retrieve_document_context, "invoke")
        else retrieve_document_context(query)
    )
    return {
        **state,
        "rag_context": context,
        "trace": _append_trace(state, f"Agente RAG recupero contexto para ventana: {query}"),
    }


def _fallback_window_text(state: WindowReportState, reason: str) -> tuple[str, str]:
    """Create a deterministic segment if the LLM is unavailable."""
    metrics = state.get("window_metrics", {})
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    total_events = metrics.get("total_events", 0)
    total_shots = metrics.get("total_shots", 0)
    total_goals = metrics.get("total_goals", 0)
    teams = metrics.get("teams", [])
    top_team = max(teams, key=lambda item: item.get("events", 0), default={})
    text = (
        f"Minutos {start}-{end}: en esta ventana se registraron {total_events} eventos, "
        f"{total_shots} tiros y {total_goals} goles. "
        f"El equipo con mas actividad fue {top_team.get('team', 'N/D')}. "
        "La lectura debe considerarse descriptiva: procede de los eventos y metricas "
        "calculadas para este tramo, con apoyo contextual del RAG cuando esta disponible."
    )
    return text, f"fallback:{reason}"


def window_drafting_agent_node(state: WindowReportState) -> WindowReportState:
    """Generate the short incremental narrative for one closed window."""
    start = state.get("window_start_minute", 0)
    end = state.get("window_end_minute", 0)
    final_prompt = (
        "Genera un bloque incremental breve para el informe del partido.\n"
        "Debe estar en espanol, tener entre 80 y 140 palabras, y empezar indicando "
        f"claramente 'Minutos {start}-{end}'.\n"
        "Usa solo los datos observados de la ventana y el contexto RAG como apoyo "
        "interpretativo. No inventes sucesos, marcadores ni conclusiones no presentes.\n"
        "Devuelve exclusivamente JSON con esta clave: texto.\n\n"
        f"METRICAS DE LA VENTANA:\n{json.dumps(state.get('window_metrics', {}), ensure_ascii=False, default=str)}\n\n"
        f"EVENTOS DE LA VENTANA:\n{json.dumps(state.get('window_events', []), ensure_ascii=False, default=str)}\n\n"
        f"METRICAS ACUMULADAS HASTA EL MOMENTO:\n{json.dumps(state.get('cumulative_metrics', {}), ensure_ascii=False, default=str)}\n\n"
        f"CONTEXTO RAG:\n{state.get('rag_context', '')}"
    )
    try:
        payload, llm_model = _invoke_json_llm(final_prompt, ["texto"])
        text = payload["texto"]
    except Exception as exc:
        text, llm_model = _fallback_window_text(state, str(exc))

    return {
        **state,
        "final_prompt": final_prompt,
        "text": text,
        "llm_model": llm_model,
        "trace": _append_trace(state, f"Agente redactor genero segmento incremental ({llm_model})."),
    }


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
) -> WindowReportState:
    """Generate one incremental report segment for a closed match-minute window."""
    initial_state: WindowReportState = {
        "match_id": match_id,
        "window_start_minute": window_start_minute,
        "window_end_minute": window_end_minute,
        "window_metrics": window_metrics,
        "window_events": window_events,
        "cumulative_metrics": cumulative_metrics,
        "trace": [f"Inicio de segmento incremental minutos {window_start_minute}-{window_end_minute}."],
        "used_langgraph": LANGGRAPH_AVAILABLE,
    }
    result = build_window_graph().invoke(initial_state)
    return {**result, "used_langgraph": LANGGRAPH_AVAILABLE}
