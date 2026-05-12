# Flujo GenAI: agentes, tools, RAG y LLM

Este documento explica como el proyecto genera textos automaticos a partir de metricas de partido, documentos RAG y un LLM local. El flujo esta pensado para producir narrativas incrementales por ventanas de partido, por ejemplo cada 5 minutos, y despues componer un PDF final.

## Objetivo del flujo

El objetivo no es que el LLM mire Kafka ni procese eventos crudos. El LLM recibe contexto ya preparado:

- metricas calculadas por Spark y pandas;
- eventos representativos de una ventana cerrada;
- documentos RAG creados a partir de esa ventana y de ventanas anteriores;
- reglas de redaccion que obligan a no inventar datos.

Asi se separan responsabilidades:

- Spark calcula hechos.
- RAG aporta contexto interpretable.
- Agentes ordenan el trabajo.
- LLM redacta.
- Fallback determinista evita que falle el pipeline completo.

## Componentes principales

### `incremental_report.py`

Coordina la generacion incremental de segmentos. Detecta ventanas cerradas, prepara payloads de metricas, genera documentos RAG incrementales, ejecuta el workflow de agentes y persiste el resultado.

Funciones clave:

- `report_window_status()`: identifica ventanas abiertas, cerradas, pendientes o ya generadas.
- `generate_missing_report_segments()`: genera textos para ventanas cerradas pendientes.
- `build_window_payload()`: calcula metricas y ejemplos de eventos para una ventana.
- `build_window_rag_document()`: transforma datos de ventana en un documento textual.
- `upsert_incremental_rag_document()`: guarda o reemplaza el documento RAG incremental.
- `build_incremental_rag_context()`: recupera el documento actual y hasta tres ventanas anteriores.

### `graph_workflow.py`

Define el workflow de agentes. Usa LangGraph si esta instalado; si no, usa una clase fallback que ejecuta los nodos en secuencia.

Hay dos workflows:

- `build_graph()`: flujo general para informe completo.
- `build_window_graph()`: flujo incremental para una ventana cerrada.

El flujo incremental es el mas importante para este proyecto en tiempo real.

### `tools.py`

Expone tools invocables por los agentes:

- `query_match_metrics(query)`: lee Parquet de Spark y devuelve insights estructurados en JSON.
- `retrieve_document_context(query)`: recupera contexto documental desde el corpus RAG local.

En el flujo incremental, el contexto RAG principal se construye desde `incremental_report.py`, pero la tool documental sigue disponible para el workflow general y como apoyo semantico.

### `rag_pipeline.py`

Implementa el RAG documental local.

Fuentes:

- `data/docs/rag_corpus/performance_indicators.md`
- `data/docs/rag_corpus/reporting_guidelines.md`
- `data/docs/rag_corpus/tactical_profiles.md`

Backends:

- Chroma + embeddings de HuggingFace si estan instaladas las dependencias opcionales.
- Fallback TF-IDF local si no estan disponibles Chroma, LangChain o los embeddings.

El modelo de embeddings por defecto es `sentence-transformers/all-mpnet-base-v2`.

### `report_generator.py`

No llama al LLM. Genera el PDF desde artefactos ya existentes:

- metricas Parquet;
- segmentos incrementales;
- graficos y tablas;
- trazabilidad basica del origen de los textos.

## Flujo paso a paso de una ventana incremental

### 1. Spark detecta una ventana cerrada

`streaming_pipeline.py` escribe snapshots cada `SNAPSHOT_INTERVAL_SECONDS`. Despues de cada snapshot llama a `_generate_incremental_reports()`.

Esa funcion ejecuta:

```python
generate_missing_report_segments(max_windows=1, progress_callback=_report_generation_log)
```

El parametro `max_windows=1` hace que el pipeline genere como maximo un segmento por ciclo, evitando bloquear demasiado el streaming.

### 2. Se calcula el estado de ventanas

`report_window_status()` lee `output/processed/events.parquet` y calcula:

- ventana de cada evento segun `REPORT_WINDOW_MINUTES`;
- reloj actual detectado del partido;
- si una ventana esta cerrada;
- si ya tiene texto generado;
- estado final: `abierta`, `pendiente` o `generada`.

Una ventana se considera cerrada cuando el maximo minuto/segundo observado ya supera el final de la ventana.

### 3. Se prepara el payload de metricas

Para cada ventana cerrada pendiente, `build_window_payload()` genera tres bloques:

- `window_metrics`: metricas agregadas de la ventana.
- `window_events`: eventos representativos.
- `cumulative_metrics`: resumen acumulado hasta el final de la ventana.

Incluye datos como:

- eventos totales;
- tiros y goles;
- presiones;
- indice ofensivo y defensivo;
- metricas por equipo;
- equipo mas activo;
- equipo de mayor impacto;
- jugador destacado;
- desglose por tipo de evento;
- muestra de eventos relevantes.

### 4. Se crea un documento RAG incremental

`build_window_rag_document()` convierte el payload en texto. Este texto no es aun la narracion final: es un documento de contexto para que el LLM tenga una fuente compacta y verificable.

El documento contiene:

- rango de minutos;
- partido;
- metricas totales;
- equipo mas activo;
- impacto ofensivo/defensivo;
- jugador destacado;
- metricas por equipo;
- desglose de eventos;
- ejemplos representativos;
- contexto acumulado.

Ese documento se guarda en:

```text
output/reports/incremental_rag.parquet
```

### 5. Se recupera contexto RAG incremental

`build_incremental_rag_context()` arma el contexto que recibira el agente:

- documento de la ventana actual;
- hasta tres documentos previos del mismo partido.

Esto permite que el texto de una ventana no este aislado: puede mencionar continuidad, cambio de ritmo o acumulacion de dominio sin inventar informacion.

### 6. Se ejecuta el workflow de agentes

`run_window_analysis_workflow()` inicia el grafo de ventana con:

- `match_id`;
- minuto inicial y final;
- metricas de ventana;
- eventos representativos;
- metricas acumuladas;
- contexto RAG incremental;
- callback de progreso.

Si LangGraph esta disponible, se compila un `StateGraph`. Si no, `_FallbackWindowGraph` ejecuta los nodos en orden.

El estado compartido se llama `WindowReportState`.

### 7. Agente de metricas

Nodo: `window_metrics_agent_node()`.

Responsabilidad:

- inspecciona las metricas de ventana;
- identifica equipo mas activo y equipo defensivo destacado;
- construye una query semantica con eventos, tiros, goles, indices, presion, pases, duelos e intensidad;
- registra trazabilidad.

Salida principal:

- `rag_query`.

### 8. Agente RAG

Nodo: `window_rag_agent_node()`.

Responsabilidad:

- si ya existe `rag_context`, lo usa directamente;
- si no existe, invoca la tool `retrieve_document_context`;
- registra si el RAG fue correcto o si hubo fallback.

En el flujo incremental normal, el contexto ya viene preparado desde `incremental_report.py`, asi que el nodo suele reutilizar ese contexto.

### 9. Agente redactor

Nodo: `window_drafting_agent_node()`.

Responsabilidad:

- construye el prompt final;
- invoca el LLM;
- valida que la respuesta sea texto natural, no JSON ni datos crudos;
- si falla, genera un fallback determinista.

Reglas del prompt:

- redactar en espanol;
- producir entre 90 y 150 palabras;
- empezar indicando `Minutos X-Y`;
- no devolver JSON;
- no copiar diccionarios ni claves internas;
- no inventar tiros o goles;
- mencionar solo metricas concretas integradas en frases;
- usar tono profesional.

### 10. Llamada al LLM

La funcion `_invoke_text_llm()` llama a Ollama mediante HTTP:

```text
POST {OLLAMA_BASE_URL}/api/chat
```

Variables:

- `LLM_PROVIDER=ollama`
- `OLLAMA_BASE_URL=http://localhost:11434` en host
- `OLLAMA_BASE_URL=http://host.docker.internal:11434` en Docker
- `OLLAMA_MODEL=llama3.2`
- `OLLAMA_TIMEOUT_SECONDS`
- `LLM_MAX_RETRIES`
- `LLM_RETRY_BACKOFF_SECONDS`

La temperatura se fija en `0.2` para reducir variabilidad.

### 11. Fallback determinista

Si Ollama no responde, devuelve texto vacio, responde con JSON o copia datos crudos, el sistema crea un texto determinista con:

- rango de minutos;
- eventos;
- tiros;
- goles;
- indices;
- equipo mas activo;
- detalle por equipos.

El modelo queda marcado como:

```text
fallback:<motivo>
```

Esto permite que el streaming siga funcionando aunque el LLM no este disponible.

### 12. Persistencia del segmento

Cada segmento se guarda en:

```text
output/reports/incremental_segments.parquet
```

Columnas:

- `segment_id`
- `match_id`
- `window_start_minute`
- `window_end_minute`
- `window_minutes`
- `text`
- `metrics_json`
- `rag_context`
- `llm_model`
- `trace_json`
- `generated_at`

La traza permite auditar que pasos se ejecutaron y detectar problemas de RAG o LLM.

### 13. Regeneracion del PDF

Cuando se genera al menos un segmento nuevo, `streaming_pipeline.py` llama a:

```python
generate_pdf_report(include_incremental=True)
```

El PDF se escribe en:

```text
output/reports/match_report.pdf
```

El PDF integra:

- resumen general de metricas;
- lectura evolutiva;
- tabla por equipos;
- jugador destacado;
- textos por ventanas;
- graficos de distribucion y evolucion;
- justificacion del origen de los artefactos.

## Diferencia entre RAG documental y RAG incremental

El proyecto usa dos ideas de RAG:

- RAG documental: recupera fragmentos desde `data/docs/rag_corpus`; aporta criterios tacticos, indicadores y guias de redaccion.
- RAG incremental: transforma cada ventana procesada en un documento textual y lo reutiliza para narrar la ventana actual con memoria reciente del partido.

El RAG incremental es el que soporta directamente los textos minuto a minuto. El documental aporta contexto experto cuando se invoca la tool `retrieve_document_context`.

## Donde se ve el resultado

Streamlit lee los segmentos desde `output/reports/incremental_segments.parquet` y los muestra en la pestana `Informe incremental`.

Para cada segmento se puede consultar:

- texto final;
- modelo usado o fallback;
- fecha de generacion;
- etapas de generacion;
- contexto RAG;
- metricas JSON.

La pestana `Exportar informe` solo ofrece descarga si `match_report.pdf` ya existe.
