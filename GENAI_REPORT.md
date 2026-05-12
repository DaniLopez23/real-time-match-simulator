# Flujo GenAI: tools, agentes, RAG y resumen cada 5 minutos

Este documento describe el flujo actual. Ya no hay un flujo general de informe completo ni un nodo que pida JSON estructurado al LLM. El sistema solo genera fragmentos incrementales por ventanas de partido.

## Idea principal

Cada `REPORT_WINDOW_MINUTES` minutos de partido, por defecto 5:

1. Spark consolida eventos y metricas.
2. El pipeline detecta si una ventana ya esta cerrada.
3. Los agentes preparan datos, recuperan RAG y redactan un fragmento.
4. El fragmento se guarda en Parquet.
5. Streamlit muestra los fragmentos.
6. El PDF se compone con esos textos, metricas y graficas.

## Ficheros implicados

- `src/streaming_pipeline.py`: consume Kafka, calcula metricas y lanza la generacion cuando hay ventanas cerradas.
- `src/incremental_report.py`: calcula metricas de una ventana y guarda los textos generados.
- `src/graph_workflow.py`: contiene el unico grafo de agentes.
- `src/tools.py`: expone la tool RAG documental.
- `src/rag_pipeline.py`: recupera contexto desde `data/docs/rag_corpus`.
- `src/report_generator.py`: compone el PDF desde artefactos ya generados.
- `src/app.py`: muestra textos, metricas, fuentes RAG y PDF.

## Grafo de agentes

El flujo es deliberadamente simple:

```text
metrics_agent -> rag_agent -> writer_agent
```

### 1. `metrics_agent`

Recibe:

- `window_metrics`
- `window_events`
- `cumulative_metrics`

Su trabajo es preparar una consulta RAG breve con los hechos principales de la ventana: eventos, tiros, goles, presiones, equipo mas activo e intensidad.

### 2. `rag_agent`

Usa la tool:

```python
retrieve_document_context(query)
```

Esa tool recupera fragmentos desde el corpus local:

```text
data/docs/rag_corpus
```

El RAG aporta criterios de interpretacion y redaccion. No decide lo que ocurrio en el partido.

### 3. `writer_agent`

Recibe:

- metricas de la ventana;
- eventos representativos;
- metricas acumuladas;
- contexto RAG.

Llama a Ollama para escribir un texto natural en espanol. El texto debe empezar con el rango de minutos, por ejemplo:

```text
Minutos 10-15: ...
```

Si Ollama no responde, se guarda un fallback determinista para que el streaming no se bloquee.

## Persistencia

Cada fragmento se guarda en:

```text
output/reports/incremental_segments.parquet
```

Columnas principales:

- `match_id`
- `window_start_minute`
- `window_end_minute`
- `text`
- `metrics_json`
- `rag_context`
- `llm_model`
- `trace_json`
- `generated_at`

## Visualizacion y PDF

Streamlit no llama al LLM. Solo lee artefactos:

- eventos;
- metricas acumuladas;
- metricas por ventana;
- segmentos generados;
- PDF ya compuesto.

El PDF se refresca desde el pipeline cuando se genera un segmento nuevo. Incluye los textos de las ventanas, metricas y graficas.
