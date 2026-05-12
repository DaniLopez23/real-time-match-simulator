# Real-Time Match Simulator

Proyecto de analitica deportiva en tiempo real para simular, procesar y visualizar eventos de un partido de futbol. La arquitectura usa Kafka para ingesta, PySpark Structured Streaming para procesamiento, Parquet como capa de persistencia, Streamlit como dashboard y un flujo GenAI con agentes, tools, RAG y LLM local para generar reportes narrativos.

## Objetivo

El sistema toma eventos StatsBomb desde `data/static/events.json`, los publica de forma gradual en Kafka, los transforma en metricas deportivas en streaming y genera visualizaciones e informes incrementales. El resultado permite seguir el partido por eventos, comparar rendimiento entre equipos y obtener textos automaticos por ventanas de minutos.

## Componentes

- Kafka: bus de eventos en el topic `match_events`.
- Kafka Producer: lee, filtra, normaliza y publica eventos StatsBomb.
- PySpark Streaming: consume Kafka, valida eventos, calcula metricas y persiste snapshots Parquet.
- GenAI reporting: cada 5 minutos de partido, detecta ventanas cerradas, usa tools de metricas/RAG y redacta textos con Ollama.
- Streamlit: muestra eventos, metricas, estado de ventanas, textos generados y descarga del PDF.

## Ejecucion con Docker

Requisitos:

- Docker y Docker Compose.
- `data/static/events.json` disponible.
- Ollama opcional si se quieren textos LLM reales; si no esta disponible, el proyecto genera fallbacks deterministas.

Para usar Ollama en local:

```bash
ollama pull llama3.2
ollama serve
```

Levantar todo:

```bash
docker compose up --build
```

Servicios levantados:

- Kafka en `localhost:9092`.
- Productor Kafka con broker interno `kafka:29092`.
- Streaming Spark con broker interno `kafka:29092`.
- Dashboard Streamlit en `http://localhost:8501`.

Parar:

```bash
docker compose down
```

El dashboard estara disponible en:

```text
http://localhost:8501
```

## Ejecucion con .venv

Requisitos:

- Python 3.11 recomendado.
- Java instalado para PySpark.
- Kafka levantado en `localhost:9092`.
- Ollama opcional para generacion LLM real.

Crear entorno:

```bash
python -m venv .venv
```

Activar en Windows PowerShell:

```powershell
.\.venv\Scripts\Activate.ps1
```

Instalar dependencias:

```bash
pip install -r docker/requirements-producer.txt
pip install -r docker/requirements-streaming.txt
pip install -r docker/requirements-dashboard.txt
```

Copiar variables de entorno:

```bash
cp .env.example .env
```

En Windows PowerShell, si no tienes `cp`:

```powershell
Copy-Item .env.example .env
```

Arrancar Kafka. La opcion mas simple es usar solo el broker de Docker:

```bash
docker compose up kafka
```

En terminales separadas, ejecutar:

```bash
python src/kafka_producer.py
```

```bash
python src/streaming_pipeline.py
```

```bash
streamlit run src/app.py
```

Abrir:

```text
http://localhost:8501
```

## Variables importantes

- `KAFKA_BROKER`: `localhost:9092` en host, `kafka:29092` en Docker.
- `KAFKA_TOPIC`: topic de eventos, por defecto `match_events`.
- `DATA_PATH`: ruta del JSON de eventos.
- `MATCH_ID`: identificador del partido.
- `EVENT_BATCH_SIZE`: eventos publicados por lote.
- `SNAPSHOT_INTERVAL_SECONDS`: frecuencia de escritura de snapshots.
- `REPORT_WINDOW_MINUTES`: tamano de ventana para reportes incrementales.
- `LLM_PROVIDER`: actualmente `ollama`.
- `OLLAMA_BASE_URL`: URL de Ollama.
- `OLLAMA_MODEL`: modelo LLM, por defecto `llama3.2`.
- `RAG_DOCS_PATH`: corpus documental local.
- `RAG_CHROMA_DIR`: persistencia Chroma para RAG.

## Salidas generadas

El pipeline escribe en `output/`:

- `output/processed/events.parquet`: eventos limpios acumulados.
- `output/aggregates/team_metrics.parquet`: metricas acumuladas por equipo.
- `output/report_windows/window_metrics.parquet`: metricas por ventanas de partido.
- `output/reports/incremental_segments.parquet`: textos generados por ventana.
- `output/reports/match_report.pdf`: informe PDF final.

Al arrancar, el streaming limpia `output/`, por lo que cada ejecucion empieza desde cero.

## Documentacion del proyecto

- `ARCHITECTURE.md`: explica los nodos principales de infraestructura: Kafka, producer, streaming pipeline, Streamlit y GenAI reporting.
- `FLOW.md`: describe el flujo de datos completo, desde el preprocesado del productor hasta la persistencia, generacion de reportes y visualizacion.
- `GENAI_REPORT.md`: detalla paso a paso como se generan textos cada 5 minutos con agentes, tools, RAG documental y LLM.

## Notas

Streamlit no llama al LLM ni consume Kafka directamente. La app solo lee artefactos ya generados. La generacion textual ocurre dentro del proceso de streaming cuando hay ventanas cerradas pendientes.

Si Ollama no esta disponible, el sistema continua funcionando y guarda un texto fallback para no bloquear los reportes.
