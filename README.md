# Final Project - Real-Time Sports Analytics

This project scaffolds a university final project for real-time football analytics using:

- Apache Kafka for event ingestion
- PySpark Structured Streaming for stream processing
- Streamlit for live dashboards
- LangGraph and RAG components for automated analysis and reporting

## Quick Start

1. Install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy environment template and update values if needed:

   ```bash
   cp .env.example .env
   ```

3. Run Kafka producer:

   ```bash
   python src/kafka_producer.py
   ```

4. Run Spark streaming pipeline:

   ```bash
   python src/streaming_pipeline.py
   ```

5. Launch dashboard:

   ```bash
   streamlit run src/app.py
   ```

## Notes

- Place StatsBomb events at `data/static/events.json`.
- Placeholder modules in `src/` are ready for incremental implementation.

## Running with Docker

1. Make sure `data/static/events.json` exists.
2. Build and start the Kafka broker, producer, and streaming job:

   ```bash
   docker compose up --build
   ```

3. The compose file starts:
   - Kafka broker in KRaft mode
   - `src/kafka_producer.py` with `KAFKA_BROKER=kafka:29092`
   - `src/streaming_pipeline.py` with `KAFKA_BROKER=kafka:29092`
   - the Streamlit dashboard on `http://localhost:8501`

   Docker builds use service-specific images:
   - producer: only Kafka producer dependencies (`confluent-kafka`, `python-dotenv`)
   - streaming: Java 17, pinned PySpark 3.5.1, PDF generation, LangGraph, ChromaDB and embedding dependencies
   - dashboard: only Streamlit plus parquet readers

   To rebuild only one service after dependency changes:

   ```bash
   docker compose build producer
   docker compose up producer
   ```

   The streaming service processes Kafka microbatches as soon as Spark receives
   data. It keeps a live internal event store in `output/live/events` and
   publishes the dashboard parquet snapshots every 30 seconds by default. Change
   that interval with `SNAPSHOT_INTERVAL_SECONDS`. It also generates the
   incremental report segments and `output/reports/match_report.pdf`; Streamlit
   only reads those artifacts.

   `REPORT_WINDOW_MINUTES=2` controls the match-minute window used for each
   incremental report segment. ChromaDB and sentence-transformer embeddings live
   in the streaming image, so that image can be large; the dashboard image stays
   lightweight because it never imports LangGraph, ChromaDB or embeddings.

4. The streaming job stores consolidated parquet output in:
   - `output/processed/events.parquet` for accumulated cleaned events
   - `output/aggregates/team_metrics.parquet` for cumulative per-team metric snapshots

5. The dashboard report export generates only `match_report.pdf`. The PDF is built through:
   - Tool 1: match metric queries against Spark parquet outputs
   - Tool 2: semantic RAG retrieval from `data/docs/rag_corpus` using local embeddings and persistent Chroma in `output/chroma_db`
   - a LangGraph workflow that coordinates metrics, RAG context, and final report drafting
   - a real local LLM call in the drafting node through Ollama, using `llama3.2` by default
   - incremental report segments every `REPORT_WINDOW_MINUTES` match minutes, stored in `output/reports/incremental_segments.parquet`

   Before generating the PDF, make sure Ollama is running locally and the model is available:

   ```bash
   ollama pull llama3.2
   ollama serve
   ```

   The incremental report flow is:
   - Spark writes consolidated events and per-window metrics to `output/report_windows/window_metrics.parquet`
   - Streamlit detects closed match-minute windows, one at a time, and launches the agents
   - the agents combine window events, calculated metrics, cumulative context, and RAG evidence
   - each generated block is shown in the `Informe incremental` tab with its minute range and traceability
   - the PDF export includes all generated blocks plus a window-activity chart and window metrics table

6. If you want to run the scripts from the host instead of Docker, use `localhost:9092` as the broker.

7. To stop the stack:

   ```bash
   docker compose down
   ```

Notes:
- Kafka is exposed on `localhost:9092` for host-based clients.
- Containers inside the compose network should use `kafka:29092`.
- The streaming job downloads the Kafka connector JAR on first run and caches it in the container.
