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
   - streaming: Java 17 and pinned PySpark 3.5.1 for Kafka/Spark/parquet processing
   - dashboard: only Streamlit plus parquet readers

   To rebuild only one service after dependency changes:

   ```bash
   docker compose build producer
   docker compose up producer
   ```

   The producer filters the StatsBomb feed to Pressure, Duel, Interception,
   Block, Clearance, Pass, Shot, Carry and Dribble events. It publishes them in
   bursts of `EVENT_BATCH_SIZE=5` events, then waits a random 5-10 seconds
   before the next burst.

   The streaming service processes Kafka microbatches as soon as Spark receives
   data. It keeps the live event store in memory and publishes a single
   consolidated events parquet plus metric parquet snapshots every 30 seconds by
   default. Change that interval with `SNAPSHOT_INTERVAL_SECONDS`. Spark does not
   call LLM/RAG/PDF code inside microbatches.

   `REPORT_WINDOW_MINUTES=5` controls the match-minute window used for each
   incremental report segment. Spark also calculates offensive/defensive
   indexes, pass-success percentage and duel-success percentage for each team and
   match-minute window. The streaming and dashboard images stay lightweight and
   do not include LLM/RAG/PDF generation services.

4. The streaming job stores consolidated parquet output in:
   - `output/processed/events.parquet` for accumulated cleaned events
   - `output/aggregates/team_metrics.parquet` for cumulative per-team metric snapshots

5. The dashboard report export only downloads `match_report.pdf` when it already exists. The PDF is built from existing artifacts:
   - Spark parquet outputs for events, team metrics and window metrics
   - incremental narrative segments every `REPORT_WINDOW_MINUTES` match minutes, stored in `output/reports/incremental_segments.parquet`

   The streaming pipeline materializes pending narrative segments when it detects closed
   match-minute windows and writes the RAG/LLM stages to the streaming console logs.
   Make sure Ollama is running locally and the model is available:

   ```bash
   ollama pull llama3.2
   ollama serve
   ```

   The incremental report flow is:
   - Spark writes consolidated events and per-window metrics to `output/report_windows/window_metrics.parquet`
   - the streaming process detects closed 5-minute match windows and writes a textual RAG document to `output/reports/incremental_rag.parquet`
   - the graph/tools/RAG/LLM flow generates one natural-language summary for that window
   - Streamlit shows which windows are open, pending or already narrated
   - each generated block is shown in the `Informe incremental` tab with its minute range, RAG/LLM stages and traceability
   - the streaming process refreshes `output/reports/match_report.pdf` from the latest metrics and generated segment texts
   - Streamlit does not call LLM/RAG or regenerate PDFs on demand
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
