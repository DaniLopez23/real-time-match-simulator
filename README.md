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

4. The streaming job stores parquet output in:
   - `output/processed` for cleaned events
   - `output/aggregates` for per-team metrics

5. If you want to run the scripts from the host instead of Docker, use `localhost:9092` as the broker.

6. To stop the stack:

   ```bash
   docker compose down
   ```

Notes:
- Kafka is exposed on `localhost:9092` for host-based clients.
- Containers inside the compose network should use `kafka:29092`.
- The streaming job downloads the Kafka connector JAR on first run and caches it in the container.
