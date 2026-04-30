"""PySpark Structured Streaming pipeline for real-time sports analytics.

Pipeline architecture overview:
1. Read JSON events from Kafka topic `match_events`.
2. Parse raw Kafka bytes into structured event rows with a strict schema.
3. Validate and filter low-quality rows before downstream analytics.
4. Print intuitive per-batch summary and per-team metrics.
5. Persist cleaned events to `output/processed` and metrics to `output/aggregates`.
"""

import os
import sys
import socket
import logging
from pathlib import Path
from datetime import datetime

# Windows: suppress HADOOP_HOME warning by setting a dummy value
if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = "."
    os.environ["hadoop.home.dir"] = "."

import pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, col, count, from_json, lower, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

# Global stats counter
_stats = {"total_events": 0, "batches": 0}

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "match_events")

# Detect Spark version and select appropriate Kafka package
_spark_version = pyspark.__version__
_major_minor = ".".join(_spark_version.split(".")[:2])
_scala = "2.13" if _major_minor.startswith("4") else "2.12"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{_scala}:{_spark_version}"


def check_kafka_reachable(broker: str, timeout: int = 5) -> bool:
    """Check if Kafka broker is reachable via TCP before starting the stream."""
    host, port = broker.split(":")
    try:
        sock = socket.create_connection((host, int(port)), timeout=timeout)
        sock.close()
        print(f"[OK] Kafka broker reachable at {broker}")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError) as e:
        print(f"[ERROR] Cannot reach Kafka at {broker}: {e}")
        print("[HINT] Make sure Kafka is running. Start it with:")
        print("  docker compose up -d kafka")
        print("  OR run a local Kafka broker on localhost:9092")
        return False


def _prepare_output_dirs() -> tuple[Path, Path, Path, Path]:
    """Create streaming output and checkpoint directories."""
    output_root = Path("output")
    processed_dir = output_root / "processed"
    aggregates_dir = output_root / "aggregates"
    processed_checkpoint = output_root / "checkpoints" / "processed"
    aggregates_checkpoint = output_root / "checkpoints" / "aggregates"

    for directory in (processed_dir, aggregates_dir, processed_checkpoint, aggregates_checkpoint):
        directory.mkdir(parents=True, exist_ok=True)

    return processed_dir, aggregates_dir, processed_checkpoint, aggregates_checkpoint


def summarize_team_metrics(batch_df: DataFrame) -> DataFrame:
    """Build per-team metrics for a micro-batch."""
    event_type_lower = lower(col("event_type"))
    return (
        batch_df.groupBy("team")
        .agg(
            count(col("event_id")).alias("total_events"),
            count(when(event_type_lower == "goal", 1)).alias("goals"),
            count(when(event_type_lower == "shot", 1)).alias("shots"),
            count(when(event_type_lower == "pass", 1)).alias("passes"),
            count(when(event_type_lower.isin("foul committed", "foul won"), 1)).alias("fouls"),
            count(when(event_type_lower == "ball recovery", 1)).alias("recoveries"),
            avg(col("value")).alias("avg_value"),
        )
        .orderBy(col("team"))
    )


def process_events_batch(batch_df: DataFrame, batch_id: int, output_dir: Path) -> None:
    """Print batch summary and persist cleaned events to parquet."""
    event_count = batch_df.count()
    if event_count == 0:
        return

    _stats["total_events"] += event_count
    _stats["batches"] += 1

    ts = datetime.now().strftime("%H:%M:%S")
    print(f"\n  ✅ [{ts}] BATCH #{batch_id}: {event_count} nuevos eventos procesados")
    print(f"     📊 Total acumulado: {_stats['total_events']} eventos | {_stats['batches']} batches")

    batch_df.write.mode("append").parquet(str(output_dir))
    print(f"     💾 Guardado en: output/processed/")


def process_metrics_batch(batch_df: DataFrame, batch_id: int, output_dir: Path) -> None:
    """Print per-team metrics and persist them to parquet."""
    event_count = batch_df.count()
    if event_count == 0:
        return

    team_metrics = summarize_team_metrics(batch_df)
    ts = datetime.now().strftime("%H:%M:%S")

    print(f"\n  📈 [{ts}] MÉTRICAS POR EQUIPO - Batch {batch_id}:")
    print("     " + "─"*85)

    rows = team_metrics.collect()
    for idx, row in enumerate(rows, 1):
        avg_val = float(row["avg_value"]) if row["avg_value"] is not None else 0.0
        print(
            f"     [{idx}] {row['team']:18} │ "
            f"Eventos:{row['total_events']:3} │ Goles:{row['goals']:1} │ "
            f"Tiros:{row['shots']:2} │ Pases:{row['passes']:3} │ "
            f"Faltas:{row['fouls']:2} │ Recuper:{row['recoveries']:2} │ Avg:{avg_val:.2f}"
        )

    print("     " + "─"*85)
    team_metrics.write.mode("append").parquet(str(output_dir))
    print(f"     💾 Guardado en: output/aggregates/\n")


def build_spark_session() -> SparkSession:
    """Create and configure SparkSession for Kafka-based streaming."""
    return (
        SparkSession.builder.appName("SportAnalyticsPipeline")
        .master("local[2]")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # Hadoop/Windows fixes (compatible with Java 17+)
        .config("spark.driver.extraJavaOptions",
                "-Dhadoop.home.dir=. "
                "-Dio.netty.tryReflectionSetAccessible=true")
        .getOrCreate()
    )


def main() -> None:
    """Run the streaming job from Kafka ingestion to debug output."""
    print("\n" + "="*90)
    print("🚀 INICIANDO PIPELINE DE STREAMING EN TIEMPO REAL")
    print("="*90)
    print(f"   Broker Kafka: {KAFKA_BROKER}")
    print(f"   Tópico: {KAFKA_TOPIC}")
    print(f"   Hora inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*90 + "\n")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)
    
    processed_dir, aggregates_dir, processed_checkpoint, aggregates_checkpoint = _prepare_output_dirs()

    print(f"📁 Directorios de salida:")
    print(f"   • Eventos: {processed_dir}")
    print(f"   • Métricas: {aggregates_dir}\n")

    print("🔌 Verificando conexión a Kafka...")
    if not check_kafka_reachable(KAFKA_BROKER):
        sys.exit(1)
    print("✅ Conectado a Kafka.\n")

    # Exact schema expected from kafka_producer.py flattened events.
    event_schema = StructType(
        [
            StructField("event_id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("match_id", StringType(), True),
            StructField("team", StringType(), True),
            StructField("player", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("zone", StringType(), True),
            StructField("minute", IntegerType(), True),
            StructField("value", IntegerType(), True),
            StructField("period", IntegerType(), True),
        ]
    )

    # Topic 1: raw football events.
    raw_events_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", "match_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse Kafka binary payload into structured event rows.
    parsed_events = (
        raw_events_kafka.selectExpr("CAST(value AS STRING) AS json_payload")
        .select(from_json(col("json_payload"), event_schema).alias("event"))
        .select("event.*")
    )

    # VALIDATION: keep rows with event_id and at least one known identity field.
    cleaned_events = parsed_events.filter(
        col("event_id").isNotNull()
        & ~((col("team") == "Unknown") & (col("player") == "Unknown"))
    )

    # ================================================================
    # CONSOLE OUTPUT + PARQUET SINK - Cleaned events
    # ================================================================
    print("⚡ Iniciando queries de streaming...\n")
    print("  [1/2] Procesando eventos (cada 5 seg)...")
    events_query = (
        cleaned_events.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: process_events_batch(batch_df, batch_id, processed_dir))
        .trigger(processingTime="5 seconds")
        .queryName("EventsDisplay")
        .option("checkpointLocation", str(processed_checkpoint))
        .start()
    )
    print("       ✅ Listo\n")

    # ================================================================
    # CONSOLE OUTPUT + PARQUET SINK - Team metrics
    # ================================================================
    print("  [2/2] Agregando métricas por equipo (cada 10 seg)...")
    metrics_query = (
        cleaned_events.writeStream.outputMode("append")
        .foreachBatch(lambda batch_df, batch_id: process_metrics_batch(batch_df, batch_id, aggregates_dir))
        .trigger(processingTime="10 seconds")
        .queryName("MetricsDisplay")
        .option("checkpointLocation", str(aggregates_checkpoint))
        .start()
    )
    print("       ✅ Listo\n")

    print("="*90)
    print("🟢 ESPERANDO EVENTOS DE KAFKA...")
    print("   Streamlit disponible en: http://localhost:8501")
    print("   Presiona Ctrl+C para detener")
    print("="*90 + "\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n" + "="*90)
        print("⏹️  DETENIENDO PIPELINE")
        print(f"   Total eventos procesados: {_stats['total_events']}")
        print(f"   Total batches: {_stats['batches']}")
        print("="*90)
        spark.stop()


if __name__ == "__main__":
    main()
