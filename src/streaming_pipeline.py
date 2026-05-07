"""PySpark Structured Streaming pipeline for real-time sports analytics.

Pipeline architecture overview:
1. Read JSON events from Kafka topic `match_events`.
2. Parse raw Kafka bytes into structured event rows with a strict schema.
3. Validate and filter low-quality rows before downstream analytics.
4. Every 30 seconds, persist one consolidated parquet file with all cleaned events.
5. Recalculate cumulative per-team metrics and append one snapshot row per team.
"""

import logging
import os
import shutil
import socket
import sys
import uuid
from datetime import datetime
from pathlib import Path

# Windows: suppress HADOOP_HOME warning by setting a dummy value.
if sys.platform == "win32" and "HADOOP_HOME" not in os.environ:
    os.environ["HADOOP_HOME"] = "."
    os.environ["hadoop.home.dir"] = "."

import pyspark
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import avg, col, count, floor, from_json, lit, lower, when
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

_stats = {"total_events": 0, "batches_with_events": 0}

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "match_events")
REPORT_WINDOW_MINUTES = int(os.getenv("REPORT_WINDOW_MINUTES", "2"))
SNAPSHOT_INTERVAL_SECONDS = int(os.getenv("SNAPSHOT_INTERVAL_SECONDS", "30"))
REPORT_GENERATION_ENABLED = os.getenv("REPORT_GENERATION_ENABLED", "true").strip().lower() in {"1", "true", "yes"}
_last_snapshot_epoch = 0.0

_spark_version = pyspark.__version__
_major_minor = ".".join(_spark_version.split(".")[:2])
_scala = "2.13" if _major_minor.startswith("4") else "2.12"
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_{_scala}:{_spark_version}"


def check_kafka_reachable(broker: str, timeout: int = 5) -> bool:
    """Check whether the Kafka broker is reachable via TCP."""
    host, port = broker.split(":")
    try:
        sock = socket.create_connection((host, int(port)), timeout=timeout)
        sock.close()
        print(f"[OK] Kafka broker reachable at {broker}")
        return True
    except (socket.timeout, ConnectionRefusedError, OSError) as exc:
        print(f"[ERROR] Cannot reach Kafka at {broker}: {exc}")
        print("[HINT] Make sure Kafka is running:")
        print("  docker compose up -d kafka")
        print("  or run a local Kafka broker on localhost:9092")
        return False


def _prepare_output_paths() -> tuple[Path, Path, Path, Path, Path]:
    """Create output directories and return live staging plus snapshot paths."""
    output_root = Path("output")
    staging_dir = output_root / "live" / "events"
    processed_dir = output_root / "processed"
    aggregates_dir = output_root / "aggregates"
    report_windows_dir = output_root / "report_windows"
    checkpoint_dir = output_root / "checkpoints" / "snapshots"

    for directory in (staging_dir, processed_dir, aggregates_dir, report_windows_dir, checkpoint_dir):
        directory.mkdir(parents=True, exist_ok=True)

    return (
        staging_dir,
        processed_dir / "events.parquet",
        aggregates_dir / "team_metrics.parquet",
        report_windows_dir / "window_metrics.parquet",
        checkpoint_dir,
    )


def summarize_team_metrics(events_df: DataFrame) -> DataFrame:
    """Build cumulative per-team metrics for the provided events dataframe."""
    event_type_lower = lower(col("event_type"))
    return (
        events_df.groupBy("team")
        .agg(
            count(col("event_id")).alias("total_events"),
            count(when((event_type_lower == "goal") | ((event_type_lower == "shot") & (col("value") == 3)), 1)).alias("goals"),
            count(when(event_type_lower == "shot", 1)).alias("shots"),
            count(when(event_type_lower == "pass", 1)).alias("passes"),
            count(when(event_type_lower.isin("foul committed", "foul won"), 1)).alias("fouls"),
            count(when(event_type_lower == "ball recovery", 1)).alias("recoveries"),
            avg(col("value")).alias("avg_value"),
        )
        .orderBy(col("team"))
    )


def summarize_window_metrics(events_df: DataFrame, window_minutes: int) -> DataFrame:
    """Build per-team metrics grouped by match-minute windows."""
    event_type_lower = lower(col("event_type"))
    windowed = (
        events_df.withColumn("window_start_minute", floor(col("minute") / window_minutes) * window_minutes)
        .withColumn("window_end_minute", col("window_start_minute") + lit(window_minutes))
    )
    return (
        windowed.groupBy("match_id", "window_start_minute", "window_end_minute", "team")
        .agg(
            count(col("event_id")).alias("events"),
            count(when((event_type_lower == "goal") | ((event_type_lower == "shot") & (col("value") == 3)), 1)).alias("goals"),
            count(when(event_type_lower == "shot", 1)).alias("shots"),
            count(when(event_type_lower == "pass", 1)).alias("passes"),
            count(when(event_type_lower.isin("foul committed", "foul won"), 1)).alias("fouls"),
            count(when(event_type_lower == "ball recovery", 1)).alias("recoveries"),
            avg(col("value")).alias("avg_value"),
        )
        .orderBy("window_start_minute", "team")
    )


def _read_parquet_if_exists(spark: SparkSession, file_path: Path) -> DataFrame | None:
    """Read a consolidated parquet file when it already exists."""
    if file_path.exists() and file_path.is_file():
        return spark.read.parquet(str(file_path))
    if file_path.exists() and file_path.is_dir() and any(file_path.glob("*.parquet")):
        return spark.read.parquet(str(file_path))
    return None


def _write_single_parquet_file(df: DataFrame, file_path: Path) -> None:
    """Write a dataframe as one physical parquet file, replacing the previous file."""
    tmp_dir = file_path.parent / f".{file_path.stem}_tmp_{uuid.uuid4().hex}"
    df.coalesce(1).write.mode("overwrite").parquet(str(tmp_dir))

    part_files = sorted(tmp_dir.glob("part-*.parquet"))
    if not part_files:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise RuntimeError(f"No parquet part file produced for {file_path}")

    file_path.parent.mkdir(parents=True, exist_ok=True)
    if file_path.is_dir():
        shutil.rmtree(file_path)
    elif file_path.exists():
        file_path.unlink()
    shutil.move(str(part_files[0]), str(file_path))
    shutil.rmtree(tmp_dir, ignore_errors=True)


def _write_live_events(batch_df: DataFrame, staging_dir: Path, batch_id: int) -> int:
    """Append one processed microbatch to the internal live event store."""
    live_events = (
        batch_df.withColumn("ingested_at", lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        .withColumn("source_batch_id", lit(batch_id))
        .persist()
    )
    try:
        event_count = live_events.count()
        if event_count == 0:
            return 0
        live_events.write.mode("append").parquet(str(staging_dir))
        return event_count
    finally:
        live_events.unpersist()


def _snapshot_due(now_epoch: float) -> bool:
    return now_epoch - _last_snapshot_epoch >= SNAPSHOT_INTERVAL_SECONDS


def _generate_report_artifacts() -> None:
    """Generate agent/RAG report artifacts from the streaming container."""
    if not REPORT_GENERATION_ENABLED:
        return

    try:
        from incremental_report import generate_missing_report_segments
        from report_generator import generate_pdf_report
        from settings import REPORT_PDF_FILE

        _, generated = generate_missing_report_segments()
        if not generated and REPORT_PDF_FILE.exists():
            return

        pdf_bytes, workflow_output = generate_pdf_report(include_incremental=True)
        REPORT_PDF_FILE.parent.mkdir(parents=True, exist_ok=True)
        REPORT_PDF_FILE.write_bytes(pdf_bytes)
        llm_model = workflow_output.get("llm_model", "no disponible")
        print(
            f"     Report artifacts: {len(generated)} new segments, "
            f"PDF updated at {REPORT_PDF_FILE} using {llm_model}"
        )
    except Exception as exc:  # noqa: BLE001
        print(f"     [WARN] Report artifact generation skipped: {exc}")


def process_live_batch(
    batch_df: DataFrame,
    batch_id: int,
    staging_dir: Path,
    events_file: Path,
    metrics_file: Path,
    window_metrics_file: Path,
) -> None:
    """Process Kafka microbatches immediately and publish snapshots on an interval."""
    global _last_snapshot_epoch

    spark = batch_df.sparkSession
    now_epoch = datetime.now().timestamp()
    snapshot_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    new_event_count = _write_live_events(batch_df, staging_dir, batch_id)
    if new_event_count > 0:
        _stats["batches_with_events"] += 1
        print(f"[{snapshot_time}] Batch #{batch_id}: processed {new_event_count} new events")

    staged_events = _read_parquet_if_exists(spark, staging_dir)
    if staged_events is None:
        print(f"[{snapshot_time}] Batch #{batch_id}: no events yet")
        return

    if not _snapshot_due(now_epoch):
        return

    all_events = staged_events.dropDuplicates(["event_id"])
    total_events = all_events.count()
    snapshotted_events = (
        all_events.drop("ingested_at", "source_batch_id")
        .withColumn("snapshot_time", lit(snapshot_time))
        .withColumn("batch_id", lit(batch_id))
    )
    _write_single_parquet_file(snapshotted_events, events_file)

    team_metrics = (
        summarize_team_metrics(all_events)
        .withColumn("snapshot_time", lit(snapshot_time))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("snapshot_event_count", lit(total_events))
    )

    existing_metrics = _read_parquet_if_exists(spark, metrics_file)
    metrics_history = (
        existing_metrics.unionByName(team_metrics, allowMissingColumns=True)
        if existing_metrics is not None
        else team_metrics
    )
    _write_single_parquet_file(metrics_history, metrics_file)

    window_metrics = (
        summarize_window_metrics(all_events, REPORT_WINDOW_MINUTES)
        .withColumn("snapshot_time", lit(snapshot_time))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("window_minutes", lit(REPORT_WINDOW_MINUTES))
    )
    _write_single_parquet_file(window_metrics, window_metrics_file)
    _generate_report_artifacts()

    _stats["total_events"] = total_events
    _last_snapshot_epoch = now_epoch

    print(f"\n  [{snapshot_time}] Snapshot #{batch_id}: published")
    print(f"     Accumulated events: {total_events}")
    print(f"     Events file: {events_file}")
    print(f"     Metrics file: {metrics_file}")
    print(f"     Window metrics file: {window_metrics_file}")
    print("     " + "-" * 85)

    for idx, row in enumerate(team_metrics.collect(), 1):
        avg_val = float(row["avg_value"]) if row["avg_value"] is not None else 0.0
        print(
            f"     [{idx}] {row['team']:18} | "
            f"Events:{row['total_events']:3} | Goals:{row['goals']:1} | "
            f"Shots:{row['shots']:2} | Passes:{row['passes']:3} | "
            f"Fouls:{row['fouls']:2} | Recoveries:{row['recoveries']:2} | Avg:{avg_val:.2f}"
        )

    print("     " + "-" * 85)


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
        .config(
            "spark.driver.extraJavaOptions",
            "-Dhadoop.home.dir=. -Dio.netty.tryReflectionSetAccessible=true",
        )
        .getOrCreate()
    )


def main() -> None:
    """Run the streaming job from Kafka ingestion to consolidated parquet outputs."""
    global _last_snapshot_epoch

    print("\n" + "=" * 90)
    print("STARTING REAL-TIME STREAMING PIPELINE")
    print("=" * 90)
    print(f"   Kafka broker: {KAFKA_BROKER}")
    print(f"   Topic: {KAFKA_TOPIC}")
    print(f"   Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 90 + "\n")

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    logging.getLogger("pyspark").setLevel(logging.ERROR)

    staging_dir, events_file, metrics_file, window_metrics_file, checkpoint_dir = _prepare_output_paths()
    _last_snapshot_epoch = datetime.now().timestamp()

    print("Output files:")
    print(f"   - Events: {events_file}")
    print(f"   - Metrics: {metrics_file}")
    print(f"   - Window metrics ({REPORT_WINDOW_MINUTES} min): {window_metrics_file}\n")

    print("Checking Kafka connection...")
    if not check_kafka_reachable(KAFKA_BROKER):
        sys.exit(1)
    print("Connected to Kafka.\n")

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

    raw_events_kafka = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_events = (
        raw_events_kafka.selectExpr("CAST(value AS STRING) AS json_payload")
        .select(from_json(col("json_payload"), event_schema).alias("event"))
        .select("event.*")
    )

    cleaned_events = parsed_events.filter(
        col("event_id").isNotNull()
        & ~((col("team") == "Unknown") & (col("player") == "Unknown"))
    )

    print("Starting consolidated snapshot query...")
    (
        cleaned_events.writeStream.outputMode("append")
        .foreachBatch(
            lambda batch_df, batch_id: process_live_batch(
                batch_df,
                batch_id,
                staging_dir,
                events_file,
                metrics_file,
                window_metrics_file,
            )
        )
        .queryName("ConsolidatedSnapshots")
        .option("checkpointLocation", str(checkpoint_dir))
        .start()
    )
    print("Ready.\n")

    print("=" * 90)
    print("WAITING FOR KAFKA EVENTS")
    print("Streamlit available at: http://localhost:8501")
    print("Press Ctrl+C to stop")
    print("=" * 90 + "\n")

    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("\n" + "=" * 90)
        print("STOPPING PIPELINE")
        print(f"Total events processed: {_stats['total_events']}")
        print(f"Batches with data: {_stats['batches_with_events']}")
        print("=" * 90)
        spark.stop()


if __name__ == "__main__":
    main()
