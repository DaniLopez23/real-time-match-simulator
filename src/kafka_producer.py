"""Kafka producer for streaming StatsBomb match events in near real-time.

This script loads events from a StatsBomb JSON file, maps each event to a flat schema,
and publishes each mapped event to Kafka topic `match_events` with a random delay.
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
import os


# Load environment variables from a .env file (if present).
load_dotenv()

# Defaults are provided so the script runs even without a configured .env file.
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
DATA_PATH = Path(os.getenv("DATA_PATH", "data/static/events.json"))
MATCH_ID = os.getenv("MATCH_ID", "3946454")
TOPIC_NAME = "match_events"


def wait_for_kafka(broker: str, max_retries: int = 20, wait_seconds: int = 5) -> bool:
    """Wait until Kafka broker is ready before starting to produce events."""
    admin = AdminClient({"bootstrap.servers": broker})
    for attempt in range(1, max_retries + 1):
        try:
            admin.list_topics(timeout=5)
            print(f"[KAFKA] Connected successfully to {broker}")
            return True
        except Exception:
            print(f"[WAIT] Waiting for Kafka to be ready... attempt {attempt}/{max_retries}")
            time.sleep(wait_seconds)
    raise RuntimeError(
        f"[ERROR] Could not connect to Kafka at {broker} after {max_retries} attempts"
    )


def delivery_report(err, msg) -> None:
    """Callback for Kafka message delivery results."""
    if err is not None:
        print(f"[ERROR] Delivery failed for message to {msg.topic()}: {err}")


def derive_zone(location: Optional[List[float]]) -> str:
    """Map StatsBomb location [x, y] to a 3x3 pitch zone label.

    The pitch is assumed to be 120x80 and split into 3 equal vertical and horizontal bands.
    """
    if not location or len(location) < 2:
        return "unknown"

    x_coord, y_coord = location[0], location[1]

    # Guard against malformed coordinates.
    if x_coord is None or y_coord is None:
        return "unknown"

    # Determine horizontal third by x-axis.
    if x_coord < 40:
        x_band = "defensive"
    elif x_coord < 80:
        x_band = "middle"
    else:
        x_band = "attacking"

    # Determine vertical third by y-axis.
    if y_coord < (80 / 3):
        y_band = "left"
    elif y_coord < (160 / 3):
        y_band = "center"
    else:
        y_band = "right"

    return f"{x_band}_{y_band}"


def compute_event_value(event_type: str, event: Dict[str, Any]) -> int:
    """Assign a numeric value by event type and outcome rules."""
    if event_type == "Shot":
        shot_data = event.get("shot", {})
        outcome_name = (shot_data.get("outcome") or {}).get("name")
        if outcome_name == "Goal":
            return 3
        return 2

    if event_type in ["Foul Committed", "Foul Won"]:
        return 0

    return 1


def map_event(event: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten a StatsBomb event into the project analytics schema."""
    team_name = (event.get("team") or {}).get("name", "Unknown")
    player_name = (event.get("player") or {}).get("name", "Unknown")
    event_type = (event.get("type") or {}).get("name", "Unknown")

    mapped = {
        "event_id": event.get("id"),
        "timestamp": event.get("timestamp"),
        "match_id": MATCH_ID,
        "team": team_name,
        "player": player_name,
        "event_type": event_type,
        "zone": derive_zone(event.get("location")),
        "minute": int(event.get("minute", 0)),
        "value": compute_event_value(event_type, event),
        "period": int(event.get("period", 0)),
    }
    return mapped


def load_and_sort_events(file_path: Path) -> List[Dict[str, Any]]:
    """Load StatsBomb events and sort by `index` to preserve match chronology."""
    with file_path.open("r", encoding="utf-8") as infile:
        events = json.load(infile)

    if not isinstance(events, list):
        raise ValueError("Expected the events file to contain a JSON list of event objects.")

    return sorted(events, key=lambda item: item.get("index", float("inf")))


def stream_events() -> None:
    """Read events, map each one, and publish to Kafka with a random delay."""
    try:
        events = load_and_sort_events(DATA_PATH)
    except FileNotFoundError:
        print(f"[ERROR] Events file not found at: {DATA_PATH}")
        return
    except json.JSONDecodeError as exc:
        print(f"[ERROR] Invalid JSON in file {DATA_PATH}: {exc}")
        return
    except Exception as exc:  # noqa: BLE001
        print(f"[ERROR] Failed to load events: {exc}")
        return

    # Wait for Kafka to become available before creating the producer connection.
    wait_for_kafka(KAFKA_BROKER)

    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    print(f"Loaded {len(events)} events from {DATA_PATH}. Publishing to topic '{TOPIC_NAME}'...")

    for raw_event in events:
        mapped_event = map_event(raw_event)

        # Serialize mapped event as JSON and send it to Kafka.
        producer.produce(TOPIC_NAME, value=json.dumps(mapped_event), callback=delivery_report)
        producer.poll(0)

        display_time = datetime.now().strftime("%H:%M:%S")
        event_index = raw_event.get("index", "N/A")
        print(
            f"[{display_time}] Published event #{event_index}: "
            f"{mapped_event['event_type']} by {mapped_event['player']} "
            f"({mapped_event['team']}) | min {mapped_event['minute']} | zone {mapped_event['zone']}"
        )

        # Simulate real-time ingestion pace.
        sleep_seconds = random.uniform(5, 10)
        time.sleep(sleep_seconds)

    # Ensure any buffered messages are delivered before exit.
    producer.flush()
    print("Finished publishing all events.")


if __name__ == "__main__":
    stream_events()
