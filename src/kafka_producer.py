"""Kafka producer for streaming StatsBomb match events in near real-time.

This script loads events from a StatsBomb JSON file, maps each event to a flat schema,
and publishes mapped events to Kafka topic `match_events` in small simulated bursts.
"""

import json
import random
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

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
SCHEMA_VERSION = "1.0"
EVENT_BATCH_SIZE = max(1, int(os.getenv("EVENT_BATCH_SIZE", "5")))
MIN_SLEEP_SECONDS = float(os.getenv("MIN_SLEEP_SECONDS", "5"))
MAX_SLEEP_SECONDS = float(os.getenv("MAX_SLEEP_SECONDS", "10"))
SLEEP_RANGE_SECONDS = tuple(sorted((MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS)))
PRESSURE_RECOVERY_WINDOW_SECONDS = 5

PUBLISHABLE_EVENT_TYPES = {
    "Pressure",
    "Duel",
    "Interception",
    "Block",
    "Clearance",
    "Pass",
    "Shot",
    "Carry",
    "Dribble",
}

SHOT_OUTCOMES = {
    "Goal": "GOAL",
    "Saved": "ON_TARGET",
    "Saved to Post": "ON_TARGET",
    "Blocked": "BLOCKED",
    "Off T": "OFF_TARGET",
    "Wayward": "OFF_TARGET",
    "Post": "OFF_TARGET",
}


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


def get_event_type(event: Dict[str, Any]) -> str:
    """Return the StatsBomb event type name."""
    return (event.get("type") or {}).get("name", "Unknown")


def get_team_name(event: Dict[str, Any]) -> str:
    """Return the event team name."""
    return (event.get("team") or {}).get("name", "Unknown")


def get_nested_outcome_name(event: Dict[str, Any], event_type: str) -> Optional[str]:
    """Extract the native StatsBomb outcome name for event types that expose one."""
    detail_key = event_type.lower()
    detail = event.get(detail_key) or {}
    outcome = detail.get("outcome") or {}
    return outcome.get("name")


def safe_int(value: Any, default: int = 0) -> int:
    """Convert StatsBomb numeric fields defensively."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def event_clock_seconds(event: Dict[str, Any]) -> int:
    """Convert StatsBomb minute/second fields into match-clock seconds."""
    return safe_int(event.get("minute")) * 60 + safe_int(event.get("second"))


def build_event_lookup(events: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """Index source events by id so related event context can be inspected quickly."""
    return {event["id"]: event for event in events if event.get("id")}


def has_related_event_type(
    event: Dict[str, Any],
    event_lookup: Dict[str, Dict[str, Any]],
    related_types: set[str],
) -> bool:
    """Check whether this event is explicitly related to one of the provided types."""
    for related_id in event.get("related_events") or []:
        related_event = event_lookup.get(related_id)
        if related_event and get_event_type(related_event) in related_types:
            return True
    return False


def build_pressure_success_ids(events: List[Dict[str, Any]]) -> set[str]:
    """Infer successful pressures from a same-team recovery shortly afterwards."""
    pressure_success_ids: set[str] = set()
    event_lookup = build_event_lookup(events)
    recoveries = [event for event in events if get_event_type(event) == "Ball Recovery"]

    for pressure in events:
        if get_event_type(pressure) != "Pressure":
            continue

        pressure_id = pressure.get("id")
        pressure_team = get_team_name(pressure)
        pressure_time = event_clock_seconds(pressure)

        for recovery in recoveries:
            recovery_time = event_clock_seconds(recovery)
            if recovery_time < pressure_time:
                continue
            if recovery_time - pressure_time > PRESSURE_RECOVERY_WINDOW_SECONDS:
                continue
            if get_team_name(recovery) != pressure_team:
                continue

            pressure_success_ids.add(pressure_id)
            break

        if pressure_id not in pressure_success_ids:
            for related_id in pressure.get("related_events") or []:
                related_event = event_lookup.get(related_id)
                if (
                    related_event
                    and get_event_type(related_event) == "Ball Recovery"
                    and get_team_name(related_event) == pressure_team
                ):
                    pressure_success_ids.add(pressure_id)
                    break

    return pressure_success_ids


def map_shot_outcome(event: Dict[str, Any]) -> str:
    """Normalize shot outcomes into the requested shot categories."""
    outcome_name = get_nested_outcome_name(event, "Shot")
    return SHOT_OUTCOMES.get(outcome_name, "UNKNOWN")


def map_binary_outcome(
    event: Dict[str, Any],
    event_lookup: Dict[str, Dict[str, Any]],
    pressure_success_ids: set[str],
) -> str:
    """Normalize non-shot events into SUCCESS or FAIL."""
    event_type = get_event_type(event)
    outcome_name = get_nested_outcome_name(event, event_type)
    outcome_normalized = (outcome_name or "").strip().lower()

    if event_type == "Pass":
        return "FAIL" if outcome_name else "SUCCESS"

    if event_type == "Dribble":
        return "SUCCESS" if outcome_normalized == "complete" else "FAIL"

    if event_type == "Interception":
        if not outcome_name or outcome_normalized.startswith("success") or outcome_normalized == "won":
            return "SUCCESS"
        return "FAIL"

    if event_type == "Duel":
        if outcome_normalized.startswith("success") or outcome_normalized == "won":
            return "SUCCESS"
        return "FAIL"

    if event_type == "Pressure":
        return "SUCCESS" if event.get("id") in pressure_success_ids else "FAIL"

    if event_type == "Carry":
        return "FAIL" if has_related_event_type(event, event_lookup, {"Dispossessed", "Miscontrol"}) else "SUCCESS"

    return "SUCCESS"


def map_outcome(
    event: Dict[str, Any],
    event_lookup: Dict[str, Dict[str, Any]],
    pressure_success_ids: set[str],
) -> str:
    """Map each publishable event to the requested normalized outcome."""
    if get_event_type(event) == "Shot":
        return map_shot_outcome(event)
    return map_binary_outcome(event, event_lookup, pressure_success_ids)


def map_event(
    event: Dict[str, Any],
    event_lookup: Dict[str, Dict[str, Any]],
    pressure_success_ids: set[str],
) -> Dict[str, Any]:
    """Flatten a StatsBomb event into the project analytics schema."""
    team_name = get_team_name(event)
    player_name = (event.get("player") or {}).get("name", "Unknown")
    event_type = get_event_type(event)
    outcome = map_outcome(event, event_lookup, pressure_success_ids)

    mapped = {
        "schema_version": SCHEMA_VERSION,
        "event_id": event.get("id"),
        "timestamp": event.get("timestamp"),
        "match_id": MATCH_ID,
        "team": team_name,
        "player": player_name,
        "event_type": event_type,
        "zone": derive_zone(event.get("location")),
        "minute": safe_int(event.get("minute")),
        "second": safe_int(event.get("second")),
        "outcome": outcome,
        "period": safe_int(event.get("period")),
    }
    return mapped


def load_and_sort_events(file_path: Path) -> List[Dict[str, Any]]:
    """Load StatsBomb events and sort by `index` to preserve match chronology."""
    with file_path.open("r", encoding="utf-8") as infile:
        events = json.load(infile)

    if not isinstance(events, list):
        raise ValueError("Expected the events file to contain a JSON list of event objects.")

    return sorted(events, key=lambda item: item.get("index", float("inf")))


def is_publishable_event(event: Dict[str, Any]) -> bool:
    """Return whether an event type should be sent to Kafka."""
    return get_event_type(event) in PUBLISHABLE_EVENT_TYPES


def batched(events: List[Dict[str, Any]], batch_size: int) -> Iterable[List[Dict[str, Any]]]:
    """Yield fixed-size batches while preserving match chronology."""
    for start in range(0, len(events), batch_size):
        yield events[start : start + batch_size]


def stream_events() -> None:
    """Read events, map publishable events, and send them to Kafka in batches."""
    try:
        source_events = load_and_sort_events(DATA_PATH)
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
    event_lookup = build_event_lookup(source_events)
    pressure_success_ids = build_pressure_success_ids(source_events)
    events = [event for event in source_events if is_publishable_event(event)]

    print(
        f"Loaded {len(source_events)} events from {DATA_PATH}. "
        f"Publishing {len(events)} filtered events to topic '{TOPIC_NAME}' "
        f"in batches of {EVENT_BATCH_SIZE}..."
    )

    for batch_number, raw_batch in enumerate(batched(events, EVENT_BATCH_SIZE), 1):
        display_time = datetime.now().strftime("%H:%M:%S")
        first_index = raw_batch[0].get("index", "N/A")
        last_index = raw_batch[-1].get("index", "N/A")

        for raw_event in raw_batch:
            mapped_event = map_event(raw_event, event_lookup, pressure_success_ids)
            producer.produce(TOPIC_NAME, value=json.dumps(mapped_event), callback=delivery_report)
            producer.poll(0)

        producer.flush()

        print(
            f"[{display_time}] Published batch #{batch_number}: "
            f"{len(raw_batch)} events | indexes {first_index}-{last_index}"
        )

        if batch_number * EVENT_BATCH_SIZE < len(events):
            sleep_seconds = random.uniform(*SLEEP_RANGE_SECONDS)
            time.sleep(sleep_seconds)

    # Ensure any buffered messages are delivered before exit.
    producer.flush()
    print("Finished publishing all events.")


if __name__ == "__main__":
    stream_events()
