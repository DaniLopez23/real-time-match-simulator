"""Shared configuration for incremental reporting."""

from __future__ import annotations

import os
from pathlib import Path

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass


PROJECT_ROOT = Path(__file__).resolve().parent.parent

REPORT_WINDOW_MINUTES = int(os.getenv("REPORT_WINDOW_MINUTES", "5"))

EVENTS_FILE = PROJECT_ROOT / "output" / "processed" / "events.parquet"
METRICS_FILE = PROJECT_ROOT / "output" / "aggregates" / "team_metrics.parquet"
WINDOW_METRICS_FILE = PROJECT_ROOT / "output" / "report_windows" / "window_metrics.parquet"
INCREMENTAL_SEGMENTS_FILE = PROJECT_ROOT / "output" / "reports" / "incremental_segments.parquet"
REPORT_PDF_FILE = PROJECT_ROOT / "output" / "reports" / "match_report.pdf"
