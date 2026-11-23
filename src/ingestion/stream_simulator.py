"""Streaming simulation: replay a refresh's stream file as ~1-second micro-batches to POST /ingest/stream.

Each event is stamped with source_created_ts at send time; Postgres stamps db_insert_ts,
so end-to-end latency = db_insert_ts - source_created_ts.
"""
from __future__ import annotations

import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from src.config import ARCHIVE_DIR, INGEST_API_URL, STREAM_DIR, STREAM_ROWS_PER_SEC, get_logger

log = get_logger(__name__)


def pending_stream_files() -> list[Path]:
    STREAM_DIR.mkdir(parents=True, exist_ok=True)
    return sorted(STREAM_DIR.glob("*.csv"))


def replay_file(path: Path, run_id: str | None = None, rows_per_sec: int = STREAM_ROWS_PER_SEC,
                api_url: str = INGEST_API_URL, archive: bool = True) -> dict[str, Any]:
    """Send the file's rows in micro-batches of `rows_per_sec`, one batch per second."""
    df = pd.read_csv(path, dtype=str, keep_default_na=False)
    records = df.replace({"": None}).to_dict(orient="records")
    stream_id = f"{path.stem[:60]}_{uuid.uuid4().hex[:6]}"
    totals = {"stream_id": stream_id, "file": path.name, "events_sent": 0, "events_ingested": 0,
              "malformed": 0, "duplicates": 0, "windows": 0}
    session = requests.Session()
    log.info("Streaming %s rows from %s at %s rows/s", len(records), path.name, rows_per_sec)
    for start in range(0, len(records), rows_per_sec):
        tick = time.perf_counter()
        batch = records[start:start + rows_per_sec]
        sent_ts = datetime.now(timezone.utc).isoformat()
        for r in batch:
            r["source_created_ts"] = sent_ts
        resp = session.post(f"{api_url}/ingest/stream", timeout=60, json={
            "stream_id": stream_id, "run_id": run_id, "source_file": path.name, "rows": batch})
        resp.raise_for_status()
        m = resp.json()
        totals["events_sent"] += len(batch)
        totals["events_ingested"] += m["events_ingested"]
        totals["malformed"] += m["malformed_count"]
        totals["duplicates"] += m["duplicate_count"]
        totals["windows"] += 1
        time.sleep(max(0.0, 1.0 - (time.perf_counter() - tick)))
    if archive:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(ARCHIVE_DIR / path.name))
    log.info("Stream finished: %s", totals)
    return totals


def run_stream_simulation(run_id: str | None = None) -> list[dict[str, Any]]:
    """Replay every pending stream file (normally one per refresh). No file -> nothing to do."""
    files = pending_stream_files()
    if not files:
        log.info("No stream files in %s (initial load has no stream); skipping", STREAM_DIR)
    return [replay_file(f, run_id) for f in files]


if __name__ == "__main__":
    print(run_stream_simulation())
