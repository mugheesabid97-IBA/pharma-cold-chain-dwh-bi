"""File watcher service: detects new CSVs in data/landing/ and triggers the Airflow pipeline.

* Uses watchdog's PollingObserver: inotify events do not cross Docker Desktop bind mounts on
  Windows/macOS, so polling is the reliable choice.
* A file is "ready" when its size has been stable for STABLE_SECONDS (the generator also writes
  *.tmp files and renames them, so half-written files are never picked up).
* Files arriving together (one refresh = 3 files) are debounced into ONE DAG run.
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers.polling import PollingObserver

from src.config import (AIRFLOW_API_URL, AIRFLOW_PASSWORD, AIRFLOW_USER, LANDING_DIR,
                        PIPELINE_DAG_ID, get_logger)
from src.staging.schemas import source_for_file

log = get_logger("file-watcher")
STABLE_SECONDS = 10
QUIET_SECONDS = 15
HEARTBEAT = Path("/tmp/watcher_heartbeat")


class LandingHandler(FileSystemEventHandler):
    def __init__(self) -> None:
        self.pending: dict[str, tuple[int, float]] = {}   # path -> (last size, time size last changed)

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        path = getattr(event, "dest_path", None) or event.src_path
        if str(path).endswith(".csv"):
            self.pending.setdefault(str(path), (-1, time.time()))


def is_phase1_csv(path: Path) -> bool:
    return path.suffix == ".csv" and source_for_file(path.name) is not None


def trigger_dag(files: list[str]) -> str:
    run_id = f"watcher__{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    resp = requests.post(f"{AIRFLOW_API_URL}/dags/{PIPELINE_DAG_ID}/dagRuns",
                         auth=(AIRFLOW_USER, AIRFLOW_PASSWORD), timeout=30,
                         json={"dag_run_id": run_id, "conf": {"files": files, "triggered_by": "file-watcher"}})
    resp.raise_for_status()
    log.info("Triggered %s run %s for %s", PIPELINE_DAG_ID, run_id, files)
    return run_id


def main() -> None:
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    handler = LandingHandler()
    for p in LANDING_DIR.glob("*.csv"):          # files that were already there at start-up
        handler.pending[str(p)] = (-1, time.time())
    observer = PollingObserver(timeout=3)
    observer.schedule(handler, str(LANDING_DIR), recursive=False)
    observer.start()
    log.info("Watching %s (polling)", LANDING_DIR)
    triggered: set[str] = set()
    ready: set[str] = set()
    last_change = time.time()
    try:
        while True:
            HEARTBEAT.write_text(str(time.time()))
            now = time.time()
            for path_s, (size, since) in list(handler.pending.items()):
                p = Path(path_s)
                if not p.exists():
                    handler.pending.pop(path_s, None)
                    ready.discard(path_s)
                    triggered.discard(path_s)
                    continue
                cur = p.stat().st_size
                if cur != size:
                    handler.pending[path_s] = (cur, now)
                    last_change = now
                elif now - since >= STABLE_SECONDS and path_s not in triggered:
                    if is_phase1_csv(p):
                        ready.add(path_s)
                    else:
                        log.info("Ignoring non-Phase-1 file %s", p.name)
                        triggered.add(path_s)
            unsettled = [s for s, (sz, since) in handler.pending.items()
                         if s not in triggered and s not in ready and Path(s).exists()]
            if ready and not unsettled and now - last_change >= QUIET_SECONDS:
                names = sorted(Path(s).name for s in ready)
                try:
                    trigger_dag(names)
                    triggered |= ready
                    ready.clear()
                except requests.RequestException as exc:
                    log.warning("Airflow not reachable yet (%s); will retry", exc)
            time.sleep(2)
    finally:
        observer.stop()
        observer.join()


if __name__ == "__main__":
    main()
