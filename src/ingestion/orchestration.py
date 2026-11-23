"""Helpers used by the Airflow pipeline tasks (kept out of the DAG file so they are testable)."""
from __future__ import annotations

from typing import Any

import requests

from src.config import INGEST_API_URL, LANDING_DIR, get_logger
from src.staging import db
from src.staging.schemas import SOURCES, source_for_file

log = get_logger(__name__)


def detect_files(conf_files: list[str] | None = None) -> dict[str, list[str]]:
    """Files to ingest this run, grouped by source. Uses the watcher's list if given, else scans landing/."""
    db.apply_postgres_ddl()
    LANDING_DIR.mkdir(parents=True, exist_ok=True)
    names = conf_files if conf_files else sorted(p.name for p in LANDING_DIR.glob("*.csv"))
    grouped: dict[str, list[str]] = {k: [] for k in SOURCES}
    for name in names:
        spec = source_for_file(name)
        if spec is None:
            log.info("skip non-Phase-1 file %s", name)
            continue
        if not (LANDING_DIR / name).exists():
            log.warning("%s listed but not in landing (already processed?)", name)
            continue
        grouped[spec.key].append(name)
    log.info("files to ingest: %s", grouped)
    return grouped


def ingest_via_api(files: list[str], source: str, run_id: str) -> list[dict[str, Any]]:
    """POST each file to the ingestion API. Raises if a file fails, so the task goes red/retries."""
    results = []
    for name in files:
        resp = requests.post(f"{INGEST_API_URL}/ingest/batch", timeout=3600,
                             json={"file_path": name, "source": source, "run_id": run_id})
        body = resp.json()
        if resp.status_code != 200:
            raise RuntimeError(f"ingestion of {name} failed: {body}")
        log.info("%s -> read %s, loaded %s, rejected %s in %ss", name, body["rows_read"], body["rows_loaded"],
                 body["rows_rejected"], body["duration_seconds"])
        results.append(body)
    if not files:
        log.info("no %s files in this run", source)
    return results


def validation_report(run_id: str) -> dict[str, Any]:
    """Summarise this run's batch results + rejection reasons (also visible in Grafana)."""
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute("""SELECT source, file_name, rows_read, rows_loaded, rows_rejected, status
                       FROM monitoring.batch_ingestion_log WHERE run_id = %s ORDER BY id""", (run_id,))
        files = [dict(zip(("source", "file", "read", "loaded", "rejected", "status"), r)) for r in cur.fetchall()]
        cur.execute("""SELECT r.source, r.rejection_reason, count(*)
                       FROM staging.rejected_rows r
                       JOIN monitoring.batch_ingestion_log b ON b.batch_id = r.batch_id
                       WHERE b.run_id = %s GROUP BY 1, 2 ORDER BY 1, 3 DESC""", (run_id,))
        reasons = [dict(zip(("source", "reason", "rows"), r)) for r in cur.fetchall()]
    failed = [f for f in files if f["status"] != "success"]
    report = {"files": files, "rejection_reasons": reasons,
              "batch_success_rate_pct": round(100 * (len(files) - len(failed)) / len(files), 1) if files else None}
    log.info("validation report: %s", report)
    if failed:
        raise RuntimeError(f"failed batches: {failed}")
    return report
