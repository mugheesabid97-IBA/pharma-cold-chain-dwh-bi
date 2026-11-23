"""Batch ingestion: CSV in landing/ -> validate -> staging.* / staging.rejected_rows -> archive/."""
from __future__ import annotations

import shutil
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config import ARCHIVE_DIR, INGEST_CHUNK_ROWS, LANDING_DIR, REJECTED_DIR, get_logger
from src.ingestion.validation import SchemaValidationError, check_columns, pk_values, validate
from src.staging import db
from src.staging.schemas import SOURCES, SourceSpec, source_for_file

log = get_logger(__name__)


def new_batch_id(source: str) -> str:
    return f"{source}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S}_{uuid.uuid4().hex[:6]}"


def resolve_path(file_path: str) -> Path:
    """Accept an absolute path or a bare file name relative to landing/."""
    p = Path(file_path)
    if not p.is_absolute():
        p = LANDING_DIR / p.name
    return p


def _log_batch(row: dict[str, Any]) -> None:
    with db.get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO monitoring.batch_ingestion_log
               (run_id, batch_id, file_name, source, start_ts, end_ts, duration_seconds,
                rows_read, rows_loaded, rows_rejected, status, error_message)
               VALUES (%(run_id)s, %(batch_id)s, %(file_name)s, %(source)s, %(start_ts)s, %(end_ts)s,
                       %(duration_seconds)s, %(rows_read)s, %(rows_loaded)s, %(rows_rejected)s,
                       %(status)s, %(error_message)s)""", row)


def _move(src: Path, dest_dir: Path) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / src.name
    if dest.exists():
        dest = dest_dir / f"{src.stem}__{datetime.now():%Y%m%d%H%M%S}{src.suffix}"
    shutil.move(str(src), str(dest))
    return dest


def ingest_file(file_path: str, source: str | None = None, run_id: str | None = None,
                move_file: bool = True) -> dict[str, Any]:
    """Validate and load one CSV into staging. Returns the batch summary (also logged)."""
    path = resolve_path(file_path)
    spec: SourceSpec | None = SOURCES.get(source) if source else source_for_file(path.name)
    if spec is None:
        raise ValueError(f"Cannot determine Phase-1 source for {path.name} (source={source})")
    batch_id = new_batch_id(spec.key)
    start = datetime.now(timezone.utc)
    t0 = time.perf_counter()
    summary: dict[str, Any] = {
        "run_id": run_id, "batch_id": batch_id, "file_name": path.name, "source": spec.key,
        "start_ts": start, "rows_read": 0, "rows_loaded": 0, "rows_rejected": 0,
        "status": "failed", "error_message": None,
    }
    rejected_frames: list[pd.DataFrame] = []
    try:
        if not path.exists():
            raise FileNotFoundError(f"{path} not found")
        header = pd.read_csv(path, nrows=0).columns.tolist()
        check_columns(header, spec)
        db.apply_postgres_ddl()
        seen: set[str] = set()
        with db.get_conn() as conn:
            known = db.known_medicine_ids(conn) if spec.medicine_fk else None
            reader = pd.read_csv(path, dtype=str, keep_default_na=False, chunksize=INGEST_CHUNK_ROWS)
            offset = 0
            for chunk in reader:
                chunk.index = range(offset, offset + len(chunk))
                chunk_pks = pk_values(chunk[list(spec.pk)], spec).dropna().tolist()
                existing = db.existing_pks(conn, spec, chunk_pks)
                res = validate(chunk, spec, known_medicine_ids=known, existing_pks=existing,
                               seen_pks=seen, row_offset=0)
                valid = res.valid.assign(batch_id=batch_id, source_file=path.name, ingest_mode="batch")
                summary["rows_loaded"] += db.copy_dataframe(conn, valid, f"staging.{spec.table}")
                db.insert_rejected(conn, res.rejected, batch_id=batch_id, source=spec.key,
                                   source_file=path.name, ingest_mode="batch")
                summary["rows_read"] += len(chunk)
                summary["rows_rejected"] += res.n_rejected
                if res.n_rejected:
                    rejected_frames.append(res.rejected)
                offset += len(chunk)
                log.info("%s: %s rows read, %s loaded, %s rejected so far", path.name,
                         summary["rows_read"], summary["rows_loaded"], summary["rows_rejected"])
        summary["status"] = "success"
    except (SchemaValidationError, FileNotFoundError, ValueError) as exc:
        summary["error_message"] = str(exc)[:2000]
        log.error("Batch %s failed: %s", batch_id, exc)
    except Exception as exc:  # noqa: BLE001 - any DB/IO failure fails the file, not the API
        summary["error_message"] = f"{type(exc).__name__}: {exc}"[:2000]
        log.exception("Batch %s failed", batch_id)
        summary["rows_loaded"] = 0   # transaction was rolled back
    end = datetime.now(timezone.utc)
    summary.update(end_ts=end, duration_seconds=round(time.perf_counter() - t0, 3))
    _log_batch(summary)

    if rejected_frames:
        REJECTED_DIR.mkdir(parents=True, exist_ok=True)
        pd.concat(rejected_frames).to_csv(REJECTED_DIR / f"{path.stem}__rejected.csv", index=False)
    if move_file and path.exists():
        dest = _move(path, ARCHIVE_DIR if summary["status"] == "success" else REJECTED_DIR)
        summary["moved_to"] = str(dest)
    summary["start_ts"], summary["end_ts"] = start.isoformat(), end.isoformat()
    return summary
