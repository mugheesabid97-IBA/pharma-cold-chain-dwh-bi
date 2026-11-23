"""Airflow-facing wrappers for the generator (JSON-serialisable in/out + monitoring log)."""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any

from psycopg2.extras import Json

from src.config import get_logger
from src.generator.oversampler import GeneratedFile, default_target_period, generate_refresh, publish
from src.staging import db

log = get_logger(__name__)


def plan_refresh(refresh_no: int, target_month: str | None) -> dict[str, Any]:
    period = str(target_month or default_target_period(refresh_no))
    return {"refresh_no": refresh_no, "target_month": period}


def generate(refresh_no: int, target_month: str, sources: list[str], sales_rows: int,
             stream_rows: int) -> list[dict[str, Any]]:
    t0 = time.perf_counter()
    files = generate_refresh(refresh_no, sources=sources, target=target_month, sales_rows=sales_rows,
                             stream_rows=stream_rows, do_publish=False)
    secs = round(time.perf_counter() - t0, 3)
    return [{**{k: (str(v) if isinstance(v, Path) else v) for k, v in f.__dict__.items()}, "run_seconds": secs}
            for f in files]


def publish_files(files: list[dict[str, Any]]) -> list[str]:
    objs = [GeneratedFile(**{k: (Path(v) if k in ("path", "tmp_path") and v else v)
                             for k, v in f.items() if k != "run_seconds"}) for f in files]
    publish(objs)
    return [str(o.path) for o in objs]


def log_generation(files: list[dict[str, Any]], refresh_no: int, target_month: str, run_id: str) -> int:
    db.apply_postgres_ddl()
    with db.get_conn() as conn, conn.cursor() as cur:
        for f in files:
            cur.execute(
                """INSERT INTO monitoring.generation_log (run_id, refresh_no, target_period, source, file_name,
                   rows_generated, bad_rows_injected, bad_row_breakdown, date_min, date_max, file_size_mb, run_seconds)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (run_id, refresh_no, target_month, f["source"] + (" (stream)" if f["kind"] == "stream" else ""),
                 Path(f["path"]).name, f["rows_generated"], f["bad_rows"], Json(f["bad_breakdown"]),
                 f["date_min"], f["date_max"], f["file_size_mb"], f["run_seconds"]))
    return len(files)
