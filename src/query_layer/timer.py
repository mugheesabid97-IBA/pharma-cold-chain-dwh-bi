"""Timer wrapper for analytical queries: measures, counts and logs to monitoring.query_log."""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb

from src.config import get_logger
from src.staging import db as pgdb

log = get_logger(__name__)


@dataclass
class QueryTimer:
    """Runs SQL on DuckDB, times it, and buffers one monitoring row per execution."""
    con: duckdb.DuckDBPyConnection
    run_id: str | None = None
    counters: Counter = field(default_factory=Counter)
    rows: list[dict[str, Any]] = field(default_factory=list)

    def run(self, query_name: str, sql: str, *, widget: str, variant: str, question: str = "") -> list[tuple]:
        start = datetime.now(timezone.utc)
        t0 = time.perf_counter()
        result: list[tuple] = []
        status, error = "success", None
        try:
            result = self.con.execute(sql).fetchall()
        except Exception as exc:  # noqa: BLE001 - errors are logged (Query Error Rate), not raised
            status, error = "error", f"{type(exc).__name__}: {exc}"[:2000]
            log.error("query %s/%s failed: %s", query_name, variant, error)
        duration_ms = (time.perf_counter() - t0) * 1000
        self.counters["executed"] += 1
        self.counters[status] += 1
        self.rows.append({"run_id": self.run_id, "query_name": query_name, "business_question": question,
                          "widget": widget, "variant": variant, "start_ts": start,
                          "duration_ms": round(duration_ms, 3), "rows_returned": len(result),
                          "status": status, "error_message": error})
        return result

    def flush(self) -> int:
        """Write buffered executions to monitoring.query_log."""
        if not self.rows:
            return 0
        with pgdb.get_conn() as conn, conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO monitoring.query_log (run_id, query_name, business_question, widget, variant,
                   start_ts, duration_ms, rows_returned, status, error_message)
                   VALUES (%(run_id)s, %(query_name)s, %(business_question)s, %(widget)s, %(variant)s,
                           %(start_ts)s, %(duration_ms)s, %(rows_returned)s, %(status)s, %(error_message)s)""",
                self.rows)
        n = len(self.rows)
        self.rows.clear()
        return n
