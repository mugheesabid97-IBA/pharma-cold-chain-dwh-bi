"""Run every analytical query before (raw star join) and after (summary table) optimisation."""
from __future__ import annotations

import statistics
from typing import Any

from src.config import DOCS_DIR, get_logger
from src.query_layer.catalog import load_catalog
from src.query_layer.timer import QueryTimer
from src.transform.runner import duck

log = get_logger(__name__)
REPEATS = 5   # each variant is executed REPEATS times; the first run is a cache warm-up


def run_analytical_queries(run_id: str | None = None, repeats: int = REPEATS) -> list[dict[str, Any]]:
    catalog = load_catalog()
    summary: list[dict[str, Any]] = []
    with duck(read_only=True) as con:
        timer = QueryTimer(con, run_id)
        for q in catalog:
            timings: dict[str, list[float]] = {}
            results: dict[str, list[tuple]] = {}
            for variant in ("before", "after"):
                for _ in range(repeats):
                    results[variant] = timer.run(q.name, q.sql[variant], widget=q.widget, variant=variant,
                                                 question=q.question)
                ms = [r["duration_ms"] for r in timer.rows if r["query_name"] == q.name and r["variant"] == variant]
                timings[variant] = ms[1:] if len(ms) > 1 else ms
            same = _same_result(results["before"], results["after"])
            b, a = statistics.median(timings["before"]), statistics.median(timings["after"])
            summary.append({"query": q.name, "widget": q.widget, "before_ms": round(b, 2), "after_ms": round(a, 2),
                            "speedup": round(b / a, 1) if a else None, "rows": len(results["after"]),
                            "results_match": same})
            timer.flush()
    log.info("query layer: %s", summary)
    return summary


def _same_result(a: list[tuple], b: list[tuple]) -> bool:
    """Both variants must return the same answer (numbers compared with a small tolerance)."""
    if len(a) != len(b):
        return False
    for ra, rb in zip(a, b):
        for x, y in zip(ra, rb):
            if isinstance(x, (int, float)) or hasattr(x, "as_integer_ratio"):
                if y is None or abs(float(x) - float(y)) > 1e-6 * max(1.0, abs(float(x))):
                    return False
            elif x != y:
                return False
    return True


def write_optimisation_doc(summary: list[dict[str, Any]], fact_rows: dict[str, int]) -> None:
    lines = ["# Query optimisation: before vs after", "",
             "Each analytical query was run in two variants against `warehouse/sdi_mart.duckdb`:",
             "", "* **before**: dimensional query on the raw fact table (star join over facts + dimensions)",
             "* **after**: the same answer read from a pre-aggregated summary table rebuilt by the DAG after "
             "each load (`CREATE OR REPLACE TABLE ... AS`, since DuckDB has no materialised views)", "",
             f"Each variant was executed {REPEATS} times; the first run is a warm-up, and the table shows the median of the rest. "
             "Every execution is also logged to `monitoring.query_log` (Grafana > Query Performance).", "",
             f"Fact sizes at measurement time: {', '.join(f'{k} = {v:,}' for k, v in fact_rows.items())}.", "",
             "| query | dashboard widget | before (ms) | after (ms) | speed-up | rows | same result |",
             "|---|---|---:|---:|---:|---:|---|"]
    for s in summary:
        lines.append(f"| {s['query']} | {s['widget']} | {s['before_ms']} | {s['after_ms']} | "
                     f"{s['speedup']}x | {s['rows']} | {'yes' if s['results_match'] else 'NO'} |")
    lines += ["", "## Other optimisations", "",
              "* **Facts sorted by date key.** Every incremental insert is `ORDER BY date_key`, and each refresh "
              "adds later months, so the fact tables stay physically sorted by date. DuckDB's automatic min/max "
              "zone-maps can then skip row groups for date-filtered queries.",
              "* **No extra indexes in DuckDB.** DuckDB is a columnar engine. ART indexes only help point "
              "lookups and constraint checks, not the large scans and aggregations used here, and they slow "
              "down loads. The only keys are the dimension primary keys (surrogate keys).",
              "* **Indexes in Postgres staging.** Postgres staging has unique indexes on the natural keys "
              "(used for duplicate checks during ingestion and for `ON CONFLICT` in streaming) plus an index on "
              "`batch_id`.",
              "* **No partitioning.** At about 0.5 M fact rows the data fits in a few row groups, so partitioning "
              "would add complexity with no gain. A Parquet export partitioned by year/month is the next step "
              "if volumes grow."]
    (DOCS_DIR / "query_optimisation.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
