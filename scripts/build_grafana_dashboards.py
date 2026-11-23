"""Generate the 4 provisioned Grafana dashboards (JSON) for pipeline monitoring.

Run: python scripts/build_grafana_dashboards.py -> grafana/provisioning/dashboards/json/*.json
All panels query the Postgres `monitoring` schema (plus staging.rejected_rows for reasons).
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

OUT = Path(__file__).resolve().parents[1] / "grafana" / "provisioning" / "dashboards" / "json"
DS = {"uid": "pg-monitoring"}
# newest snapshot only (one record_storage_metrics call = one captured_at)
LATEST = "captured_at = (SELECT max(captured_at) FROM monitoring.storage_metrics)"


class Board:
    def __init__(self, uid: str, title: str, description: str):
        self.uid, self.title, self.description = uid, title, description
        self.panels: list[dict[str, Any]] = []
        self.x = self.y = 0
        self.row_h = 0

    def add(self, ptype: str, title: str, sql: str, w: int = 12, h: int = 8, fmt: str = "table",
            unit: str | None = None, options: dict | None = None, field_defaults: dict | None = None,
            overrides: list | None = None, description: str = "") -> None:
        if self.x + w > 24:
            self.x, self.y = 0, self.y + self.row_h
            self.row_h = 0
        defaults: dict[str, Any] = {"color": {"mode": "palette-classic"}}
        if unit:
            defaults["unit"] = unit
        defaults.update(field_defaults or {})
        self.panels.append({
            "id": len(self.panels) + 1, "type": ptype, "title": title, "description": description,
            "datasource": DS, "gridPos": {"h": h, "w": w, "x": self.x, "y": self.y},
            "targets": [{"refId": "A", "datasource": DS, "editorMode": "code", "rawQuery": True,
                         "format": fmt, "rawSql": " ".join(sql.split())}],
            "fieldConfig": {"defaults": defaults, "overrides": overrides or []},
            "options": options or {},
        })
        self.x += w
        self.row_h = max(self.row_h, h)

    def json(self) -> dict[str, Any]:
        return {"uid": self.uid, "title": self.title, "description": self.description, "editable": False,
                "schemaVersion": 39, "version": 1, "refresh": "15s", "tags": ["sdi", "phase1", "monitoring"],
                "time": {"from": "now-7d", "to": "now"}, "timepicker": {"refresh_intervals": ["10s", "15s", "30s", "1m"]},
                "timezone": "browser", "panels": self.panels, "templating": {"list": []}, "annotations": {"list": []},
                "links": [{"title": "Other dashboards", "type": "dashboards", "tags": ["sdi"], "asDropdown": True}]}


def stat_opts(color_mode: str = "value") -> dict:
    return {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": color_mode, "graphMode": "none", "textMode": "auto", "justifyMode": "center"}


def bar_opts(orientation: str = "horizontal", stacking: str = "none", x_field: str | None = None) -> dict:
    o = {"orientation": orientation, "stacking": stacking, "showValue": "auto", "barWidth": 0.8,
         "groupWidth": 0.7, "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
         "tooltip": {"mode": "multi"}, "xTickLabelRotation": 0, "xTickLabelMaxLength": 40}
    if x_field:
        o["xField"] = x_field
    return o


TS_OPTS = {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True}, "tooltip": {"mode": "multi"}}
THRESH_PCT = {"thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None},
                                                              {"color": "orange", "value": 90},
                                                              {"color": "green", "value": 99}]}}


def batch_board() -> Board:
    b = Board("sdi-batch-ingestion", "01 Batch Ingestion", "Per-file batch loads: duration, rows, success rate, rejects")
    b.add("stat", "Batch success rate %",
          "SELECT round(100.0 * count(*) FILTER (WHERE status = 'success') / nullif(count(*), 0), 1) AS success_rate "
          "FROM monitoring.batch_ingestion_log", w=6, h=5, unit="percent", options=stat_opts("background"),
          field_defaults={"color": {"mode": "thresholds"}, **THRESH_PCT, "min": 0, "max": 100},
          description="successful / attempted x 100 (all batch files ever ingested)")
    b.add("stat", "Files ingested", "SELECT count(*) AS files FROM monitoring.batch_ingestion_log", w=6, h=5,
          options=stat_opts())
    b.add("stat", "Rows loaded to staging", "SELECT sum(rows_loaded) AS rows_loaded FROM monitoring.batch_ingestion_log",
          w=6, h=5, unit="short", options=stat_opts())
    b.add("stat", "Rows rejected", "SELECT sum(rows_rejected) AS rows_rejected FROM monitoring.batch_ingestion_log",
          w=6, h=5, unit="short", options=stat_opts("background"),
          field_defaults={"color": {"mode": "fixed", "fixedColor": "orange"}})
    b.add("barchart", "Load duration (s) per source file",
          "SELECT file_name, duration_seconds FROM monitoring.batch_ingestion_log ORDER BY id", w=12, h=10, unit="s",
          options=bar_opts("horizontal", x_field="file_name"))
    b.add("barchart", "Rows processed per batch (loaded vs rejected)",
          "SELECT file_name, rows_loaded, rows_rejected FROM monitoring.batch_ingestion_log ORDER BY id", w=12, h=10,
          unit="short", options=bar_opts("horizontal", "normal", "file_name"))
    b.add("barchart", "Rejected rows by reason",
          "SELECT source || ': ' || rejection_reason AS reason, count(*) AS rejected_rows FROM staging.rejected_rows "
          "GROUP BY 1 ORDER BY 2 DESC LIMIT 20", w=12, h=9, options=bar_opts("horizontal", x_field="reason"))
    b.add("timeseries", "Rows loaded over time",
          "SELECT end_ts AS time, source AS metric, rows_loaded AS value FROM monitoring.batch_ingestion_log "
          "WHERE $__timeFilter(end_ts) ORDER BY 1", w=12, h=9, fmt="time_series", options=TS_OPTS,
          field_defaults={"custom": {"drawStyle": "bars", "barAlignment": 0, "fillOpacity": 60}})
    b.add("table", "Batch log", "SELECT id, run_id, file_name, source, start_ts, duration_seconds, rows_read, rows_loaded, "
          "rows_rejected, status, error_message FROM monitoring.batch_ingestion_log ORDER BY id DESC LIMIT 50",
          w=24, h=9, options={"showHeader": True})
    return b


def stream_board() -> Board:
    b = Board("sdi-streaming", "02 Streaming Ingestion", "Micro-batch stream simulation: throughput, latency, malformed events")
    b.add("stat", "Events ingested (total)", "SELECT sum(events_ingested) FROM monitoring.stream_metrics", w=6, h=5,
          options=stat_opts())
    b.add("stat", "Avg events / sec", "SELECT round(avg(events_per_sec), 1) FROM monitoring.stream_metrics", w=6, h=5,
          options=stat_opts())
    b.add("stat", "p95 end-to-end latency (ms)",
          "SELECT round(percentile_cont(0.95) WITHIN GROUP (ORDER BY p95_latency_ms)::numeric, 1) FROM monitoring.stream_metrics",
          w=6, h=5, unit="ms", options=stat_opts())
    b.add("stat", "Malformed / dropped events", "SELECT sum(malformed_count) FROM monitoring.stream_metrics", w=6, h=5,
          options=stat_opts("background"), field_defaults={"color": {"mode": "fixed", "fixedColor": "orange"}})
    b.add("timeseries", "Events per second", "SELECT window_ts AS time, events_per_sec AS \"events/sec\" "
          "FROM monitoring.stream_metrics WHERE $__timeFilter(window_ts) ORDER BY 1", w=24, h=9, fmt="time_series",
          options=TS_OPTS, field_defaults={"custom": {"drawStyle": "line", "lineWidth": 2, "fillOpacity": 15,
                                                      "spanNulls": False, "showPoints": "never"}})
    b.add("timeseries", "End-to-end latency (db_insert_ts - source_created_ts)",
          "SELECT window_ts AS time, avg_latency_ms AS \"avg latency\", p95_latency_ms AS \"p95 latency\" "
          "FROM monitoring.stream_metrics WHERE $__timeFilter(window_ts) ORDER BY 1", w=12, h=9, fmt="time_series",
          unit="ms", options=TS_OPTS, field_defaults={"custom": {"drawStyle": "line", "showPoints": "never"}})
    b.add("timeseries", "Malformed events per window",
          "SELECT window_ts AS time, malformed_count AS malformed, duplicate_count AS duplicates "
          "FROM monitoring.stream_metrics WHERE $__timeFilter(window_ts) ORDER BY 1", w=12, h=9, fmt="time_series",
          options=TS_OPTS, field_defaults={"custom": {"drawStyle": "bars", "fillOpacity": 70}})
    b.add("table", "Stream runs", "SELECT stream_id, min(window_ts) AS started, max(window_ts) AS ended, count(*) AS windows, "
          "sum(events_received) AS received, sum(events_ingested) AS ingested, sum(malformed_count) AS malformed, "
          "round(avg(events_per_sec), 1) AS avg_eps, round(avg(avg_latency_ms), 1) AS avg_latency_ms, "
          "round(max(p95_latency_ms), 1) AS max_p95_ms FROM monitoring.stream_metrics GROUP BY stream_id ORDER BY 2 DESC",
          w=24, h=7)
    return b


def storage_board() -> Board:
    b = Board("sdi-storage", "03 Storage & Staging", "Row counts and sizes per layer, DB size, date coverage")
    b.add("piechart", "Staging rows per source domain (latest)",
          f"SELECT domain, row_count FROM monitoring.storage_metrics WHERE {LATEST} "
          "AND layer = 'staging' ORDER BY 2 DESC", w=8, h=9,
          options={"pieType": "donut", "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": True},
                   "legend": {"displayMode": "table", "placement": "right", "values": ["value", "percent"]},
                   "displayLabels": ["percent"]})
    b.add("stat", "Postgres DB size", f"SELECT size_mb FROM monitoring.storage_metrics WHERE {LATEST} "
          "AND layer = 'database'", w=4, h=9, unit="decmbytes", options=stat_opts())
    b.add("stat", "DuckDB mart file size", f"SELECT size_mb FROM monitoring.storage_metrics WHERE {LATEST} "
          "AND layer = 'file'", w=4, h=9, unit="decmbytes", options=stat_opts())
    b.add("barchart", "Rows per layer (latest)",
          f"SELECT engine || ' ' || layer AS layer, sum(row_count) AS rows FROM monitoring.storage_metrics "
          f"WHERE {LATEST} AND row_count IS NOT NULL GROUP BY 1 ORDER BY 2 DESC", w=8, h=9,
          options=bar_opts("horizontal", x_field="layer"))
    b.add("table", "Table row counts (latest snapshot)",
          f"SELECT engine, layer, coalesce(domain, '') AS domain, object_name, row_count, round(size_mb, 2) AS size_mb "
          f"FROM monitoring.storage_metrics WHERE {LATEST} ORDER BY engine DESC, layer, object_name",
          w=12, h=12)
    b.add("table", "Date coverage per source (latest)",
          "SELECT DISTINCT ON (source) source, min_date, max_date, distinct_year_months, covers_2023_2025, "
          "refresh_months, captured_at FROM monitoring.date_coverage ORDER BY source, captured_at DESC, id DESC",
          w=12, h=6)
    b.add("timeseries", "Fact / staging row growth across loads",
          "SELECT captured_at AS time, object_name AS metric, row_count AS value FROM monitoring.storage_metrics "
          "WHERE stage = 'final' AND layer IN ('fact', 'staging') AND $__timeFilter(captured_at) ORDER BY 1",
          w=12, h=6, fmt="time_series", options=TS_OPTS,
          field_defaults={"custom": {"drawStyle": "line", "lineInterpolation": "stepAfter", "showPoints": "always"}})
    return b


def query_board() -> Board:
    b = Board("sdi-query-performance", "04 Query Performance", "Analytical query layer: timings before vs after optimisation")
    b.add("stat", "Daily active query count (today)",
          "SELECT count(*) FROM monitoring.query_log WHERE start_ts >= date_trunc('day', now())", w=6, h=5,
          options=stat_opts())
    b.add("stat", "Query error rate %",
          "SELECT round(100.0 * count(*) FILTER (WHERE status = 'error') / nullif(count(*), 0), 2) FROM monitoring.query_log",
          w=6, h=5, unit="percent", options=stat_opts("background"),
          field_defaults={"color": {"mode": "thresholds"}, "thresholds": {"mode": "absolute", "steps": [
              {"color": "green", "value": None}, {"color": "orange", "value": 1}, {"color": "red", "value": 5}]}})
    b.add("stat", "Avg speed-up (raw / summary)",
          "SELECT round(avg(b.ms / nullif(a.ms, 0))::numeric, 1) FROM (SELECT query_name, avg(duration_ms) ms FROM "
          "monitoring.query_log WHERE variant = 'before' AND status = 'success' GROUP BY 1) b JOIN (SELECT query_name, "
          "avg(duration_ms) ms FROM monitoring.query_log WHERE variant = 'after' AND status = 'success' GROUP BY 1) a "
          "USING (query_name)", w=6, h=5, unit="x", options=stat_opts())
    b.add("stat", "Total query executions", "SELECT count(*) FROM monitoring.query_log", w=6, h=5, options=stat_opts())
    b.add("barchart", "Average execution time per widget: before vs after optimisation (ms)",
          "SELECT widget, round(avg(duration_ms) FILTER (WHERE variant = 'before'), 2) AS \"before (raw star join)\", "
          "round(avg(duration_ms) FILTER (WHERE variant = 'after'), 2) AS \"after (summary table)\" "
          "FROM monitoring.query_log WHERE status = 'success' GROUP BY widget ORDER BY widget",
          w=24, h=11, unit="ms", options=bar_opts("vertical", x_field="widget"))
    b.add("timeseries", "Daily query count",
          "SELECT date_trunc('day', start_ts) AS time, count(*) AS queries FROM monitoring.query_log "
          "WHERE $__timeFilter(start_ts) GROUP BY 1 ORDER BY 1", w=12, h=8, fmt="time_series", options=TS_OPTS,
          field_defaults={"custom": {"drawStyle": "bars", "fillOpacity": 70}})
    b.add("timeseries", "Query duration per run (median ms)",
          "SELECT min(start_ts) AS time, variant AS metric, percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS value "
          "FROM monitoring.query_log WHERE $__timeFilter(start_ts) GROUP BY run_id, variant ORDER BY 1", w=12, h=8,
          fmt="time_series", unit="ms", options=TS_OPTS,
          field_defaults={"custom": {"drawStyle": "line", "showPoints": "always"}})
    b.add("table", "Before vs after per query",
          "SELECT query_name, widget, round(avg(duration_ms) FILTER (WHERE variant = 'before'), 2) AS before_ms, "
          "round(avg(duration_ms) FILTER (WHERE variant = 'after'), 2) AS after_ms, "
          "round((avg(duration_ms) FILTER (WHERE variant = 'before') / nullif(avg(duration_ms) FILTER (WHERE variant = 'after'), 0))::numeric, 1) AS speedup_x, "
          "count(*) AS executions, count(*) FILTER (WHERE status = 'error') AS errors "
          "FROM monitoring.query_log GROUP BY 1, 2 ORDER BY 1", w=24, h=9)
    return b


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    for i, board in enumerate((batch_board(), stream_board(), storage_board(), query_board()), start=1):
        name = ["batch_ingestion", "streaming_ingestion", "storage", "query_performance"][i - 1]
        (OUT / f"0{i}_{name}.json").write_text(json.dumps(board.json(), indent=2), encoding="utf-8")
        print("wrote", f"0{i}_{name}.json", len(board.panels), "panels")


if __name__ == "__main__":
    main()
