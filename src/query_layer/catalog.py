"""Catalog of the analytical queries (business question -> dashboard widget -> SQL file).

Each file in sql/duckdb/queries/ holds two variants separated by '-- @variant:' markers:
  before = dimensional query on the raw fact table (star join)
  after  = the same answer from a pre-aggregated summary table
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from src.config import SQL_DIR

QUERY_DIR = SQL_DIR / "duckdb" / "queries"
_VARIANT = re.compile(r"^-- @variant:\s*(\w+).*$", re.M)


@dataclass(frozen=True)
class AnalyticalQuery:
    name: str
    question: str
    widget: str
    sql: dict[str, str]     # variant -> SQL


def parse_query_file(path: Path) -> AnalyticalQuery:
    text = path.read_text(encoding="utf-8")
    question = re.search(r"^-- question:\s*(.+)$", text, re.M)
    widget = re.search(r"^-- widget:\s*(.+)$", text, re.M)
    parts = _VARIANT.split(text)
    # parts = [header, variant1, sql1, variant2, sql2, ...]
    sql = {parts[i]: parts[i + 1].strip().rstrip(";") for i in range(1, len(parts), 2)}
    return AnalyticalQuery(path.stem, question.group(1).strip() if question else "",
                           widget.group(1).strip() if widget else path.stem, sql)


def load_catalog(query_dir: Path = QUERY_DIR) -> list[AnalyticalQuery]:
    return [parse_query_file(p) for p in sorted(query_dir.glob("q*.sql"))]
