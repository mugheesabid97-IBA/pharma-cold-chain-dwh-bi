"""Data Oversampling DAG: synthesise one refresh period of source data.

plan_refresh -> generate_files -> publish_to_landing -> log_generation_stats

Publishing drops the CSVs into data/landing/ (and the stream replay file into data/stream/);
the file-watcher then triggers sdi_phase1_pipeline automatically.
Phase 1 generates 01 (master price changes), 05 (sales) and 06 (hospital demand);
02, 03, 04 and 07 are supported via the 'sources' parameter for later phases.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context


@dag(
    dag_id="data_generator_dag",
    description="Oversample source files for the next refresh period (Phase 1: 01, 05, 06)",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "sdi-team", "retries": 2, "retry_delay": timedelta(seconds=30),
                  "execution_timeout": timedelta(minutes=20)},
    params={
        "refresh_no": Param(1, type="integer", minimum=1, description="Refresh number (1 = first month after the data)"),
        "target_month": Param("", type="string", description="YYYY-MM; empty = last data month + refresh_no"),
        "sources": Param(["01", "05", "06"], type="array", description="Files to oversample: any of 01..07"),
        "sales_rows": Param(14000, type="integer", description="Sales rows per month (batch + stream)"),
        "stream_rows": Param(3000, type="integer", description="Sales rows replayed by the stream simulation"),
    },
    tags=["phase1", "generator"],
    doc_md=__doc__,
)
def data_generator_dag():

    @task
    def plan_refresh() -> dict:
        from src.generator.tasks import plan_refresh as plan
        p = get_current_context()["params"]
        return plan(int(p["refresh_no"]), p["target_month"] or None)

    @task
    def generate_files(plan: dict) -> list[dict]:
        from src.generator.tasks import generate
        p = get_current_context()["params"]
        return generate(plan["refresh_no"], plan["target_month"], list(p["sources"]),
                        int(p["sales_rows"]), int(p["stream_rows"]))

    @task
    def publish_to_landing(files: list[dict]) -> list[str]:
        from src.generator.tasks import publish_files
        return publish_files(files)

    @task
    def log_generation_stats(files: list[dict], plan: dict) -> int:
        from src.generator.tasks import log_generation
        return log_generation(files, plan["refresh_no"], plan["target_month"], get_current_context()["run_id"])

    plan = plan_refresh()
    files = generate_files(plan)
    published = publish_to_landing(files)
    published >> log_generation_stats(files, plan)


data_generator_dag()
