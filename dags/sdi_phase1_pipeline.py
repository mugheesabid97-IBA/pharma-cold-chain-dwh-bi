"""SDI Phase-1 pipeline: landing CSVs -> Postgres staging -> DuckDB star schema -> query layer.

detect_files -> ingest_master -> [ingest_sales, ingest_hospital] -> validate_report -> stream_simulation
-> record_storage_metrics_staging -> transform_dims -> transform_facts -> dq_checks
-> build_summary_tables -> run_analytical_queries -> record_storage_metrics_final

Triggered by the file-watcher (conf = {"files": [...]}) or manually (picks up everything in landing/).
All business logic lives in src/; every task is a thin wrapper.
"""
from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    "owner": "sdi-team",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=45),
}


def _run_id() -> str:
    return get_current_context()["run_id"]


@dag(
    dag_id="sdi_phase1_pipeline",
    description="Phase 1 Sales & Demand Intelligence: ingest -> staging -> DuckDB star schema -> query layer",
    schedule=os.environ.get("SDI_PIPELINE_SCHEDULE") or None,   # event-driven by default (file-watcher)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["phase1", "sdi", "pipeline"],
    doc_md=__doc__,
)
def sdi_phase1_pipeline():

    @task
    def detect_files() -> dict[str, list[str]]:
        from src.ingestion.orchestration import detect_files as detect
        conf = get_current_context()["dag_run"].conf or {}
        return detect(conf.get("files"))

    @task
    def ingest_master(files: dict[str, list[str]]) -> list[dict]:
        from src.ingestion.orchestration import ingest_via_api
        return ingest_via_api(files["master"], "master", _run_id())

    @task
    def ingest_sales(files: dict[str, list[str]]) -> list[dict]:
        from src.ingestion.orchestration import ingest_via_api
        return ingest_via_api(files["sales"], "sales", _run_id())

    @task
    def ingest_hospital(files: dict[str, list[str]]) -> list[dict]:
        from src.ingestion.orchestration import ingest_via_api
        return ingest_via_api(files["hospital"], "hospital", _run_id())

    @task
    def validate_report() -> dict:
        from src.ingestion.orchestration import validation_report
        return validation_report(_run_id())

    @task(execution_timeout=timedelta(minutes=20))
    def stream_simulation() -> list[dict]:
        from src.ingestion.stream_simulator import run_stream_simulation
        return run_stream_simulation(_run_id())

    @task
    def record_storage_metrics_staging() -> dict:
        from src.monitoring.storage_metrics import record_storage_metrics
        return record_storage_metrics("staging", _run_id())

    @task_group(group_id="transform_dims")
    def transform_dims():
        @task
        def extract_staging_increment() -> dict:
            from src.transform.runner import extract_increments
            return extract_increments()

        @task
        def load_dim_date() -> dict:
            from src.transform.runner import load_dim_date as run
            return run()

        @task
        def load_dim_medicine_scd2() -> dict:
            from src.transform.runner import load_dim_medicine_scd2 as run
            return run()

        @task
        def load_other_dims() -> dict:
            from src.transform.runner import load_other_dims as run
            return run()

        extract_staging_increment() >> load_dim_date() >> load_dim_medicine_scd2() >> load_other_dims()

    @task_group(group_id="transform_facts")
    def transform_facts():
        @task
        def load_fact_sales() -> dict:
            from src.transform.runner import load_fact_sales as run
            return run()

        @task
        def load_fact_hospital_demand() -> dict:
            from src.transform.runner import load_fact_hospital_demand as run
            return run()

        load_fact_sales() >> load_fact_hospital_demand()   # sequential: DuckDB has a single writer

    @task
    def dq_checks() -> int:
        from src.transform.dq import run_dq_checks
        return len(run_dq_checks(_run_id()))

    @task
    def build_summary_tables() -> dict:
        from src.transform.runner import build_summary_tables as run
        return run()

    @task
    def run_analytical_queries() -> list[dict]:
        from src.query_layer.runner import run_analytical_queries as run
        return run(_run_id())

    @task
    def record_storage_metrics_final() -> dict:
        from src.monitoring.storage_metrics import record_storage_metrics
        return record_storage_metrics("final", _run_id())

    files = detect_files()
    master = ingest_master(files)
    sales = ingest_sales(files)
    hospital = ingest_hospital(files)
    master >> [sales, hospital]     # medicines must be staged before MedicineID checks
    report = validate_report()
    [sales, hospital] >> report
    (report >> stream_simulation() >> record_storage_metrics_staging() >> transform_dims()
     >> transform_facts() >> dq_checks() >> build_summary_tables() >> run_analytical_queries()
     >> record_storage_metrics_final())


sdi_phase1_pipeline()
