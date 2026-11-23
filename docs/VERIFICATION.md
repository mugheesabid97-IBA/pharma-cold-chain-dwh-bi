# Verification: commands and actual output

All commands are run from the project folder after `docker compose up -d --build`.
The output below is what was actually obtained on 2026-11-23/24 (Windows 11, Docker Desktop 29.9.3, 4 GB / 4 CPUs for Docker).

## 1. All services up and healthy

```bash
docker compose ps --format "table {{.Service}}\t{{.Image}}\t{{.Status}}"
```
```
SERVICE        IMAGE                        STATUS
airflow        pharma-sdi-airflow:2.10.4    Up 24 minutes (healthy)
file-watcher   pharma-sdi-airflow:2.10.4    Up 24 minutes (healthy)
grafana        grafana/grafana-oss:11.3.0   Up 24 minutes (healthy)
ingest-api     pharma-sdi-airflow:2.10.4    Up 24 minutes (healthy)
postgres       postgres:16-alpine           Up 24 minutes (healthy)
```

```bash
curl -s localhost:8000/health
```
```
{"status":"ok","database":"ok"}
```

## 2. Airflow: both DAGs, no import errors, all runs green

```bash
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags list
```
```
No data found
dag_id              | fileloc                                  | owners   | is_paused
data_generator_dag  | /opt/airflow/dags/data_generator_dag.py  | sdi-team | False
sdi_phase1_pipeline | /opt/airflow/dags/sdi_phase1_pipeline.py | sdi-team | False
```

```bash
curl -s -u airflow:airflow "localhost:8080/api/v1/dags/sdi_phase1_pipeline/dagRuns?order_by=start_date"
curl -s -u airflow:airflow "localhost:8080/api/v1/dags/data_generator_dag/dagRuns?order_by=start_date"
```
```
sdi_phase1_pipeline watcher__20261123T204322 success   <- initial load (triggered by file-watcher)
sdi_phase1_pipeline watcher__20261123T204832 success   <- refresh 1 (triggered by file-watcher)
sdi_phase1_pipeline watcher__20261123T205355 success   <- refresh 2 (triggered by file-watcher)
sdi_phase1_pipeline manual_rerun_no_new_data success   <- idempotency: re-run, nothing new
sdi_phase1_pipeline watcher__20261123T210055 success   <- idempotency: processed files dropped in again
data_generator_dag  refresh_01               success
data_generator_dag  refresh_02               success
```

Every task of every run succeeded on its first try. Follow a run live with `python scripts/wait_for_run.py sdi_phase1_pipeline`:
```
[     1s] detect_files                                  success         7.1s try=1
[     1s] ingest_master                                 success         1.6s try=1
[     6s] ingest_hospital                               success         11.1s try=1
[    76s] ingest_sales                                  success         79.8s try=1
[    82s] validate_report                               success         3.2s try=1
[    82s] stream_simulation                             success         1.4s try=1
[    93s] record_storage_metrics_staging                success         7.4s try=1
[   156s] transform_dims.extract_staging_increment      success         61.2s try=1
[   162s] transform_dims.load_dim_date                  success         3.2s try=1
[   162s] transform_dims.load_dim_medicine_scd2         success         1.4s try=1
[   168s] transform_dims.load_other_dims                success         2.4s try=1
[   173s] transform_facts.load_fact_sales               success         7.6s try=1
[   178s] transform_facts.load_fact_hospital_demand     success         2.0s try=1
[   178s] dq_checks                                     success         1.8s try=1
[   184s] build_summary_tables                          success         1.6s try=1
[   190s] run_analytical_queries                        success         3.1s try=1
[   195s] record_storage_metrics_final                  success         7.5s try=1
RUN watcher__20261123T204322: SUCCESS in 195s
```
(Initial load. In the refresh runs `stream_simulation` takes about 125 s, because it replays 3,000 events at 25 events/s.)

## 3. Staging populated, bad rows rejected with reasons

```bash
docker compose exec postgres psql -U pharma -d pharma_dwh -c \
  "select source, rejection_reason, count(*) from staging.rejected_rows group by 1,2 order by 1,3 desc"
```
After Refresh 2, before the idempotency test (cumulative over both refreshes, batch + stream):

| source | reason | rows |
|---|---|---:|
| hospital | null MedicineID | 4 |
| hospital | MedicineID not found in master data | 4 |
| hospital | ForecastDemandPacks must be >= 0 | 4 |
| hospital | invalid date in DemandMonth | 2 |
| hospital | duplicate PK within file | 2 |
| master | RecommendedRetailPriceUSD must be > 0 | 2 |
| sales | 5 injected types (price <= 0, null / unknown MedicineID, invalid date, duplicate PK) | 280 |

298 rejected in total = 2 x (1 master + 8 hospital + 110 sales batch + 30 sales stream), i.e. exactly the injected bad rows.
Full breakdown: [run_evidence.md](run_evidence.md).

## 4. Mart: dims + facts, counts match, no orphans, SCD2 history

```bash
docker compose exec postgres psql -U pharma -d pharma_dwh -c \
  "select run_id, count(*) checks, count(*) filter (where status='fail') failed from monitoring.dq_check_log group by 1 order by min(id)"
```
```
          run_id          | checks | failed
--------------------------+--------+--------
 watcher__20261123T204322 |     39 |      0
 watcher__20261123T204832 |     39 |      0
 watcher__20261123T205355 |     39 |      0
 manual_rerun_no_new_data |     39 |      0
 watcher__20261123T210055 |     39 |      0
```
The 39 checks cover: fact rows = valid staging rows (both facts), no NULL surrogate keys and no orphan keys for every
fact FK (15 FKs), no duplicate natural keys, one current SCD2 version per medicine, no overlapping validity ranges,
and no unknown medicine / sale-date keys.

SCD2 after Refresh 2 (6 medicines with a price change, each with a closed and a current version):
```
| MED007 | 46 | 50.1600 | 2023-01-01 | 2025-12-31 | False |
| MED007 | 65 | 53.3300 | 2026-01-01 | 9999-12-31 | True  |
| MED008 | 51 | 48.8400 | 2023-01-01 | 2026-01-31 | False |
| MED008 | 69 | 53.8400 | 2026-02-01 | 9999-12-31 | True  |
  ... MED026, MED039, MED040, MED057 (see run_evidence.md)
```

## 5. Growth across loads + idempotency

```bash
docker compose exec airflow python scripts/collect_evidence.py "label"     # appends a snapshot to docs/run_evidence.md
```

| stage | fact_sales | fact_hospital_demand | max sale date |
|---|---:|---:|---|
| Initial load | 500,000 | 27,216 | 2025-12-31 |
| Refresh 1 | 514,000 | 27,972 | 2026-01-31 |
| Refresh 2 | 528,000 | 28,728 | 2026-02-28 |
| Re-run + re-ingest of processed files | **528,000** | **28,728** | 2026-02-28 |

Re-ingest of the processed Refresh 2 files: `11,110 + 764 rows read, 0 loaded`.

## 6. Grafana: 4 dashboards with real data

```bash
python scripts/check_grafana_panels.py      # runs every panel query through Grafana's /api/ds/query
```
```
01 Batch Ingestion
  OK    Batch success rate %: 1 row(s) value=100
  OK    Files ingested: 1 row(s) value=11
  OK    Rows loaded to staging: 1 row(s) value=550797
  OK    Rows rejected: 1 row(s) value=12112
  OK    Load duration (s) per source file: 11 row(s)
  ...
02 Streaming Ingestion
  OK    Events ingested (total): 1 row(s) value=6000
  OK    Avg events / sec: 1 row(s) value=24.6
  OK    p95 end-to-end latency (ms): 1 row(s) value=155.2
  OK    Malformed / dropped events: 1 row(s) value=60
  OK    Events per second: 244 row(s)
  ...
03 Storage & Staging
  OK    Staging rows per source domain (latest): 3 row(s)
  OK    Postgres DB size: 1 row(s) value=373.202
  OK    DuckDB mart file size: 1 row(s) value=65.512
  ...
04 Query Performance
  OK    Query error rate %: 1 row(s) value=0
  OK    Avg speed-up (raw / summary): 1 row(s) value=9.7
  OK    Average execution time per widget: before vs after optimisation (ms): 10 row(s)
  ...
32 panels, 0 errors, 0 empty
```
The panels changed across the loads: e.g. Files ingested 3 -> 6 -> 9 (-> 11 with the re-ingest), stream events
0 -> 3,000 -> 6,000, and the row-growth time series has one step per load.

## 7. Query layer: before vs after optimisation

```bash
docker compose exec postgres psql -U pharma -d pharma_dwh -c \
  "select variant, count(*) executions, round(avg(duration_ms),2) avg_ms, count(*) filter (where status='error') errors from monitoring.query_log group by 1"
```
```
 variant | executions | avg_ms | errors
---------+------------+--------+--------
 after   |        300 |   4.93 |      0
 before  |        300 |  33.47 |      0
```
Per-query table: [query_optimisation.md](query_optimisation.md).

## 8. Unit tests

```bash
docker compose exec airflow python -m pytest -q tests        # or locally: .venv/Scripts/python -m pytest -q tests
```
```
...............................                                          [100%]
31 passed in 15.50s
```

## 9. Git hygiene

```bash
git ls-files | grep -iE "\.env$|\.duckdb|^data/|\.csv$|\.whl$"     # -> no output
```
`.env`, `data/`, `warehouse/*.duckdb`, `logs/`, `docker/wheels/*.whl` and `__pycache__` are git-ignored; the largest
committed file is well under 1 MB.
