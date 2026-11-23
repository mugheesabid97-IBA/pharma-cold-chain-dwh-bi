# Query optimisation: before vs after

Each analytical query was run in two variants against `warehouse/sdi_mart.duckdb`:

* **before**: dimensional query on the raw fact table (star join over facts + dimensions)
* **after**: the same answer read from a pre-aggregated summary table rebuilt by the DAG after each load (`CREATE OR REPLACE TABLE ... AS`, since DuckDB has no materialised views)

Each variant was executed 5 times; the first run is a warm-up, and the table shows the median of the rest. Every execution is also logged to `monitoring.query_log` (Grafana > Query Performance).

Fact sizes at measurement time: fact_sales = 528,000, fact_hospital_demand = 28,728.

| query | dashboard widget | before (ms) | after (ms) | speed-up | rows | same result |
|---|---|---:|---:|---:|---:|---|
| q1a_forecast_vs_actual_by_month | Forecast vs actual ratio - monthly trend (line) | 16.24 | 4.13 | 3.9x | 38 | yes |
| q1b_forecast_vs_actual_by_hospital | Forecast accuracy + unfulfilled % by hospital (bar) | 9.0 | 11.74 | 0.8x | 12 | yes |
| q1c_unfulfilled_by_therapeutic_class | Unfulfilled demand + stockout days by therapeutic class (bar) | 10.52 | 7.38 | 1.4x | 49 | yes |
| q1d_top_unfulfilled_hospital_medicine | Top 15 hospital x medicine shortages (table) | 10.1 | 1.97 | 5.1x | 15 | yes |
| q2a_channel_revenue_margin | Revenue share + gross margin % by channel (donut / KPI) | 11.27 | 3.55 | 3.2x | 3 | yes |
| q2b_channel_revenue_by_month | Monthly net revenue by channel (stacked columns) | 19.44 | 4.35 | 4.5x | 114 | yes |
| q2c_channel_revenue_by_region | Channel x region revenue + margin matrix (heatmap) | 16.47 | 2.14 | 7.7x | 12 | yes |
| q3a_top10_fast_moving | Top 10 fast-moving medicines (horizontal bar) | 63.49 | 1.77 | 35.9x | 10 | yes |
| q3b_top10_slow_moving | Top 10 slow-moving medicines (horizontal bar) | 81.88 | 2.39 | 34.2x | 10 | yes |
| q3c_velocity_by_class_cold_chain | Velocity rank by therapeutic class, cold-chain vs ambient (table) | 79.66 | 11.06 | 7.2x | 49 | yes |

## Reading the results

* **The biggest gains are on the 528k-row `fact_sales`.** The velocity queries (q3a/q3b) drop from about 60-80 ms to about 2 ms
  (30x+), because `agg_medicine_velocity` holds only 63 rows instead of scanning and joining every transaction.
* **Small gains, or none, on `fact_hospital_demand`.** It holds only about 29k rows, so the raw star join already finishes in
  about 10 ms. `q1b` (by hospital) is not faster: the raw join and the 456-row summary table (38 months x 12 hospitals) are both at the noise level
  of a few milliseconds. The summary tables matter here only once the hospital fact grows over many refreshes.
* **Every "after" query returns exactly the same rows as its "before" query** (column *same result*), so the
  optimisation never changes an answer. This is also covered by `tests/test_transform_sql.py`.

## Other optimisations

* **Facts sorted by date key.** Every incremental insert is `ORDER BY date_key`, and each refresh adds later months, so the fact tables stay physically sorted by date. DuckDB's automatic min/max zone-maps can then skip row groups for date-filtered queries.
* **No extra indexes in DuckDB.** DuckDB is a columnar engine. ART indexes only help point lookups and constraint checks, not the large scans and aggregations used here, and they slow down loads. The only keys are the dimension primary keys (surrogate keys).
* **Indexes in Postgres staging.** Postgres staging has unique indexes on the natural keys (used for duplicate checks during ingestion and for `ON CONFLICT` in streaming) plus an index on `batch_id`.
* **No partitioning.** At about 0.5 M fact rows the data fits in a few row groups, so partitioning would add complexity with no gain. A Parquet export partitioned by year/month is the next step if volumes grow.
