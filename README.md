# Pharma Cold Chain DWH + BI

Automated data-engineering pipeline (Apache Airflow → Postgres staging → DuckDB star schema → Grafana monitoring)
for a pharmaceutical cold supply chain.

**Phase 1: Sales & Demand Intelligence (SDI) Data Mart, Milestone 1:** work in progress.

- Source data profile: [docs/data_profile.md](docs/data_profile.md)
- The raw CSVs are not committed (05_pharmacy_sales_data.csv is over 218 MB). Put the 7 dataset CSVs in `data/raw/`.
