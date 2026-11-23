-- Pre-aggregated summary tables, rebuilt after every load (DuckDB has no materialised views,
-- so CREATE OR REPLACE TABLE ... AS is used). Each serves one family of dashboard widgets.

-- Sales: month x medicine x channel x region (spec: agg_sales_month_medicine_channel)
CREATE OR REPLACE TABLE agg_sales_month_medicine_channel AS
SELECT d.year_month, d.month_start, d.year_no,
       m.medicine_id, mc.generic_name, mc.therapeutic_class, mc.cold_chain_flag,
       c.channel_name, g.region,
       count(*)                       AS txn_count,
       sum(f.quantity_sold_packs)     AS qty_packs,
       sum(f.gross_sales_usd)         AS gross_sales_usd,
       sum(f.discount_usd)            AS discount_usd,
       sum(f.net_sales_usd)           AS net_sales_usd,
       sum(f.cogs_usd)                AS cogs_usd,
       sum(f.gross_margin_usd)        AS gross_margin_usd
FROM fact_sales f
JOIN dim_date d       ON d.date_key = f.sale_date_key
JOIN dim_medicine m   ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc  ON mc.medicine_id = m.medicine_id AND mc.is_current   -- report on current attributes
JOIN dim_channel c    ON c.channel_key = f.channel_key
JOIN dim_geography g  ON g.geography_key = f.geography_key
GROUP BY ALL
ORDER BY d.year_month, m.medicine_id, c.channel_name, g.region;

-- Sales: month x channel x region (channel revenue / margin widgets)
CREATE OR REPLACE TABLE agg_sales_month_channel_region AS
SELECT year_month, month_start, year_no, channel_name, region,
       sum(txn_count) AS txn_count, sum(qty_packs) AS qty_packs,
       sum(gross_sales_usd) AS gross_sales_usd, sum(discount_usd) AS discount_usd,
       sum(net_sales_usd) AS net_sales_usd, sum(cogs_usd) AS cogs_usd,
       sum(gross_margin_usd) AS gross_margin_usd
FROM agg_sales_month_medicine_channel
GROUP BY ALL
ORDER BY year_month, channel_name, region;

-- Velocity per medicine (fast vs slow movers)
CREATE OR REPLACE TABLE agg_medicine_velocity AS
WITH monthly AS (
    SELECT medicine_id, generic_name, therapeutic_class, cold_chain_flag, year_month,
           sum(qty_packs) AS qty, sum(net_sales_usd) AS net
    FROM agg_sales_month_medicine_channel GROUP BY ALL
), last_month AS (SELECT max(year_month) AS ym FROM monthly)
SELECT medicine_id, generic_name, therapeutic_class, cold_chain_flag,
       sum(qty)                                   AS total_qty_packs,
       sum(net)                                   AS total_net_sales_usd,
       count(DISTINCT year_month)                 AS months_active,
       round(sum(qty) / count(DISTINCT year_month), 2) AS avg_monthly_qty,
       sum(CASE WHEN year_month >= strftime(CAST((SELECT ym FROM last_month) || '-01' AS DATE) - INTERVAL 2 MONTH, '%Y-%m')
                THEN qty ELSE 0 END)              AS last_3m_qty
FROM monthly
GROUP BY ALL
ORDER BY total_qty_packs DESC;

-- Hospital demand: month x hospital
CREATE OR REPLACE TABLE agg_hospital_demand_month AS
SELECT d.year_month, d.month_start, h.hospital_id, h.hospital_name, h.hospital_type, h.region,
       count(*)                          AS record_count,
       sum(f.forecast_demand_packs)      AS forecast_packs,
       sum(f.consumed_qty_packs)         AS consumed_packs,
       sum(f.requested_qty_packs)        AS requested_packs,
       sum(f.received_qty_packs)         AS received_packs,
       sum(f.unfulfilled_demand_packs)   AS unfulfilled_packs,
       sum(f.stockout_days)              AS stockout_days
FROM fact_hospital_demand f
JOIN dim_date d     ON d.date_key = f.month_date_key
JOIN dim_hospital h ON h.hospital_key = f.hospital_key
GROUP BY ALL
ORDER BY d.year_month, h.hospital_id;

-- Hospital demand: month x therapeutic class
CREATE OR REPLACE TABLE agg_hospital_demand_class AS
SELECT d.year_month, d.month_start, mc.therapeutic_class, mc.cold_chain_flag,
       count(*)                          AS record_count,
       sum(f.forecast_demand_packs)      AS forecast_packs,
       sum(f.consumed_qty_packs)         AS consumed_packs,
       sum(f.unfulfilled_demand_packs)   AS unfulfilled_packs,
       sum(f.stockout_days)              AS stockout_days
FROM fact_hospital_demand f
JOIN dim_date d      ON d.date_key = f.month_date_key
JOIN dim_medicine m  ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY ALL
ORDER BY d.year_month, mc.therapeutic_class;

-- Hospital demand: hospital x medicine (all-time unfulfilled analysis)
CREATE OR REPLACE TABLE agg_hospital_unfulfilled AS
SELECT h.hospital_id, h.hospital_name, h.region, m.medicine_id, mc.generic_name, mc.therapeutic_class,
       mc.cold_chain_flag,
       sum(f.forecast_demand_packs)      AS forecast_packs,
       sum(f.consumed_qty_packs)         AS consumed_packs,
       sum(f.unfulfilled_demand_packs)   AS unfulfilled_packs,
       sum(f.stockout_days)              AS stockout_days,
       count(*) FILTER (WHERE f.unfulfilled_demand_packs > 0) AS months_with_shortage
FROM fact_hospital_demand f
JOIN dim_hospital h  ON h.hospital_key = f.hospital_key
JOIN dim_medicine m  ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY ALL
ORDER BY unfulfilled_packs DESC;
