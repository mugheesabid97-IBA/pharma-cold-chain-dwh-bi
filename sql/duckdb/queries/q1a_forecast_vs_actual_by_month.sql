-- question: Q1 Hospital forecast vs actual consumption
-- widget: Forecast vs actual ratio - monthly trend (line)

-- @variant: before  (raw star-schema join on the fact table)
SELECT d.year_month,
       sum(f.forecast_demand_packs) AS forecast_packs,
       sum(f.consumed_qty_packs) AS consumed_packs,
       round(sum(f.forecast_demand_packs) / nullif(sum(f.consumed_qty_packs), 0), 4) AS forecast_vs_actual_ratio,
       sum(f.unfulfilled_demand_packs) AS unfulfilled_packs
FROM fact_hospital_demand f
JOIN dim_date d ON d.date_key = f.month_date_key
GROUP BY d.year_month
ORDER BY d.year_month;

-- @variant: after   (pre-aggregated summary table)
SELECT year_month,
       sum(forecast_packs) AS forecast_packs,
       sum(consumed_packs) AS consumed_packs,
       round(sum(forecast_packs) / nullif(sum(consumed_packs), 0), 4) AS forecast_vs_actual_ratio,
       sum(unfulfilled_packs) AS unfulfilled_packs
FROM agg_hospital_demand_month
GROUP BY year_month
ORDER BY year_month;
