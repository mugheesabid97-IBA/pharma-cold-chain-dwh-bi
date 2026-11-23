-- question: Q1 Hospital forecast vs actual consumption
-- widget: Forecast accuracy + unfulfilled % by hospital (bar)

-- @variant: before  (raw star-schema join on the fact table)
SELECT h.hospital_name, h.hospital_type, h.region,
       sum(f.forecast_demand_packs) AS forecast_packs,
       sum(f.consumed_qty_packs) AS consumed_packs,
       round(sum(f.forecast_demand_packs) / nullif(sum(f.consumed_qty_packs), 0), 4) AS forecast_vs_actual_ratio,
       sum(f.unfulfilled_demand_packs) AS unfulfilled_packs,
       round(100.0 * sum(f.unfulfilled_demand_packs) / nullif(sum(f.requested_qty_packs), 0), 2) AS unfulfilled_pct_of_requested
FROM fact_hospital_demand f
JOIN dim_hospital h ON h.hospital_key = f.hospital_key
GROUP BY ALL
ORDER BY forecast_vs_actual_ratio, h.hospital_name;

-- @variant: after   (pre-aggregated summary table)
SELECT hospital_name, hospital_type, region,
       sum(forecast_packs) AS forecast_packs,
       sum(consumed_packs) AS consumed_packs,
       round(sum(forecast_packs) / nullif(sum(consumed_packs), 0), 4) AS forecast_vs_actual_ratio,
       sum(unfulfilled_packs) AS unfulfilled_packs,
       round(100.0 * sum(unfulfilled_packs) / nullif(sum(requested_packs), 0), 2) AS unfulfilled_pct_of_requested
FROM agg_hospital_demand_month
GROUP BY ALL
ORDER BY forecast_vs_actual_ratio, hospital_name;
