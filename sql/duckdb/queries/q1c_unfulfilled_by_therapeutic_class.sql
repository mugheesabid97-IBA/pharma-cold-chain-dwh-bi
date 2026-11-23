-- question: Q1 Unfulfilled demand analysis
-- widget: Unfulfilled demand + stockout days by therapeutic class (bar)

-- @variant: before  (raw star-schema join on the fact table)
SELECT mc.therapeutic_class, mc.cold_chain_flag,
       sum(f.unfulfilled_demand_packs) AS unfulfilled_packs,
       sum(f.stockout_days) AS stockout_days,
       round(sum(f.forecast_demand_packs) / nullif(sum(f.consumed_qty_packs), 0), 4) AS forecast_vs_actual_ratio
FROM fact_hospital_demand f
JOIN dim_medicine m ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY ALL
ORDER BY unfulfilled_packs DESC, mc.therapeutic_class;

-- @variant: after   (pre-aggregated summary table)
SELECT therapeutic_class, cold_chain_flag,
       sum(unfulfilled_packs) AS unfulfilled_packs,
       sum(stockout_days) AS stockout_days,
       round(sum(forecast_packs) / nullif(sum(consumed_packs), 0), 4) AS forecast_vs_actual_ratio
FROM agg_hospital_demand_class
GROUP BY ALL
ORDER BY unfulfilled_packs DESC, therapeutic_class;
