-- question: Q1 Unfulfilled demand analysis
-- widget: Top 15 hospital x medicine shortages (table)

-- @variant: before  (raw star-schema join on the fact table)
SELECT h.hospital_name, mc.generic_name, mc.therapeutic_class, mc.cold_chain_flag,
       sum(f.unfulfilled_demand_packs) AS unfulfilled_packs,
       count(*) FILTER (WHERE f.unfulfilled_demand_packs > 0) AS months_with_shortage
FROM fact_hospital_demand f
JOIN dim_hospital h ON h.hospital_key = f.hospital_key
JOIN dim_medicine m ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY ALL
ORDER BY unfulfilled_packs DESC, h.hospital_name, mc.generic_name
LIMIT 15;

-- @variant: after   (pre-aggregated summary table)
SELECT hospital_name, generic_name, therapeutic_class, cold_chain_flag, unfulfilled_packs, months_with_shortage
FROM agg_hospital_unfulfilled
ORDER BY unfulfilled_packs DESC, hospital_name, generic_name
LIMIT 15;
