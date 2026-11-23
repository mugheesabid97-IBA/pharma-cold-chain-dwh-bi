-- question: Q3 Top 10 fast-moving vs slow-moving medicines by therapeutic class
-- widget: Top 10 fast-moving medicines (horizontal bar)

-- @variant: before  (raw star-schema join on the fact table)
SELECT mc.generic_name, mc.therapeutic_class, mc.cold_chain_flag,
       sum(f.quantity_sold_packs) AS total_qty_packs,
       round(sum(f.quantity_sold_packs) / count(DISTINCT f.sale_date_key // 100), 2) AS avg_monthly_qty
FROM fact_sales f
JOIN dim_medicine m ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY ALL
ORDER BY total_qty_packs DESC, mc.generic_name
LIMIT 10;

-- @variant: after   (pre-aggregated summary table)
SELECT generic_name, therapeutic_class, cold_chain_flag, total_qty_packs, avg_monthly_qty
FROM agg_medicine_velocity
ORDER BY total_qty_packs DESC, generic_name
LIMIT 10;
