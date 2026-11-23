-- question: Q3 Top 10 fast-moving vs slow-moving medicines by therapeutic class
-- widget: Velocity rank by therapeutic class, cold-chain vs ambient (table)

-- @variant: before  (raw star-schema join on the fact table)
SELECT mc.therapeutic_class, mc.cold_chain_flag,
       count(DISTINCT mc.medicine_id) AS medicines,
       sum(f.quantity_sold_packs) AS total_qty_packs,
       sum(f.net_sales_usd) AS net_sales_usd,
       rank() OVER (PARTITION BY mc.cold_chain_flag ORDER BY sum(f.quantity_sold_packs) DESC) AS velocity_rank
FROM fact_sales f
JOIN dim_medicine m ON m.medicine_key = f.medicine_key
JOIN dim_medicine mc ON mc.medicine_id = m.medicine_id AND mc.is_current
GROUP BY mc.therapeutic_class, mc.cold_chain_flag
ORDER BY mc.cold_chain_flag DESC, velocity_rank, mc.therapeutic_class;

-- @variant: after   (pre-aggregated summary table)
SELECT therapeutic_class, cold_chain_flag,
       count(*) AS medicines,
       sum(total_qty_packs) AS total_qty_packs,
       sum(total_net_sales_usd) AS net_sales_usd,
       rank() OVER (PARTITION BY cold_chain_flag ORDER BY sum(total_qty_packs) DESC) AS velocity_rank
FROM agg_medicine_velocity
GROUP BY therapeutic_class, cold_chain_flag
ORDER BY cold_chain_flag DESC, velocity_rank, therapeutic_class;
