-- question: Q2 Channel revenue split and gross margin (Retail vs Hospital vs E-commerce)
-- widget: Channel x region revenue + margin matrix (heatmap)

-- @variant: before  (raw star-schema join on the fact table)
SELECT g.region, c.channel_name,
       sum(f.net_sales_usd) AS net_sales_usd,
       round(100.0 * sum(f.gross_margin_usd) / nullif(sum(f.net_sales_usd), 0), 2) AS gross_margin_pct
FROM fact_sales f
JOIN dim_geography g ON g.geography_key = f.geography_key
JOIN dim_channel c ON c.channel_key = f.channel_key
GROUP BY ALL
ORDER BY g.region, c.channel_name;

-- @variant: after   (pre-aggregated summary table)
SELECT region, channel_name,
       sum(net_sales_usd) AS net_sales_usd,
       round(100.0 * sum(gross_margin_usd) / nullif(sum(net_sales_usd), 0), 2) AS gross_margin_pct
FROM agg_sales_month_channel_region
GROUP BY ALL
ORDER BY region, channel_name;
