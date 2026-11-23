-- question: Q2 Channel revenue split and gross margin (Retail vs Hospital vs E-commerce)
-- widget: Monthly net revenue by channel (stacked columns)

-- @variant: before  (raw star-schema join on the fact table)
SELECT d.year_month, c.channel_name,
       sum(f.net_sales_usd) AS net_sales_usd,
       sum(f.gross_margin_usd) AS gross_margin_usd,
       round(100.0 * sum(f.gross_margin_usd) / nullif(sum(f.net_sales_usd), 0), 2) AS gross_margin_pct
FROM fact_sales f
JOIN dim_date d ON d.date_key = f.sale_date_key
JOIN dim_channel c ON c.channel_key = f.channel_key
GROUP BY ALL
ORDER BY d.year_month, c.channel_name;

-- @variant: after   (pre-aggregated summary table)
SELECT year_month, channel_name,
       sum(net_sales_usd) AS net_sales_usd,
       sum(gross_margin_usd) AS gross_margin_usd,
       round(100.0 * sum(gross_margin_usd) / nullif(sum(net_sales_usd), 0), 2) AS gross_margin_pct
FROM agg_sales_month_channel_region
GROUP BY ALL
ORDER BY year_month, channel_name;
