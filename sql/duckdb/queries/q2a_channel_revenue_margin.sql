-- question: Q2 Channel revenue split and gross margin (Retail vs Hospital vs E-commerce)
-- widget: Revenue share + gross margin % by channel (donut / KPI)

-- @variant: before  (raw star-schema join on the fact table)
SELECT c.channel_name,
       count(*) AS txn_count,
       sum(f.net_sales_usd) AS net_sales_usd,
       round(100.0 * sum(f.net_sales_usd) / sum(sum(f.net_sales_usd)) OVER (), 2) AS revenue_share_pct,
       sum(f.gross_margin_usd) AS gross_margin_usd,
       round(100.0 * sum(f.gross_margin_usd) / nullif(sum(f.net_sales_usd), 0), 2) AS gross_margin_pct
FROM fact_sales f
JOIN dim_channel c ON c.channel_key = f.channel_key
GROUP BY c.channel_name
ORDER BY net_sales_usd DESC;

-- @variant: after   (pre-aggregated summary table)
SELECT channel_name,
       sum(txn_count) AS txn_count,
       sum(net_sales_usd) AS net_sales_usd,
       round(100.0 * sum(net_sales_usd) / sum(sum(net_sales_usd)) OVER (), 2) AS revenue_share_pct,
       sum(gross_margin_usd) AS gross_margin_usd,
       round(100.0 * sum(gross_margin_usd) / nullif(sum(net_sales_usd), 0), 2) AS gross_margin_pct
FROM agg_sales_month_channel_region
GROUP BY channel_name
ORDER BY net_sales_usd DESC;
