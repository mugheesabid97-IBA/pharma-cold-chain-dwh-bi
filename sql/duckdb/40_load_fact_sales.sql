-- Fact_Sales (transaction grain). Only staging rows newer than the watermark (stage_inc_sales);
-- the anti-join on the natural key makes a re-run add zero rows. Rows are appended in date order
-- so DuckDB zone-maps can skip blocks on date filters.
INSERT INTO fact_sales
SELECT s.sales_transaction_id,
       coalesce(ds.date_key, -1),
       coalesce(dd.date_key, -1),
       coalesce(de.date_key, -1),
       coalesce(m_pit.medicine_key, m_cur.medicine_key, -1),
       coalesce(ch.channel_key, -1),
       coalesce(ph.pharmacy_key, -1),
       coalesce(h.hospital_key, -1),
       coalesce(g.geography_key, -1),
       coalesce(cs.customer_segment_key, -1),
       coalesce(w.warehouse_key, -1),
       coalesce(j.sales_junk_key, -1),
       s.quantity_sold_packs, s.returned_quantity_packs, s.unit_retail_price_usd,
       s.gross_sales_amount_usd, s.discount_amount_usd, s.net_sales_amount_usd, s.sales_tax_usd,
       s.cost_of_goods_sold_usd, s.gross_margin_usd, s.days_to_expiry_at_sale,
       s.ingest_mode, s.stg_id, s.batch_id, current_timestamp
FROM stage_inc_sales s
LEFT JOIN dim_date ds ON ds.full_date = s.sales_date
LEFT JOIN dim_date dd ON dd.full_date = s.pharmacy_delivery_date
LEFT JOIN dim_date de ON de.full_date = s.batch_expiry_date
-- point-in-time SCD2 lookup: the medicine version valid on the sale date
LEFT JOIN dim_medicine m_pit ON m_pit.medicine_id = s.medicine_id
      AND s.sales_date BETWEEN m_pit.valid_from AND m_pit.valid_to
LEFT JOIN dim_medicine m_cur ON m_cur.medicine_id = s.medicine_id AND m_cur.is_current
LEFT JOIN dim_channel ch ON ch.channel_name = CASE
        WHEN s.sales_channel = 'Ecommerce' THEN 'E-commerce'
        WHEN s.pharmacy_type = 'Hospital Pharmacy' THEN 'Hospital'
        WHEN s.sales_channel = 'Physical Pharmacy' THEN 'Retail' END
LEFT JOIN dim_pharmacy ph ON ph.pharmacy_id = s.pharmacy_id
LEFT JOIN dim_hospital h ON h.hospital_id = s.hospital_id
LEFT JOIN dim_geography g ON g.city = s.pharmacy_city AND g.state = s.pharmacy_state
      AND g.country_code = s.country_code
LEFT JOIN dim_customer_segment cs ON cs.customer_segment = coalesce(s.customer_segment, 'Unknown')
      AND cs.payer_type = coalesce(s.payer_type, 'Unknown')
LEFT JOIN dim_warehouse w ON w.warehouse_id = s.source_warehouse_id
LEFT JOIN dim_sales_junk j ON j.payment_method = coalesce(s.payment_method, 'None')
      AND j.prescription_required_flag = coalesce(s.prescription_required_flag, 'N/A')
      AND j.return_flag = coalesce(s.return_flag, 'N/A')
      AND j.sale_status = coalesce(s.sale_status, 'N/A')
      AND j.ecommerce_delivery_status = coalesce(s.ecommerce_delivery_status, 'N/A')
      AND j.delivery_otif_flag = coalesce(s.delivery_otif_flag, 'N/A')
      AND j.promotion_code = coalesce(s.promotion_code, 'None')
WHERE NOT EXISTS (SELECT 1 FROM fact_sales f WHERE f.sales_transaction_id = s.sales_transaction_id)
ORDER BY coalesce(ds.date_key, -1), s.sales_transaction_id;
