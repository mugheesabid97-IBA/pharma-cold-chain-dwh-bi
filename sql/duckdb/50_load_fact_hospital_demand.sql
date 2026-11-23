-- Fact_Hospital_Demand (periodic snapshot: hospital x medicine x month). Incremental + idempotent.
INSERT INTO fact_hospital_demand
SELECT s.hospital_demand_record_id,
       coalesce(dm.date_key, -1),
       coalesce(h.hospital_key, -1),
       coalesce(m_pit.medicine_key, m_cur.medicine_key, -1),
       coalesce(j.demand_junk_key, -1),
       s.estimated_disease_cases, s.disease_related_admissions, s.outpatient_encounters,
       s.forecast_demand_packs, s.requested_quantity_packs, s.approved_quantity_packs,
       s.received_quantity_packs, s.emergency_request_quantity_packs, s.opening_stock_packs,
       s.dispensed_quantity_packs, s.consumed_quantity_packs, s.wastage_quantity_packs,
       s.closing_stock_packs, s.unfulfilled_demand_packs, s.stockout_days, s.service_level_pct,
       CASE WHEN s.consumed_quantity_packs > 0
            THEN round(s.forecast_demand_packs / s.consumed_quantity_packs, 4) END,
       s.forecast_demand_packs - s.consumed_quantity_packs,
       s.demand_trend_index, s.stg_id, s.batch_id, current_timestamp
FROM stage_inc_hospital s
LEFT JOIN dim_date dm ON dm.full_date = s.demand_month
LEFT JOIN dim_hospital h ON h.hospital_id = s.hospital_id
LEFT JOIN dim_medicine m_pit ON m_pit.medicine_id = s.medicine_id
      AND s.demand_month BETWEEN m_pit.valid_from AND m_pit.valid_to
LEFT JOIN dim_medicine m_cur ON m_cur.medicine_id = s.medicine_id AND m_cur.is_current
LEFT JOIN dim_demand_junk j ON j.medication_request_status = coalesce(s.medication_request_status, 'N/A')
      AND j.formulary_status = coalesce(s.formulary_status, 'N/A')
WHERE NOT EXISTS (SELECT 1 FROM fact_hospital_demand f
                  WHERE f.hospital_demand_record_id = s.hospital_demand_record_id)
ORDER BY coalesce(dm.date_key, -1), s.hospital_demand_record_id;
