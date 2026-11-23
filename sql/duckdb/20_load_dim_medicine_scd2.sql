-- Dim_Medicine, SCD Type 2 on price / cost / product status; Type 1 on descriptive attributes.
-- Input: stage_master_step = ONE version per medicine (one EffectiveFromDate), prepared by the runner.
INSERT INTO dim_medicine (medicine_key, medicine_id, generic_name, therapeutic_class, cold_chain_flag,
                          valid_from, valid_to, is_current, scd2_hash)
SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', DATE '1900-01-01', DATE '9999-12-31', TRUE, ''
WHERE NOT EXISTS (SELECT 1 FROM dim_medicine WHERE medicine_key = -1);

CREATE OR REPLACE TEMP TABLE m_new AS
SELECT s.*,
       md5(concat_ws('|', CAST(unit_manufacturing_cost_usd AS VARCHAR), CAST(wholesale_price_usd AS VARCHAR),
                     CAST(recommended_retail_price_usd AS VARCHAR), product_status)) AS scd2_hash
FROM stage_master_step s;

-- Type 1: overwrite descriptive attributes on every version of the medicine
UPDATE dim_medicine AS d SET
    internal_sku = n.internal_sku, gtin14 = n.gtin14, generic_name = n.generic_name,
    brand_name = n.fictional_brand_name, disease_category = n.disease_category,
    icd10_category = n.icd10_category, atc_code = n.atc_code, therapeutic_class = n.therapeutic_class,
    dosage_form = n.dosage_form, strength = n.strength, route_of_administration = n.route_of_administration,
    pack_size = n.pack_size, prescription_class = n.prescription_class,
    controlled_medicine_flag = n.controlled_medicine_flag, cold_chain_flag = n.cold_chain_flag,
    storage_min_temp_c = n.storage_min_temp_c, storage_max_temp_c = n.storage_max_temp_c,
    shelf_life_months = n.shelf_life_months, primary_supplier_id = n.primary_supplier_id,
    primary_plant_id = n.primary_plant_id
FROM m_new n
WHERE d.medicine_id = n.medicine_id;

-- Type 2: close the current version when a tracked attribute changed
UPDATE dim_medicine AS d SET
    valid_to = CAST(n.effective_from_date - INTERVAL 1 DAY AS DATE),
    is_current = FALSE
FROM m_new n
WHERE d.medicine_id = n.medicine_id AND d.is_current
  AND d.scd2_hash <> n.scd2_hash AND n.effective_from_date > d.valid_from;

-- Insert a new current version for new medicines and for the ones just closed
INSERT INTO dim_medicine
SELECT nextval('seq_medicine_key'), n.medicine_id, n.internal_sku, n.gtin14, n.generic_name,
       n.fictional_brand_name, n.disease_category, n.icd10_category, n.atc_code, n.therapeutic_class,
       n.dosage_form, n.strength, n.route_of_administration, n.pack_size, n.prescription_class,
       n.controlled_medicine_flag, n.cold_chain_flag, n.storage_min_temp_c, n.storage_max_temp_c,
       n.shelf_life_months, n.primary_supplier_id, n.primary_plant_id,
       n.unit_manufacturing_cost_usd, n.wholesale_price_usd, n.recommended_retail_price_usd,
       n.product_status, n.scd2_hash, n.effective_from_date, DATE '9999-12-31', TRUE,
       n.batch_id, current_timestamp
FROM m_new n
WHERE NOT EXISTS (SELECT 1 FROM dim_medicine d WHERE d.medicine_id = n.medicine_id AND d.is_current)
ORDER BY n.medicine_id;
