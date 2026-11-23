-- Other dimensions. Input: stage_inc_sales / stage_inc_hospital (new staging rows only).
-- New members are inserted; descriptive attributes of existing members are overwritten (SCD Type 1).

-- ---------- unknown members (-1)
INSERT INTO dim_channel SELECT -1, 'Unknown', NULL, 'Unknown / unmapped channel'
WHERE NOT EXISTS (SELECT 1 FROM dim_channel WHERE channel_key = -1);
INSERT INTO dim_geography SELECT -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_geography WHERE geography_key = -1);
INSERT INTO dim_hospital (hospital_key, hospital_id, hospital_name, hospital_type, region)
SELECT -1, 'N/A', 'Not a hospital / unknown', 'N/A', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_hospital WHERE hospital_key = -1);
INSERT INTO dim_pharmacy (pharmacy_key, pharmacy_id, pharmacy_name, pharmacy_type, region)
SELECT -1, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy WHERE pharmacy_key = -1);
INSERT INTO dim_customer_segment SELECT -1, 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_customer_segment WHERE customer_segment_key = -1);
INSERT INTO dim_warehouse SELECT -1, 'UNKNOWN'
WHERE NOT EXISTS (SELECT 1 FROM dim_warehouse WHERE warehouse_key = -1);
INSERT INTO dim_sales_junk SELECT -1, 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_sales_junk WHERE sales_junk_key = -1);
INSERT INTO dim_demand_junk SELECT -1, 'Unknown', 'Unknown'
WHERE NOT EXISTS (SELECT 1 FROM dim_demand_junk WHERE demand_junk_key = -1);

-- ---------- Dim_Channel (static business rule: Retail | Hospital | E-commerce)
INSERT INTO dim_channel
SELECT * FROM (VALUES
    (1, 'Retail',     'SalesChannel = Physical Pharmacy AND PharmacyType <> Hospital Pharmacy', 'Chain + independent community pharmacies'),
    (2, 'Hospital',   'SalesChannel = Physical Pharmacy AND PharmacyType = Hospital Pharmacy',  'Hospital outpatient pharmacies'),
    (3, 'E-commerce', 'SalesChannel = Ecommerce',                                               'Online orders delivered to customers')
) v(k, n, r, d)
WHERE k NOT IN (SELECT channel_key FROM dim_channel);

-- ---------- Dim_Geography (pharmacy + hospital locations)
INSERT INTO dim_geography
SELECT nextval('seq_geography_key'), city, state, country_code, us_region(state)
FROM (
    SELECT DISTINCT pharmacy_city AS city, pharmacy_state AS state, country_code FROM stage_inc_sales
    UNION
    SELECT DISTINCT city, state, country_code FROM stage_inc_hospital
) g
WHERE NOT EXISTS (SELECT 1 FROM dim_geography d
                  WHERE d.city = g.city AND d.state = g.state AND d.country_code = g.country_code)
ORDER BY state, city;

-- ---------- Dim_Hospital (latest attributes per hospital in the increment)
CREATE OR REPLACE TEMP TABLE h_new AS
SELECT * FROM stage_inc_hospital
QUALIFY row_number() OVER (PARTITION BY hospital_id ORDER BY demand_month DESC, stg_id DESC) = 1;

UPDATE dim_hospital AS d SET
    hospital_gln = n.hospital_gln, hospital_name = n.hospital_name, hospital_type = n.hospital_type,
    hospital_pharmacy_id = n.hospital_pharmacy_id, city = n.city, state = n.state, region = us_region(n.state),
    licensed_beds = n.licensed_beds, icu_beds = n.icu_beds, trauma_level = n.trauma_level,
    catchment_population = n.catchment_population, loaded_at = current_timestamp
FROM h_new n WHERE d.hospital_id = n.hospital_id;

INSERT INTO dim_hospital
SELECT nextval('seq_hospital_key'), hospital_id, hospital_gln, hospital_name, hospital_type, hospital_pharmacy_id,
       city, state, us_region(state), licensed_beds, icu_beds, trauma_level, catchment_population, current_timestamp
FROM h_new n
WHERE NOT EXISTS (SELECT 1 FROM dim_hospital d WHERE d.hospital_id = n.hospital_id)
ORDER BY hospital_id;

-- ---------- Dim_Pharmacy
CREATE OR REPLACE TEMP TABLE p_new AS
SELECT pharmacy_id, pharmacy_gln, pharmacy_name, pharmacy_type, pharmacy_city, pharmacy_state
FROM stage_inc_sales
QUALIFY row_number() OVER (PARTITION BY pharmacy_id ORDER BY sales_date DESC, stg_id DESC) = 1;

UPDATE dim_pharmacy AS d SET
    pharmacy_gln = n.pharmacy_gln, pharmacy_name = n.pharmacy_name, pharmacy_type = n.pharmacy_type,
    city = n.pharmacy_city, state = n.pharmacy_state, region = us_region(n.pharmacy_state),
    loaded_at = current_timestamp
FROM p_new n WHERE d.pharmacy_id = n.pharmacy_id;

INSERT INTO dim_pharmacy
SELECT nextval('seq_pharmacy_key'), pharmacy_id, pharmacy_gln, pharmacy_name, pharmacy_type,
       pharmacy_city, pharmacy_state, us_region(pharmacy_state), current_timestamp
FROM p_new n
WHERE NOT EXISTS (SELECT 1 FROM dim_pharmacy d WHERE d.pharmacy_id = n.pharmacy_id)
ORDER BY pharmacy_id;

-- ---------- Dim_Customer_Segment (customer segment x payer type)
INSERT INTO dim_customer_segment
SELECT nextval('seq_customer_segment_key'), customer_segment, payer_type
FROM (SELECT DISTINCT coalesce(customer_segment, 'Unknown') AS customer_segment,
                      coalesce(payer_type, 'Unknown') AS payer_type FROM stage_inc_sales) c
WHERE NOT EXISTS (SELECT 1 FROM dim_customer_segment d
                  WHERE d.customer_segment = c.customer_segment AND d.payer_type = c.payer_type)
ORDER BY customer_segment, payer_type;

-- ---------- Dim_Warehouse (conformed; attributes added in Phase 2)
INSERT INTO dim_warehouse
SELECT nextval('seq_warehouse_key'), source_warehouse_id
FROM (SELECT DISTINCT source_warehouse_id FROM stage_inc_sales WHERE source_warehouse_id IS NOT NULL) w
WHERE source_warehouse_id NOT IN (SELECT warehouse_id FROM dim_warehouse)
ORDER BY source_warehouse_id;

-- ---------- Junk dimensions (only the flag combinations that actually occur)
INSERT INTO dim_sales_junk
SELECT nextval('seq_sales_junk_key'), pm, rx, rf, ss, eds, otif, promo
FROM (SELECT DISTINCT coalesce(payment_method, 'None') AS pm, coalesce(prescription_required_flag, 'N/A') AS rx,
             coalesce(return_flag, 'N/A') AS rf, coalesce(sale_status, 'N/A') AS ss,
             coalesce(ecommerce_delivery_status, 'N/A') AS eds, coalesce(delivery_otif_flag, 'N/A') AS otif,
             coalesce(promotion_code, 'None') AS promo
      FROM stage_inc_sales) j
WHERE NOT EXISTS (SELECT 1 FROM dim_sales_junk d
                  WHERE d.payment_method = j.pm AND d.prescription_required_flag = j.rx AND d.return_flag = j.rf
                    AND d.sale_status = j.ss AND d.ecommerce_delivery_status = j.eds
                    AND d.delivery_otif_flag = j.otif AND d.promotion_code = j.promo)
ORDER BY pm, rx, rf, ss, eds, otif, promo;

INSERT INTO dim_demand_junk
SELECT nextval('seq_demand_junk_key'), mrs, fs
FROM (SELECT DISTINCT coalesce(medication_request_status, 'N/A') AS mrs, coalesce(formulary_status, 'N/A') AS fs
      FROM stage_inc_hospital) j
WHERE NOT EXISTS (SELECT 1 FROM dim_demand_junk d
                  WHERE d.medication_request_status = j.mrs AND d.formulary_status = j.fs)
ORDER BY mrs, fs;
