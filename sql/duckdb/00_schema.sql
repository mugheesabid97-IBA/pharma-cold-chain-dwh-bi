-- =====================================================================
-- SDI (Sales & Demand Intelligence) star schema, DuckDB: warehouse/sdi_mart.duckdb
-- Idempotent DDL: every object is CREATE ... IF NOT EXISTS. The mart is never emptied.
-- =====================================================================

CREATE SEQUENCE IF NOT EXISTS seq_medicine_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_hospital_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_pharmacy_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_geography_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_customer_segment_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_warehouse_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_sales_junk_key START 1;
CREATE SEQUENCE IF NOT EXISTS seq_demand_junk_key START 1;

-- Incremental-load bookkeeping: last staging stg_id processed per source
CREATE TABLE IF NOT EXISTS etl_watermark (
    source        VARCHAR PRIMARY KEY,
    last_stg_id   BIGINT NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);

-- ---------------------------------------------------------------- Dim_Date (role-playing)
CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,      -- yyyymmdd, -1 = unknown
    full_date       DATE,
    day_of_month    INTEGER,
    day_of_week     INTEGER,                  -- ISO 1 = Monday
    day_name        VARCHAR,
    is_weekend      BOOLEAN,
    week_of_year    INTEGER,
    month_no        INTEGER,
    month_name      VARCHAR,
    quarter_no      INTEGER,
    year_no         INTEGER,
    year_month      VARCHAR,                  -- '2025-12'
    month_start     DATE
);

-- ---------------------------------------------------------------- Dim_Medicine (conformed, SCD2)
CREATE TABLE IF NOT EXISTS dim_medicine (
    medicine_key                INTEGER PRIMARY KEY,
    medicine_id                 VARCHAR NOT NULL,     -- natural/business key
    internal_sku                VARCHAR,
    gtin14                      VARCHAR,
    generic_name                VARCHAR,
    brand_name                  VARCHAR,
    disease_category            VARCHAR,
    icd10_category              VARCHAR,
    atc_code                    VARCHAR,
    therapeutic_class           VARCHAR,
    dosage_form                 VARCHAR,
    strength                    VARCHAR,
    route_of_administration     VARCHAR,
    pack_size                   INTEGER,
    prescription_class          VARCHAR,
    controlled_medicine_flag    VARCHAR,
    cold_chain_flag             VARCHAR,
    storage_min_temp_c          DECIMAL(10,2),
    storage_max_temp_c          DECIMAL(10,2),
    shelf_life_months           INTEGER,
    primary_supplier_id         VARCHAR,
    primary_plant_id            VARCHAR,
    -- SCD Type 2 tracked attributes
    unit_manufacturing_cost_usd DECIMAL(12,4),
    wholesale_price_usd         DECIMAL(12,4),
    recommended_retail_price_usd DECIMAL(12,4),
    product_status              VARCHAR,
    -- SCD2 housekeeping
    scd2_hash                   VARCHAR,
    valid_from                  DATE NOT NULL,
    valid_to                    DATE NOT NULL,
    is_current                  BOOLEAN NOT NULL,
    source_batch_id             VARCHAR,
    loaded_at                   TIMESTAMP
);

-- ---------------------------------------------------------------- other dimensions
CREATE TABLE IF NOT EXISTS dim_channel (
    channel_key         INTEGER PRIMARY KEY,
    channel_name        VARCHAR NOT NULL,     -- Retail | Hospital | E-commerce
    channel_rule        VARCHAR,
    description         VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_geography (
    geography_key   INTEGER PRIMARY KEY,
    city            VARCHAR,
    state           VARCHAR,
    country_code    VARCHAR,
    region          VARCHAR                   -- US Census region derived from state
);

CREATE TABLE IF NOT EXISTS dim_hospital (
    hospital_key            INTEGER PRIMARY KEY,
    hospital_id             VARCHAR NOT NULL,
    hospital_gln            VARCHAR,
    hospital_name           VARCHAR,
    hospital_type           VARCHAR,
    hospital_pharmacy_id    VARCHAR,
    city                    VARCHAR,
    state                   VARCHAR,
    region                  VARCHAR,
    licensed_beds           INTEGER,
    icu_beds                INTEGER,
    trauma_level            VARCHAR,
    catchment_population    BIGINT,
    loaded_at               TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_pharmacy (
    pharmacy_key        INTEGER PRIMARY KEY,
    pharmacy_id         VARCHAR NOT NULL,
    pharmacy_gln        VARCHAR,
    pharmacy_name       VARCHAR,
    pharmacy_type       VARCHAR,
    city                VARCHAR,
    state               VARCHAR,
    region              VARCHAR,
    loaded_at           TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_customer_segment (
    customer_segment_key    INTEGER PRIMARY KEY,
    customer_segment        VARCHAR NOT NULL,
    payer_type              VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_warehouse (
    warehouse_key   INTEGER PRIMARY KEY,
    warehouse_id    VARCHAR NOT NULL          -- conformed with Phase 2 (warehouse attributes added then)
);

-- Junk dimension: low-cardinality flags of a sales transaction
CREATE TABLE IF NOT EXISTS dim_sales_junk (
    sales_junk_key              INTEGER PRIMARY KEY,
    payment_method              VARCHAR,
    prescription_required_flag  VARCHAR,
    return_flag                 VARCHAR,
    sale_status                 VARCHAR,
    ecommerce_delivery_status   VARCHAR,
    delivery_otif_flag          VARCHAR,
    promotion_code              VARCHAR
);

-- Junk dimension: hospital request / formulary status
CREATE TABLE IF NOT EXISTS dim_demand_junk (
    demand_junk_key             INTEGER PRIMARY KEY,
    medication_request_status   VARCHAR,
    formulary_status            VARCHAR
);

-- ---------------------------------------------------------------- facts
-- Transaction grain: one row per pharmacy sales transaction
CREATE TABLE IF NOT EXISTS fact_sales (
    sales_transaction_id    VARCHAR NOT NULL,   -- degenerate dimension (natural key)
    sale_date_key           INTEGER NOT NULL,   -- role: sale date
    delivery_date_key       INTEGER NOT NULL,   -- role: pharmacy delivery date
    expiry_date_key         INTEGER NOT NULL,   -- role: batch expiry date
    medicine_key            INTEGER NOT NULL,
    channel_key             INTEGER NOT NULL,
    pharmacy_key            INTEGER NOT NULL,
    hospital_key            INTEGER NOT NULL,
    geography_key           INTEGER NOT NULL,
    customer_segment_key    INTEGER NOT NULL,
    warehouse_key           INTEGER NOT NULL,
    sales_junk_key          INTEGER NOT NULL,
    quantity_sold_packs     INTEGER,
    returned_quantity_packs INTEGER,
    unit_retail_price_usd   DECIMAL(12,4),
    gross_sales_usd         DECIMAL(14,4),
    discount_usd            DECIMAL(14,4),
    net_sales_usd           DECIMAL(14,4),
    sales_tax_usd           DECIMAL(14,4),
    cogs_usd                DECIMAL(14,4),
    gross_margin_usd        DECIMAL(14,4),
    days_to_expiry_at_sale  INTEGER,
    ingest_mode             VARCHAR,            -- batch | stream
    stg_id                  BIGINT,
    batch_id                VARCHAR,
    loaded_at               TIMESTAMP
);

-- Periodic snapshot: hospital x medicine x month
CREATE TABLE IF NOT EXISTS fact_hospital_demand (
    hospital_demand_record_id   VARCHAR NOT NULL,
    month_date_key              INTEGER NOT NULL,
    hospital_key                INTEGER NOT NULL,
    medicine_key                INTEGER NOT NULL,
    demand_junk_key             INTEGER NOT NULL,
    estimated_disease_cases     INTEGER,
    disease_related_admissions  INTEGER,
    outpatient_encounters       INTEGER,
    forecast_demand_packs       INTEGER,
    requested_qty_packs         INTEGER,
    approved_qty_packs          INTEGER,
    received_qty_packs          INTEGER,
    emergency_request_packs     INTEGER,
    opening_stock_packs         INTEGER,
    dispensed_qty_packs         INTEGER,
    consumed_qty_packs          INTEGER,
    wastage_qty_packs           INTEGER,
    closing_stock_packs         INTEGER,
    unfulfilled_demand_packs    INTEGER,
    stockout_days               INTEGER,
    service_level_pct           DECIMAL(8,3),
    forecast_vs_actual_ratio    DECIMAL(10,4),  -- forecast / consumed (NULL when consumed = 0)
    forecast_error_packs        INTEGER,        -- forecast - consumed
    demand_trend_index          DECIMAL(10,4),
    stg_id                      BIGINT,
    batch_id                    VARCHAR,
    loaded_at                   TIMESTAMP
);

-- Role-playing views on dim_date
CREATE OR REPLACE VIEW dim_sale_date     AS SELECT * FROM dim_date;
CREATE OR REPLACE VIEW dim_delivery_date AS SELECT * FROM dim_date;
CREATE OR REPLACE VIEW dim_expiry_date   AS SELECT * FROM dim_date;
CREATE OR REPLACE VIEW dim_demand_month  AS SELECT * FROM dim_date WHERE day_of_month = 1 OR date_key = -1;

-- US Census region from a state code (used by geography / pharmacy / hospital dims)
CREATE OR REPLACE MACRO us_region(st) AS CASE
    WHEN st IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'Northeast'
    WHEN st IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'Midwest'
    WHEN st IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'South'
    WHEN st IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'West'
    ELSE 'Unknown' END;
