CREATE SCHEMA IF NOT EXISTS dw;

DROP TABLE IF EXISTS dw.fact_marketing_sales;
DROP TABLE IF EXISTS dw.dim_information;
DROP TABLE IF EXISTS dw.dim_machine;
DROP TABLE IF EXISTS dw.dim_department;
DROP TABLE IF EXISTS dw.dim_campaign;
DROP TABLE IF EXISTS dw.dim_customer_segment;
DROP TABLE IF EXISTS dw.dim_channel;
DROP TABLE IF EXISTS dw.dim_stage;
DROP TABLE IF EXISTS dw.dim_date;

-- ============================================================
-- DIMENSIONS
-- ============================================================

CREATE TABLE dw.dim_date (
    full_date      DATE PRIMARY KEY,
    year           INT NOT NULL,
    month          INT NOT NULL,
    month_name     VARCHAR(20),
    quarter        INT,
    year_month     VARCHAR(7)
);


CREATE TABLE dw.dim_stage (
    stage_name     VARCHAR(50) PRIMARY KEY,
    stage_order    INT NOT NULL,
    stage_group    VARCHAR(50)
);

INSERT INTO dw.dim_stage (stage_name, stage_order, stage_group)
VALUES
    ('Impression', 1, 'Awareness'),
    ('Visit website', 2, 'Consideration'),
    ('Click', 3, 'Consideration'),
    ('Add to cart', 4, 'Intent'),
    ('Purchase', 5, 'Conversion')
ON CONFLICT (stage_name) DO UPDATE SET
    stage_order = EXCLUDED.stage_order,
    stage_group = EXCLUDED.stage_group;

CREATE TABLE dw.dim_channel (
    channel_name   VARCHAR(100) PRIMARY KEY,
    channel_type   VARCHAR(50)
);

CREATE TABLE dw.dim_customer_segment (
    segment_name   VARCHAR(100) PRIMARY KEY
);

CREATE TABLE dw.dim_campaign (
    campaign_name  VARCHAR(150) PRIMARY KEY,
    campaign_type  VARCHAR(100)
);

CREATE TABLE dw.dim_department (
    department_name VARCHAR(100) PRIMARY KEY
);

CREATE TABLE dw.dim_machine (
    machine_name   VARCHAR(50) PRIMARY KEY
);

CREATE TABLE dw.dim_information (
    information_name VARCHAR(150) PRIMARY KEY
);

-- ============================================================
-- FACT TABLE
-- ============================================================

CREATE TABLE dw.fact_marketing_sales (
    order_id               BIGINT PRIMARY KEY,

    full_date              DATE REFERENCES dw.dim_date(full_date),
    stage_name             VARCHAR(50) REFERENCES dw.dim_stage(stage_name),
    channel_name           VARCHAR(100) REFERENCES dw.dim_channel(channel_name),
    information_name       VARCHAR(150) REFERENCES dw.dim_information(information_name),
    machine_name           VARCHAR(50) REFERENCES dw.dim_machine(machine_name),
    department_name        VARCHAR(100) REFERENCES dw.dim_department(department_name),
    segment_name           VARCHAR(100) REFERENCES dw.dim_customer_segment(segment_name),
    campaign_name          VARCHAR(150) REFERENCES dw.dim_campaign(campaign_name),

    -- Core sales metrics.
    quantity               NUMERIC(18,2),
    revenue                NUMERIC(18,2) NOT NULL,
    cost                   NUMERIC(18,2),
    profit                 NUMERIC(18,2),
    profit_margin          NUMERIC(18,6),

    -- Operational and funnel proxy metrics.
    inventory_level        NUMERIC(18,2),
    calls                  NUMERIC(18,2),
    waiting_time           NUMERIC(18,2),
    conversion_proxy       NUMERIC(18,6),

    -- Budget, target, and marketing effectiveness metrics.
    budget                 NUMERIC(18,2),
    target                 NUMERIC(18,2),
    marketing_cost         NUMERIC(18,2),
    roi_marketing          NUMERIC(18,6),

    -- Cost breakdown retained from source for richer analysis.
    raw_material_cost      NUMERIC(18,2),
    labor_cost             NUMERIC(18,2),
    logistics_cost         NUMERIC(18,2),
    unit_cost              NUMERIC(18,6),
    revenue_per_unit       NUMERIC(18,6),
    cost_per_machine_unit  NUMERIC(18,6),

    -- Market metrics.
    market_size            NUMERIC(18,2),
    market_share           NUMERIC(18,6),
    region_profit_share    NUMERIC(18,6),

    -- Calculated columns for dashboard logic.
    target_gap             NUMERIC(18,2)
        GENERATED ALWAYS AS (COALESCE(revenue, 0) - COALESCE(target, 0)) STORED,
    target_achievement_rate NUMERIC(18,6)
        GENERATED ALWAYS AS (
            CASE
                WHEN target IS NULL OR target = 0 THEN NULL
                ELSE revenue / target
            END
        ) STORED,
    roi_marketing_calc     NUMERIC(18,6)
        GENERATED ALWAYS AS (
            CASE
                WHEN budget IS NULL OR budget = 0 THEN NULL
                ELSE profit / budget
            END
        ) STORED,

    created_at             TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

s
