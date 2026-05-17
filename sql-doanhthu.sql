CREATE SCHEMA IF NOT EXISTS dw;

-- =========================
-- DIMENSIONS
-- =========================

CREATE TABLE dw.dim_date (
    full_date      DATE PRIMARY KEY,
    year           INT NOT NULL,
    month          INT NOT NULL,
    month_name     VARCHAR(20),
    quarter        INT,
    year_month     VARCHAR(7)
);

CREATE TABLE dw.dim_region (
    region_name    VARCHAR(50) PRIMARY KEY
);

CREATE TABLE dw.dim_branch (
    branch_name    VARCHAR(100) PRIMARY KEY,
    region_name    VARCHAR(50)
);

CREATE TABLE dw.dim_product (
    product_name   VARCHAR(150) PRIMARY KEY,
    category       VARCHAR(100)
);

CREATE TABLE dw.dim_channel (
    channel_name   VARCHAR(100) PRIMARY KEY
);

CREATE TABLE dw.dim_customer_segment (
    segment_name   VARCHAR(100) PRIMARY KEY
);

CREATE TABLE dw.dim_promotion (
    promotion_campaign VARCHAR(150) PRIMARY KEY
);

-- =========================
-- FACT TABLE
-- =========================

CREATE TABLE dw.fact_sales (
    order_id             BIGINT PRIMARY KEY,

    full_date            DATE NOT NULL REFERENCES dw.dim_date(full_date),
    region_name          VARCHAR(50) NOT NULL REFERENCES dw.dim_region(region_name),
    branch_name          VARCHAR(100) REFERENCES dw.dim_branch(branch_name),
    product_name         VARCHAR(150) REFERENCES dw.dim_product(product_name),
    channel_name         VARCHAR(100) REFERENCES dw.dim_channel(channel_name),
    segment_name         VARCHAR(100) REFERENCES dw.dim_customer_segment(segment_name),
    promotion_campaign   VARCHAR(150) REFERENCES dw.dim_promotion(promotion_campaign),

    quantity             NUMERIC(18,2),
    revenue              NUMERIC(18,2) NOT NULL,
    cost                 NUMERIC(18,2),
    profit               NUMERIC(18,2),

    market_size          NUMERIC(18,2),
    budget               NUMERIC(18,2),
    logistics_cost       NUMERIC(18,2),
    marketing_cost       NUMERIC(18,2)
);

DROP TABLE IF EXISTS dw.fact_sales;

DROP TABLE IF EXISTS dw.dim_promotion;
DROP TABLE IF EXISTS dw.dim_customer_segment;
DROP TABLE IF EXISTS dw.dim_channel;
DROP TABLE IF EXISTS dw.dim_product;
DROP TABLE IF EXISTS dw.dim_branch;
DROP TABLE IF EXISTS dw.dim_region;
DROP TABLE IF EXISTS dw.dim_date;

