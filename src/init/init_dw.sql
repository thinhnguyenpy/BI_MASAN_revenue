-- ==============================================================================
-- FILE: init_gold_datawarehouse.sql
-- MÔ TẢ: Khởi tạo toàn bộ schema Gold (Data Warehouse) cho dự án BI_MASAN
-- NGUỒN DỮ LIỆU:
--   - PostgreSQL (sales_db)  : orders, order_details, products, categories, branches
--   - MySQL (marketing_db)   : marketing_campaigns, daily_marketing_spend, monthly_budgets
--   - MongoDB (production_db): ProductionLogs, Departments
-- HƯỚNG DẪN:
--   1. Kết nối vào PostgreSQL instance chứa Data Warehouse
--   2. Chạy toàn bộ file này: psql -U <user> -d <dbname> -f init_gold_datawarehouse.sql
--   3. Chạy 1 lần duy nhất khi setup môi trường mới
--   4. Nếu cần reset: Chạy các lệnh DROP TABLE ở cuối file rồi chạy lại từ đầu
-- ==============================================================================

-- ==============================================================================
-- BƯỚC 0: TẠO SCHEMA
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS gold;

-- Đặt search_path để không cần prefix gold. trong suốt file này
SET search_path TO gold, public;


-- ==============================================================================
-- BƯỚC 1: DIMENSION TABLES
-- Thứ tự tạo quan trọng vì Fact Tables sẽ REFERENCES tới các bảng này
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 1.1 DIM_DATE
-- Smart Key: YYYYMMDD (vd: 20260611) thay vì SERIAL để dễ đọc và join
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INT PRIMARY KEY,
    full_date    DATE NOT NULL,
    day_of_week  VARCHAR(20),
    day_of_month INT,
    month_number INT,
    month_name   VARCHAR(20),
    quarter      INT,
    year         INT
);

-- ------------------------------------------------------------------------------
-- 1.2 DIM_PRODUCT (SCD Type 2)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key   SERIAL PRIMARY KEY,
    product_id    VARCHAR(50) NOT NULL,
    product_name  VARCHAR(255),
    category_name VARCHAR(255),
    start_date    DATE NOT NULL,
    end_date      DATE,
    is_current    SMALLINT DEFAULT 1
);

-- ------------------------------------------------------------------------------
-- 1.3 DIM_BRANCH
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_branch (
    branch_key  SERIAL PRIMARY KEY,
    branch_id   VARCHAR(50) NOT NULL,
    branch_name VARCHAR(255),
    region      VARCHAR(100)
);

-- ------------------------------------------------------------------------------
-- 1.4 DIM_DEPARTMENT
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_department (
    department_key  SERIAL PRIMARY KEY,
    department_id   INT NOT NULL,
    department_name VARCHAR(255)
);

-- ------------------------------------------------------------------------------
-- 1.5 DIM_CAMPAIGN
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_campaign (
    campaign_key  SERIAL PRIMARY KEY,
    campaign_id   INT NOT NULL,         -- Đã bỏ UNIQUE ở đây để cấu hình dưới Bước 3
    campaign_name VARCHAR(255),
    platform      VARCHAR(100)
);


-- ==============================================================================
-- BƯỚC 2: FACT TABLES
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 2.1 FACT_SALES
-- Granularity: 1 dòng = 1 sản phẩm trong 1 đơn hàng
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_key        SERIAL PRIMARY KEY,
    date_key         INT REFERENCES gold.dim_date(date_key),
    product_key      INT REFERENCES gold.dim_product(product_key),
    branch_key       INT REFERENCES gold.dim_branch(branch_key),
    order_id         BIGINT NOT NULL,
    sales_channel    VARCHAR(100),
    customer_segment VARCHAR(100),
    quantity         DECIMAL(18,2) DEFAULT 0,
    unit_price       DECIMAL(18,2) DEFAULT 0,
    unit_cost        DECIMAL(18,2) DEFAULT 0,
    revenue          DECIMAL(18,2) DEFAULT 0,
    total_cost       DECIMAL(18,2) DEFAULT 0,
    profit           DECIMAL(18,2) DEFAULT 0
);

-- ------------------------------------------------------------------------------
-- 2.2 FACT_MARKETING_SPEND
-- Granularity: 1 dòng = 1 chiến dịch trong 1 ngày tại 1 khu vực
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_marketing_spend (
    spend_key    SERIAL PRIMARY KEY,
    date_key     INT REFERENCES gold.dim_date(date_key),
    campaign_key INT REFERENCES gold.dim_campaign(campaign_key),
    region       VARCHAR(100),
    daily_spend  DECIMAL(18,2) DEFAULT 0
);

-- ------------------------------------------------------------------------------
-- 2.3 FACT_MONTHLY_BUDGET
-- Granularity: 1 dòng = 1 khu vực trong 1 tháng
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_monthly_budget (
    budget_key     SERIAL PRIMARY KEY,
    date_key       INT REFERENCES gold.dim_date(date_key),
    region         VARCHAR(100),
    budget_amount  DECIMAL(18,2),
    target_revenue DECIMAL(18,2),
    market_size    DECIMAL(18,2)
);

-- ------------------------------------------------------------------------------
-- 2.4 FACT_PRODUCTION_LOGS
-- Granularity: 1 dòng = 1 máy + 1 sản phẩm + 1 ngày
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_production_logs (
    log_key           SERIAL PRIMARY KEY,
    date_key          INT REFERENCES gold.dim_date(date_key),
    product_key       INT REFERENCES gold.dim_product(product_key),
    department_key    INT REFERENCES gold.dim_department(department_key),
    machine_id        VARCHAR(100),
    inventory_level   DECIMAL(18,2) DEFAULT 0,
    raw_material_cost DECIMAL(18,2) DEFAULT 0,
    labor_cost        DECIMAL(18,2) DEFAULT 0
);

-- ------------------------------------------------------------------------------
-- 2.5 FACT_LOGISTICS_COSTS
-- Granularity: 1 dòng = 1 chi nhánh trong 1 ngày
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_logistics_costs (
    logistics_key  SERIAL PRIMARY KEY,
    date_key       INT REFERENCES gold.dim_date(date_key),
    branch_key     INT REFERENCES gold.dim_branch(branch_key),
    logistics_cost DECIMAL(18,2) DEFAULT 0
);


-- ==============================================================================
-- BƯỚC 3: UNIQUE CONSTRAINTS (Dành cho chức năng UPSERT của Spark)
-- ==============================================================================
ALTER TABLE gold.dim_branch
    ADD CONSTRAINT uq_dim_branch_id  UNIQUE (branch_id);

ALTER TABLE gold.dim_product
    ADD CONSTRAINT uq_dim_product_id UNIQUE (product_id);

ALTER TABLE gold.dim_department
    ADD CONSTRAINT uq_dim_department_id UNIQUE (department_id);

ALTER TABLE gold.dim_campaign
    ADD CONSTRAINT uq_dim_campaign_id UNIQUE (campaign_id);

ALTER TABLE gold.fact_sales
    ADD CONSTRAINT uq_fact_sales_pk  UNIQUE (order_id, product_key);

ALTER TABLE gold.fact_marketing_spend
    ADD CONSTRAINT uq_fact_marketing_spend_pk UNIQUE (date_key, campaign_key, region);

ALTER TABLE gold.fact_monthly_budget
    ADD CONSTRAINT uq_fact_monthly_budget_pk  UNIQUE (date_key, region);

ALTER TABLE gold.fact_logistics_costs
    ADD CONSTRAINT uq_fact_logistics_costs_pk UNIQUE (date_key, branch_key);

ALTER TABLE gold.fact_production_logs
    ADD CONSTRAINT uq_fact_production_logs_pk UNIQUE (date_key, product_key, department_key, machine_id);

-- ==============================================================================
-- BƯỚC 4: INDEXES (Tối ưu hóa tốc độ truy vấn BI)
-- ==============================================================================
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key    ON gold.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON gold.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_branch_key  ON gold.fact_sales(branch_key);

CREATE INDEX IF NOT EXISTS idx_fact_mkt_date_key     ON gold.fact_marketing_spend(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_mkt_campaign_key ON gold.fact_marketing_spend(campaign_key);

CREATE INDEX IF NOT EXISTS idx_fact_prod_date_key       ON gold.fact_production_logs(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_product_key    ON gold.fact_production_logs(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_department_key ON gold.fact_production_logs(department_key);

CREATE INDEX IF NOT EXISTS idx_fact_logi_date_key   ON gold.fact_logistics_costs(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_logi_branch_key ON gold.fact_logistics_costs(branch_key);

CREATE INDEX IF NOT EXISTS idx_dim_product_is_current ON gold.dim_product(is_current);

-- ==============================================================================
-- BƯỚC 5: UTILITIES (Dành cho quá trình Dev / Reset)
-- Bôi đen và chạy riêng các dòng dưới đây khi cần đập đi xây lại
-- ==============================================================================
/*
DROP TABLE IF EXISTS gold.fact_sales CASCADE;
DROP TABLE IF EXISTS gold.fact_marketing_spend CASCADE;
DROP TABLE IF EXISTS gold.fact_monthly_budget CASCADE;
DROP TABLE IF EXISTS gold.fact_production_logs CASCADE;
DROP TABLE IF EXISTS gold.fact_logistics_costs CASCADE;

DROP TABLE IF EXISTS gold.dim_date CASCADE;
DROP TABLE IF EXISTS gold.dim_product CASCADE;
DROP TABLE IF EXISTS gold.dim_branch CASCADE;
DROP TABLE IF EXISTS gold.dim_department CASCADE;
DROP TABLE IF EXISTS gold.dim_campaign CASCADE;
*/