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
--   4. Nếu cần reset: chạy phần DROP ở cuối file rồi chạy lại từ đầu
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
-- Trục thời gian trung tâm, sinh tự động từ 2020 đến 2030
-- Smart Key: YYYYMMDD (vd: 20260611) thay vì SERIAL để dễ đọc và join
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key     INT PRIMARY KEY,   -- Smart key: YYYYMMDD
    full_date    DATE NOT NULL,
    day_of_week  VARCHAR(20),       -- Monday, Tuesday...
    day_of_month INT,               -- 1 - 31
    month_number INT,               -- 1 - 12
    month_name   VARCHAR(20),       -- January, February...
    quarter      INT,               -- 1, 2, 3, 4
    year         INT
);

-- ------------------------------------------------------------------------------
-- 1.2 DIM_PRODUCT (SCD Type 2)
-- Nguồn: PostgreSQL sales_db.products + categories
-- SCD Type 2: Lưu lịch sử thay đổi tên/danh mục sản phẩm
--   - Khi sản phẩm đổi category: set is_current=0, end_date=hôm nay cho bản cũ
--                                 insert bản mới với is_current=1, start_date=hôm nay
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key   SERIAL PRIMARY KEY,  -- Surrogate Key (tự tăng)
    product_id    VARCHAR(50) NOT NULL, -- Natural Key từ PostgreSQL
    product_name  VARCHAR(255),
    category_name VARCHAR(255),
    -- SCD Type 2 tracking columns
    start_date    DATE NOT NULL,        -- Ngày bản ghi bắt đầu có hiệu lực
    end_date      DATE,                 -- NULL = đang hiệu lực
    is_current    SMALLINT DEFAULT 1    -- 1 = hiện tại, 0 = lịch sử
);

-- ------------------------------------------------------------------------------
-- 1.3 DIM_BRANCH
-- Nguồn: PostgreSQL sales_db.branches
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_branch (
    branch_key  SERIAL PRIMARY KEY,
    branch_id   VARCHAR(50) NOT NULL,  -- Natural Key từ PostgreSQL
    branch_name VARCHAR(255),
    region      VARCHAR(100)
);

-- ------------------------------------------------------------------------------
-- 1.4 DIM_DEPARTMENT
-- Nguồn: MongoDB production_db.Departments
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_department (
    department_key  SERIAL PRIMARY KEY,
    department_id   INT NOT NULL,       -- Natural Key từ MongoDB (kiểu INT)
    department_name VARCHAR(255)
);

-- ------------------------------------------------------------------------------
-- 1.5 DIM_CAMPAIGN
-- Nguồn: MySQL marketing_db.marketing_campaigns
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.dim_campaign (
    campaign_key  SERIAL PRIMARY KEY,
    campaign_id   INT NOT NULL,         -- Natural Key từ MySQL (AUTO_INCREMENT)
    campaign_name VARCHAR(255),
    platform      VARCHAR(100)          -- Facebook, Google, TikTok...
);


-- ==============================================================================
-- BƯỚC 2: FACT TABLES
-- Thứ tự tạo: sau tất cả Dimension Tables vì có FOREIGN KEY
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- 2.1 FACT_SALES
-- Nguồn: PostgreSQL orders + order_details
-- Granularity: 1 dòng = 1 sản phẩm trong 1 đơn hàng
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_key        SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_key         INT REFERENCES gold.dim_date(date_key),
    product_key      INT REFERENCES gold.dim_product(product_key),
    branch_key       INT REFERENCES gold.dim_branch(branch_key),
    -- Degenerate Dimensions (không cần dim table riêng)
    order_id         BIGINT NOT NULL,
    sales_channel    VARCHAR(100),      -- Online, Offline...
    customer_segment VARCHAR(100),      -- VIP, Regular...
    -- Measures
    quantity         DECIMAL(18,2) DEFAULT 0,
    unit_price       DECIMAL(18,2) DEFAULT 0,
    unit_cost        DECIMAL(18,2) DEFAULT 0,
    revenue          DECIMAL(18,2) DEFAULT 0,   -- quantity * unit_price
    total_cost       DECIMAL(18,2) DEFAULT 0,   -- quantity * unit_cost
    profit           DECIMAL(18,2) DEFAULT 0    -- revenue - total_cost
);

-- ------------------------------------------------------------------------------
-- 2.2 FACT_MARKETING_SPEND
-- Nguồn: MySQL daily_marketing_spend + marketing_campaigns
-- Granularity: 1 dòng = 1 chiến dịch trong 1 ngày tại 1 khu vực
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_marketing_spend (
    spend_key    SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_key     INT REFERENCES gold.dim_date(date_key),
    campaign_key INT REFERENCES gold.dim_campaign(campaign_key),
    -- Degenerate Dimension
    region       VARCHAR(100),
    -- Measures
    daily_spend  DECIMAL(18,2) DEFAULT 0
);

-- ------------------------------------------------------------------------------
-- 2.3 FACT_MONTHLY_BUDGET
-- Nguồn: MySQL monthly_budgets
-- Granularity: 1 dòng = 1 khu vực trong 1 tháng
-- Lưu ý: date_key trỏ tới ngày đầu tiên của tháng (vd: 20260601)
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_monthly_budget (
    budget_key     SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_key       INT REFERENCES gold.dim_date(date_key),
    -- Degenerate Dimension
    region         VARCHAR(100),
    -- Measures
    budget_amount  DECIMAL(18,2),
    target_revenue DECIMAL(18,2),
    market_size    DECIMAL(18,2)
);

-- ------------------------------------------------------------------------------
-- 2.4 FACT_PRODUCTION_LOGS
-- Nguồn: MongoDB ProductionLogs
-- Granularity: 1 dòng = 1 máy + 1 sản phẩm + 1 ngày
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_production_logs (
    log_key           SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_key          INT REFERENCES gold.dim_date(date_key),
    product_key       INT REFERENCES gold.dim_product(product_key),
    department_key    INT REFERENCES gold.dim_department(department_key),
    -- Degenerate Dimension
    machine_id        VARCHAR(100),
    -- Measures
    inventory_level   DECIMAL(18,2) DEFAULT 0,
    raw_material_cost DECIMAL(18,2) DEFAULT 0,
    labor_cost        DECIMAL(18,2) DEFAULT 0
);

-- ------------------------------------------------------------------------------
-- 2.5 FACT_LOGISTICS_COSTS
-- Nguồn: MongoDB LogisticsCosts
-- Granularity: 1 dòng = 1 chi nhánh trong 1 ngày
-- ------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.fact_logistics_costs (
    logistics_key  SERIAL PRIMARY KEY,
    -- Foreign Keys
    date_key       INT REFERENCES gold.dim_date(date_key),
    branch_key     INT REFERENCES gold.dim_branch(branch_key),
    -- Measures
    logistics_cost DECIMAL(18,2) DEFAULT 0
);


-- ==============================================================================
-- BƯỚC 3: UNIQUE CONSTRAINTS
-- Dùng cho ON CONFLICT DO UPDATE trong ETL job (upsert pattern)
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


-- ==============================================================================
-- BƯỚC 4: INDEXES
-- Tăng tốc query cho BI tools (Power BI, Metabase...)
-- ==============================================================================

-- fact_sales: các cột hay filter/join nhất
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key    ON gold.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key ON gold.fact_sales(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_branch_key  ON gold.fact_sales(branch_key);

-- fact_marketing_spend
CREATE INDEX IF NOT EXISTS idx_fact_mkt_date_key     ON gold.fact_marketing_spend(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_mkt_campaign_key ON gold.fact_marketing_spend(campaign_key);

-- fact_production_logs
CREATE INDEX IF NOT EXISTS idx_fact_prod_date_key       ON gold.fact_production_logs(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_product_key    ON gold.fact_production_logs(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_prod_department_key ON gold.fact_production_logs(department_key);

-- fact_logistics_costs
CREATE INDEX IF NOT EXISTS idx_fact_logi_date_key   ON gold.fact_logistics_costs(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_logi_branch_key ON gold.fact_logistics_costs(branch_key);

-- dim_product: hay filter theo is_current
CREATE INDEX IF NOT EXISTS idx_dim_product_is_current ON gold.dim_product(is_current);


-- ==============================================================================
-- BƯỚC 5: VERIFY — Kiểm tra sau khi chạy
-- ==============================================================================
SELECT
    table_name,
    pg_size_pretty(pg_total_relation_size('gold.' || table_name)) AS size
FROM information_schema.tables
WHERE table_schema = 'gold'
  AND table_name NOT LIKE 'stg_%'
ORDER BY
    CASE
        WHEN table_name LIKE 'dim_%'  THEN 1
        WHEN table_name LIKE 'fact_%' THEN 2
        ELSE 3
    END,
    table_name;
