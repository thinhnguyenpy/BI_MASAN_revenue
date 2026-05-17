-- ============================================================================
-- 1. DROP BẢNG CŨ (MÔ PHỎNG DROP TABLE IF EXISTS)
-- ============================================================================
-- Khối lệnh PL/SQL này sẽ tự động tìm và xóa các bảng nếu chúng đã tồn tại,
-- kèm theo CASCADE CONSTRAINTS để xóa luôn các khóa ngoại (Foreign Keys) liên quan.

BEGIN
   FOR cur_rec IN (
      SELECT table_name FROM user_tables 
      WHERE table_name IN (
         'FACT_SALES', 'DIM_PROMOTION', 'DIM_CUSTOMER_SEGMENT', 
         'DIM_CHANNEL', 'DIM_PRODUCT', 'DIM_BRANCH', 'DIM_REGION', 'DIM_DATE'
      )
   ) LOOP
      EXECUTE IMMEDIATE 'DROP TABLE ' || cur_rec.table_name || ' CASCADE CONSTRAINTS';
   END LOOP;
END;
/

-- ============================================================================
-- 2. TẠO CÁC BẢNG DIMENSION (CHIỀU DỮ LIỆU)
-- ============================================================================

CREATE TABLE dim_date (
    full_date      DATE PRIMARY KEY,
    year           NUMBER(4) NOT NULL,
    month          NUMBER(2) NOT NULL,
    month_name     VARCHAR2(20),
    quarter        NUMBER(1),
    year_month     VARCHAR2(7)
);

CREATE TABLE dim_region (
    region_name    VARCHAR2(50) PRIMARY KEY
);

CREATE TABLE dim_branch (
    branch_name    VARCHAR2(100) PRIMARY KEY,
    region_name    VARCHAR2(50)
);

CREATE TABLE dim_product (
    product_name   VARCHAR2(150) PRIMARY KEY,
    category       VARCHAR2(100)
);

CREATE TABLE dim_channel (
    channel_name   VARCHAR2(100) PRIMARY KEY
);

CREATE TABLE dim_customer_segment (
    segment_name   VARCHAR2(100) PRIMARY KEY
);

CREATE TABLE dim_promotion (
    promotion_campaign VARCHAR2(150) PRIMARY KEY
);

-- ============================================================================
-- 3. TẠO BẢNG FACT (BẢNG SỰ KIỆN CHÍNH)
-- ============================================================================

CREATE TABLE fact_sales (
    order_id             NUMBER(19) PRIMARY KEY,

    full_date            DATE NOT NULL REFERENCES dim_date(full_date),
    region_name          VARCHAR2(50) NOT NULL REFERENCES dim_region(region_name),
    branch_name          VARCHAR2(100) REFERENCES dim_branch(branch_name),
    product_name         VARCHAR2(150) REFERENCES dim_product(product_name),
    channel_name         VARCHAR2(100) REFERENCES dim_channel(channel_name),
    segment_name         VARCHAR2(100) REFERENCES dim_customer_segment(segment_name),
    promotion_campaign   VARCHAR2(150) REFERENCES dim_promotion(promotion_campaign),

    quantity             NUMBER(18,2),
    revenue              NUMBER(18,2) NOT NULL,
    cost                 NUMBER(18,2),
    profit               NUMBER(18,2),

    market_size          NUMBER(18,2),
    budget               NUMBER(18,2),
    logistics_cost       NUMBER(18,2),
    marketing_cost       NUMBER(18,2)
);