-- ============================================================================
-- 1. DROP BẢNG CŨ (Tắt Foreign Key Check để xóa an toàn)
-- ============================================================================
SET FOREIGN_KEY_CHECKS = 0;
DROP TABLE IF EXISTS monthly_budgets;
DROP TABLE IF EXISTS daily_marketing_spend;
DROP TABLE IF EXISTS marketing_campaigns;
SET FOREIGN_KEY_CHECKS = 1;

CREATE TABLE marketing_campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    campaign_name VARCHAR(255) UNIQUE,
    platform VARCHAR(100)
);

CREATE TABLE daily_marketing_spend (
    spend_id INT AUTO_INCREMENT PRIMARY KEY,
    campaign_id INT,
    spend_date VARCHAR(100),
    region VARCHAR(100),
    amount_spent DECIMAL(18,2),
    FOREIGN KEY (campaign_id) REFERENCES marketing_campaigns(campaign_id)
);

CREATE TABLE monthly_budgets (
    budget_id INT AUTO_INCREMENT PRIMARY KEY,
    month_year VARCHAR(50),
    region VARCHAR(100),
    budget_amount DECIMAL(18,2),
    target_revenue DECIMAL(18,2),
    market_size DECIMAL(18,2)
);