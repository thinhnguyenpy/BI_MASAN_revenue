CREATE TABLE categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(255) UNIQUE -- Thêm UNIQUE để chạy được ON CONFLICT
);

CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_name VARCHAR(255),        -- Bỏ NOT NULL, nới rộng VARCHAR
    category_id INT REFERENCES categories(category_id)
);

CREATE TABLE branches (
    branch_id VARCHAR(50) PRIMARY KEY,
    branch_name VARCHAR(100),         -- Bỏ NOT NULL
    region VARCHAR(50)                -- Bỏ NOT NULL
);

CREATE TABLE orders (
    order_id BIGINT PRIMARY KEY,      -- Sửa BIGSERIAL thành BIGINT vì ta nạp ID trực tiếp
    order_date VARCHAR(100),          -- CHUYỂN THÀNH VARCHAR ĐỂ NHẬN DỮ LIỆU RÁC
    branch_id VARCHAR(50) REFERENCES branches(branch_id),
    sales_channel VARCHAR(100),
    customer_segment VARCHAR(100)
);

CREATE TABLE order_details (
    order_id BIGINT REFERENCES orders(order_id),
    product_id VARCHAR(50) REFERENCES products(product_id),
    quantity DECIMAL(18,2),           -- Bỏ NOT NULL
    unit_price DECIMAL(18,2),         -- Bỏ NOT NULL
    unit_cost DECIMAL(18,2),          -- Bỏ NOT NULL
    PRIMARY KEY (order_id, product_id)
);