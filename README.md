# BI_MASAN_revenue
# Hệ Thống ETL: Từ Kiến trúc Phân tán (PostgreSQL, MongoDB, MySQL) sang Data Warehouse

Dự án này là một Data Pipeline (ETL) mô phỏng hệ thống dữ liệu doanh nghiệp đa nguồn (Multi-source Distributed System). Dữ liệu thô ban đầu (Raw Data) từ file Excel sẽ được nạp vào 3 cơ sở dữ liệu đóng vai trò là hệ thống vận hành (OLTP/Bronze Layer). Sau đó, dữ liệu sẽ được trích xuất, làm sạch và tổng hợp bằng **Apache Spark** để tải lên **Oracle Database** (đóng vai trò là Data Warehouse) theo mô hình Star Schema. Toàn bộ luồng được điều phối tự động bởi **Apache Airflow**.

---

## Kiến trúc Hệ thống

### Hệ thống nguồn (Bronze Layer)
Dữ liệu thô được chia làm 3 Domain độc lập, lưu trữ mọi dữ liệu thực tế (bao gồm cả dữ liệu lỗi, sai định dạng):

| # | Domain | Database | Nội dung |
|---|--------|----------|----------|
| 1 | Bán hàng & CRM | **PostgreSQL** | Đơn hàng, khách hàng, danh mục sản phẩm |
| 2 | Sản xuất & Vận hành | **MongoDB** | Nhật ký máy móc, chi phí kho bãi (NoSQL) |
| 3 | Tài chính & Marketing | **MySQL** | Ngân sách theo tháng, chi phí quảng cáo theo ngày |

### Data Lake (Medallion Architecture)
Dữ liệu được tổ chức theo kiến trúc Medallion với các file **Parquet**:

```text
datalake/
├── bronze/    # Raw Data      — Dữ liệu thô hút nguyên bản từ 3 Database (lưu vết lịch sử)
├── silver/    # Cleansed Data — Dữ liệu đã ép kiểu, làm sạch rác, xử lý Null
└── gold/      # Aggregated    — Dữ liệu Star Schema sẵn sàng cho BI
```

---

## Yêu cầu hệ thống (Prerequisites)

Trước khi bắt đầu, đảm bảo máy tính đã cài đặt:

- **Docker** và **Docker Compose**
- **Python** 3.8 trở lên
- File JAR driver: `jars/postgresql-42.7.3.jar`

---

## Hướng dẫn thiết lập môi trường (Dev/Test)

### Bước 1: Cấu hình hệ thống

**1. Tạo file `.env`** tại thư mục gốc của project:

```env
# Kết nối PostgreSQL (Bán hàng)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sales_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin_password

# Kết nối MongoDB (Sản xuất)
MONGO_URI=mongodb://admin:admin_password@localhost:27017/

# Kết nối MySQL (Tài chính)
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_DB=finance_db
MYSQL_USER=admin
MYSQL_PASSWORD=admin_password
```

> **Lưu ý:** Nếu triển khai Spark bên trong Docker, đổi `POSTGRES_HOST` thành tên service tương ứng (ví dụ: `stg_postgres_sales`).

**2. Cấu hình Java** (bắt buộc cho PySpark — chỉ cần trên Ubuntu/Linux):

```bash
sudo apt update
sudo apt install openjdk-17-jre-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

**3. Đảm bảo file JAR driver** đã có sẵn tại đường dẫn:

```
jars/postgresql-42.7.3.jar
```

---

### Bước 2: Khởi chạy hạ tầng

```bash
# Khởi chạy cụm Database (Source & Warehouse)
docker compose up -d

# Khởi chạy Airflow
docker compose -f docker-compose.airflow.yml up -d
```

> Hệ thống sử dụng Docker để dựng đồng thời PostgreSQL, MongoDB và MySQL cùng các script tự động khởi tạo bảng (`init-*.sql`, `init-mongo.js`).

**Lấy mật khẩu đăng nhập Airflow (lần đầu khởi động):**

```bash
docker logs bi_masan_airflow | grep "password"
```

Truy cập giao diện Airflow tại: `http://localhost:8080`

---

### Bước 3: Thiết lập môi trường Python

Tạo và kích hoạt Virtual Environment:

**Trên Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Trên Linux / macOS:**
```bash
python3 -m venv venv
source venv/bin/activate
```

Cài đặt các thư viện:
```bash
pip install -r requirements.txt
```

---

### Bước 4: Nạp dữ liệu giả vào hệ thống (Data Seeding)

Chạy một lần để mồi dữ liệu từ file Excel (`masan_case.xlsx`) vào 3 database nguồn:

```bash
# Nạp hệ thống Bán hàng (PostgreSQL)
python src/load_data/load_postgres.py
# Output mong đợi: PostgreSQL Sales Data Loading completed successfully!

# Nạp hệ thống Tài chính (MySQL)
python src/load_data/load_mysql.py
# Output mong đợi: MySQL Finance Data Loading completed successfully!

# Nạp hệ thống Sản xuất (MongoDB)
python src/load_data/load_mongo.py
# Output mong đợi: MongoDB Production Data Loading completed successfully!
```

> **Lưu ý:** Các cảnh báo `UserWarning` liên quan đến định dạng ngày tháng của Pandas là bình thường — hệ thống cố tình lưu dữ liệu thô dưới dạng Text.

---

## Luồng ETL (Pipeline)

### Giai đoạn 1 — Bronze: Hút dữ liệu thô (Ingestion)

Spark kết nối JDBC đến database nguồn, kéo toàn bộ dữ liệu và lưu xuống Parquet. Lớp Bronze chỉ làm duy nhất một việc: sao chép nguyên bản, không transform.

```bash
python spark_jobs/1a_postgres_to_bronze_dim.py
python spark_jobs/1b_postgres_to_bronze_fact.py
```

### Giai đoạn 2 — Silver: Làm sạch dữ liệu (Transformation)

Spark đọc Parquet từ Bronze (không kết nối lại database nguồn), thực hiện:

- **Xử lý rác:** Loại bỏ chuỗi `"NaN"`, `"N/A"`, khoảng trắng vô nghĩa
- **Ép kiểu (Type Casting):** Chuyển từ `String` sang `Decimal`, `Date`,...
- **Loại bỏ trùng lặp:** Xóa các dòng thiếu khóa chính hoặc bị Duplicate

```bash
# Làm sạch các bảng Danh mục (Dimension)
python spark_jobs/silver_jobs/2a_bronze_to_silver_dim.py

# Làm sạch các bảng Sự kiện (Fact)
python spark_jobs/silver_jobs/2b_bronze_to_silver_fact.py
```

### Giai đoạn 3 — Gold: Tải lên Data Warehouse

Spark đọc dữ liệu từ Silver và nạp lên Data Warehouse theo mô hình **Star Schema**, thực hiện:

- **Look-up Keys:** Thay thế mã tự nhiên (ví dụ: `branch_id`) bằng khóa nhân tạo (`branch_key`)
- **Staging:** Đẩy vào bảng tạm trước khi đưa vào bảng chính để đảm bảo an toàn
- **UPSERT:** Dùng `ON CONFLICT DO UPDATE` — tự động cập nhật nếu đã tồn tại, thêm mới nếu chưa có (Idempotent)

```bash
# Khởi tạo lịch, nạp Dimensions và Fact Bán hàng
python spark_jobs/gold_jobs/3_silver_to_gold_sales.py
```

---

## Quy trình chạy ETL qua Airflow

1. Truy cập `http://localhost:8080`
2. Bật (Toggle) DAG `bi_masan_revenue_pipeline`
3. Nhấn **Trigger DAG** để bắt đầu quá trình
4. Nếu task báo lỗi (màu đỏ):
   - Kiểm tra tab **Logs** của task đó
   - Sửa lỗi code trên máy thật
   - Nhấn **Clear** trên giao diện Airflow để chạy lại task đó mà không ảnh hưởng cả luồng

---

## Reset hệ thống (Dành cho Test nhiều lần)

Nếu sửa code Spark hoặc muốn chạy lại luồng từ đầu, dùng quy trình "Sạch từ gốc" để tránh xung đột dữ liệu:

```bash
# 1. Dừng toàn bộ các container
docker compose -f docker-compose.airflow.yml down
docker compose down

# 2. Xóa sạch dữ liệu đã qua xử lý (giữ nguyên Source Database)
sudo rm -rf data/postgres_dw
sudo rm -rf datalake/bronze/*
sudo rm -rf datalake/silver/*
sudo rm -rf datalake/gold/*

# 3. Khởi động lại hạ tầng
docker compose up -d
docker compose -f docker-compose.airflow.yml up -d

# 4. Lấy lại mật khẩu Airflow
docker logs bi_masan_airflow | grep "password"
```

> **Lưu ý:** Lệnh `docker compose down -v` và `sudo rm -rf ./data` sẽ xóa **toàn bộ** dữ liệu kể cả Source Database — chỉ dùng khi cần reset hoàn toàn từ đầu.

---

## Cấu trúc thư mục dự án

```text
BI_MASAN_revenue/
├── datalake/
│   ├── bronze/
│   ├── silver/
│   └── gold/
├── jars/
│   └── postgresql-42.7.3.jar
├── load_data/                        # Script seeding dữ liệu thô
│   ├── load_postgres.py
│   ├── load_mysql.py
│   └── load_mongo.py
├── spark_jobs/
│   ├── 1a_postgres_to_bronze_dim.py
│   ├── 1b_postgres_to_bronze_fact.py
│   ├── silver_jobs/
│   │   ├── 2a_bronze_to_silver_dim.py
│   │   └── 2b_bronze_to_silver_fact.py
│   └── gold_jobs/
│       └── 3_silver_to_gold_sales.py
├── docker-compose.yml
├── docker-compose.airflow.yml
├── requirements.txt
├── .env                              # Không commit file này lên Git!
└── masan_case.xlsx                   # Dữ liệu nguồn
```