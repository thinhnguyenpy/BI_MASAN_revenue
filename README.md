# BI_MASAN_revenue
# Hệ Thống ETL: Từ Kiến trúc Phân tán (PostgreSQL, MongoDB, MySQL) sang Data Warehouse

Dự án này là một Data Pipeline (ETL) mô phỏng hệ thống dữ liệu doanh nghiệp đa nguồn (Multi-source Distributed System). Dữ liệu thô ban đầu (Raw Data) từ file Excel sẽ được nạp vào 3 cơ sở dữ liệu đóng vai trò là hệ thống vận hành (OLTP/Bronze Layer). Sau đó, dữ liệu sẽ được trích xuất, làm sạch và tổng hợp bằng **Apache Spark** (chuẩn bị triển khai) để tải lên **Oracle Database** (đóng vai trò là Data Warehouse) theo mô hình Star Schema.

## Kiến trúc Hệ thống nguồn (Bronze Layer)
Hệ thống lưu trữ dữ liệu thô được chia làm 3 Domain độc lập để nhận mọi dữ liệu thực tế (bao gồm cả dữ liệu lỗi, sai định dạng):
1. **Hệ thống Bán hàng & CRM (PostgreSQL):** Quản lý đơn hàng, khách hàng và danh mục sản phẩm.
2. **Hệ thống Sản xuất & Vận hành (MongoDB):** Quản lý nhật ký máy móc, chi phí kho bãi theo cấu trúc NoSQL.
3. **Hệ thống Tài chính & Marketing (MySQL):** Quản lý ngân sách theo tháng và chi phí chạy quảng cáo theo ngày.

---

## Yêu cầu hệ thống (Prerequisites)
Trước khi bắt đầu, đảm bảo máy tính của bạn đã cài đặt:
* **Docker** và **Docker Compose**
* **Python** (phiên bản 3.8 trở lên)

---

## Hướng dẫn cài đặt và chạy dự án

### Bước 1: Khởi chạy cụm Database bằng Docker
Hệ thống sử dụng Docker để dựng đồng thời PostgreSQL, MongoDB và MySQL cục bộ cùng các script tự động khởi tạo bảng (`init-*.sql`, `init-mongo.js`).
Mở terminal tại thư mục chứa file `docker-compose.yml` và chạy lệnh sau (thêm tham số `-d` để chạy ngầm):

```bash
# Xóa rác cũ nếu có (Chỉ dùng khi cần reset sạch data)
# docker compose down -v 
# sudo rm -rf ./data

# Khởi chạy toàn bộ hệ thống
docker compose up -d

### Bước 2: Thiết lập Môi trường Python
Tạo và kích hoạt môi trường ảo (Virtual Environment) để cài đặt các thư viện cần thiết mà không ảnh hưởng đến máy tính thật.

**Trên Windows:**
```bash
python -m venv venv
venv\Scripts\activate
```

**Trên Linux / MacOS (Ubuntu):**
```bash
python3 -m venv venv
source venv/bin/activate
```

**Cài đặt các thư viện (Dependencies):**
Sau khi môi trường ảo đã được kích hoạt (có chữ `(venv)` ở đầu dòng lệnh), tiến hành cài đặt thư viện từ file `requirements.txt`:
```bash
pip install -r requirements.txt
```

---

### Bước 3: Nạp dữ liệu thô vào Hệ thống (Data Seeding)
Chạy các script Python đã được chuẩn bị sẵn để mô phỏng quá trình đổ dữ liệu rác (Raw Data) từ file Excel (`masan_case.xlsx`) vào 3 cơ sở dữ liệu nguồn.

*(Lưu ý: Các cảnh báo UserWarning liên quan đến định dạng ngày tháng của thư viện Pandas là bình thường và không ảnh hưởng đến quá trình nạp dữ liệu vì hệ thống cố tình lưu trữ dữ liệu dưới dạng chuỗi Text).*

**1. Nạp hệ thống Bán hàng (PostgreSQL):**
```bash
python load_data/load_postgres.py
# Output mong đợi: PostgreSQL Sales Data Loading completed successfully!
```

**2. Nạp hệ thống Tài chính (MySQL):**
```bash
python load_data/load_mysql.py
# Output mong đợi: MySQL Finance Data Loading completed successfully!
```

**3. Nạp hệ thống Sản xuất (MongoDB):**
```bash
python load_data/load_mongo.py
# Output mong đợi: MongoDB Production Data Loading completed successfully!
```

### Bước 4: Cấu hình biến môi trường (.env) và Java (Dành cho Spark)
Hệ thống sử dụng file `.env` để bảo mật thông tin kết nối cơ sở dữ liệu thay vì ghi trực tiếp vào mã nguồn.

**1. Tạo file `.env`:**
Tại thư mục gốc của project, tạo một file tên là `.env` và dán nội dung sau vào:
```env
# Kết nối PostgreSQL (Bán hàng)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=sales_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=admin_password
```
*(Lưu ý: Nếu sau này triển khai Spark bên trong Docker, hãy đổi `POSTGRES_HOST` thành `stg_postgres_sales`)*.

**2. Cấu hình Java (Bắt buộc cho PySpark):**
Mặc dù viết bằng Python, nhân cốt lõi của Spark yêu cầu phải có Java Runtime Environment.
Trên Ubuntu, tiến hành cài đặt và cấu hình đường dẫn `JAVA_HOME` bằng lệnh sau:
```bash
sudo apt update
sudo apt install openjdk-17-jre-headless
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
```

---

## Giai đoạn 3: Trích xuất và Làm sạch dữ liệu (Data Lake - Medallion Architecture)
Hệ thống lưu trữ dữ liệu lớn (Data Lake) được thiết kế theo kiến trúc Medallion, tổ chức thành các thư mục vật lý chứa file **Parquet**.

```text
datalake/
├── bronze/    # Raw Data: Dữ liệu thô hút nguyên bản từ 3 Database (Lưu vết lịch sử).
├── silver/    # Cleansed Data: Dữ liệu đã được ép kiểu, làm sạch rác, xử lý Null.
└── gold/      # Aggregated Data: Dữ liệu gộp/Star Schema sẵn sàng cho BI (Sắp triển khai).
```

### 3.1. Hút dữ liệu thô vào lớp Bronze (Ingestion)
Lớp Bronze chỉ làm duy nhất một nhiệm vụ: Kết nối JDBC đến hệ thống nguồn, kéo nhanh toàn bộ dữ liệu và lưu xuống dạng Parquet để trả lại tài nguyên cho Database nguồn. 

Kích hoạt môi trường ảo và chạy các script trích xuất:
```bash
python spark_jobs/1a_postgres_to_bronze_dim.py

python spark_jobs/1b_postgres_to_bronze_fact.py
```

### 3.2. Làm sạch dữ liệu vào lớp Silver (Transformation)
Ở bước này, Spark hoàn toàn không kết nối đến Database nguồn. Hệ thống đọc file Parquet từ lớp Bronze lên RAM, thực hiện chuẩn hóa:

* **Xử lý rác:** Loại bỏ các chuỗi "NaN", "N/A", khoảng trắng vô nghĩa.
* **Ép kiểu (Type Casting):** Chuyển đổi dữ liệu từ dạng chuỗi (`String`) sang các kiểu định dạng chuẩn (`Decimal`, `Date`) để phục vụ tính toán.
* **Loại bỏ trùng lặp:** Xóa bỏ các dòng thiếu khóa chính hoặc bị trùng (Duplicate).

```bash
# Làm sạch các bảng Danh mục
python spark_jobs/silver_jobs/2a_bronze_to_silver_dim.py

# Làm sạch các bảng Sự kiện
python spark_jobs/silver_jobs/2b_bronze_to_silver_fact.py
```

### 4. Tải dữ liệu lên Data Warehouse (Lớp Gold)
Ở bước này, Spark sẽ đọc dữ liệu đã làm sạch từ lớp Silver và tải lên hệ thống Data Warehouse (PostgreSQL) theo mô hình Sơ đồ hình sao (Star Schema), thực hiện các nghiệp vụ:

* **Khớp khóa (Look-up Keys):** Thay thế các mã tự nhiên gốc (ví dụ: `branch_id`) bằng các khóa nhân tạo tự tăng (`branch_key`) từ các bảng Dimension.
* **Kỹ thuật Staging:** Đẩy dữ liệu từ Spark vào các bảng tạm (Staging Tables) trên Database trước khi đưa vào bảng chính thức để đảm bảo an toàn thao tác.
* **Nạp dữ liệu (UPSERT):** Sử dụng cơ chế `ON CONFLICT DO UPDATE` để hợp nhất dữ liệu. Hệ thống sẽ tự động cập nhật nếu khóa đã tồn tại hoặc thêm mới nếu chưa, đảm bảo tuyệt đối không lặp dữ liệu (Idempotent).

```bash
# Khởi tạo lịch, nạp Dimensions và Fact Bán hàng
python spark_jobs/gold_jobs/3_silver_to_gold_sales.py
```