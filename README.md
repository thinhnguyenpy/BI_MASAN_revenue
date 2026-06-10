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