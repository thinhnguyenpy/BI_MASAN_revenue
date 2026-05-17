# BI_MASAN_revenue
# Hệ Thống ETL: Từ PostgreSQL (Staging) sang Oracle (Data Warehouse)

Dự án này là một Data Pipeline (ETL) tự động trích xuất dữ liệu từ **PostgreSQL** (đóng vai trò là Staging), thực hiện làm sạch, biến đổi dữ liệu bằng thư viện **Pandas**, và tải dữ liệu lên **Oracle Database** (đóng vai trò là Data Warehouse) theo mô hình Star Schema.

## Yêu cầu hệ thống (Prerequisites)
Trước khi bắt đầu, đảm bảo máy tính của bạn đã cài đặt:
* **Docker** và **Docker Compose**
* **Python** (phiên bản 3.8 trở lên)

---

## Hướng dẫn cài đặt và chạy dự án

### Bước 1: Khởi chạy Database bằng Docker
Hệ thống sử dụng Docker để dựng PostgreSQL và Oracle Database cục bộ. Mở terminal tại thư mục chứa file `docker-compose.yml` và chạy lệnh sau:

```bash
docker-compose up -d

# Tạo môi trường ảo có tên là "venv"
python -m venv venv

# Kích hoạt môi trường ảo
venv\Scripts\activate

# Tạo môi trường ảo có tên là "venv"
python3 -m venv venv

# Kích hoạt môi trường ảo
source venv/bin/activate

pip install -r requirements.txt