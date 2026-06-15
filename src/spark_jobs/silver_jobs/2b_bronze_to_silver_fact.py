import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, to_date,
    year, month, dayofmonth
)
from pyspark.sql.types import DecimalType

# ============================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN
#    Silver đọc từ Bronze — KHÔNG kết nối DB
# ============================================================
current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
silver_dir   = os.path.join(project_root, "datalake", "silver", "sales_db")

# ============================================================
# 2. KHỞI TẠO SPARK
# ============================================================
print("⚙️ Đang khởi tạo Spark [SILVER - FACTS]...")
spark = (
    SparkSession.builder
    .appName("Silver_Facts_Sales")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. HÀM CLEAN STRING CHUNG (tái sử dụng từ Silver Dim)
# ============================================================
DIRTY_VALUES = ["", "null", "NULL", "N/A", "n/a", "none", "None", "NaN"]

def clean_string_columns(df):
    for c_name, c_type in df.dtypes:
        if c_type == "string":
            df = df.withColumn(
                c_name,
                when(trim(col(c_name)).isin(DIRTY_VALUES), None)
                .otherwise(trim(col(c_name)))
            )
    return df

# ============================================================
# 4. TRANSFORM ORDERS
# ============================================================
def transform_orders(target_date=None):
    print("\n✨ [FACT] Đang làm sạch bảng ORDERS...")

    input_path = os.path.join(bronze_dir, "orders")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Không tìm thấy Bronze path: {input_path}")

    # Đọc từ Bronze — có thể filter partition nếu incremental
    if target_date:
        df_raw = spark.read.parquet(
            os.path.join(input_path, f"ingest_date={target_date}")
        )
        # Thêm lại cột ingest_date vì đọc partition cụ thể sẽ mất cột này
        from pyspark.sql.functions import lit
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    # Bước 1: Trim + chuẩn hóa null
    df_clean = clean_string_columns(df_raw)

    # Bước 2: Parse order_date — Bronze lưu dạng string "dd/MM/yyyy"
    df_clean = df_clean.withColumn(
        "order_date",
        to_date(col("order_date"), "yyyy-MM-dd")
    )

    # Bước 3: Drop dòng thiếu khóa chính hoặc ngày không parse được
    df_clean = df_clean.dropna(subset=["order_id", "order_date"])

    # Bước 4: Bỏ duplicate theo PK
    df_clean = df_clean.dropDuplicates(["order_id"])

    count_clean = df_clean.count()
    print(f"📊 Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    # Bước 5: Ghi Silver — partition by year/month để Gold query nhanh
    output_path = os.path.join(silver_dir, "orders")
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"💾 [SILVER] Orders đã ghi tại: {output_path}")

# ============================================================
# 5. TRANSFORM ORDER_DETAILS
# ============================================================
def transform_order_details(target_date=None):
    print("\n✨ [FACT] Đang làm sạch bảng ORDER_DETAILS...")

    input_path = os.path.join(bronze_dir, "order_details")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"❌ Không tìm thấy Bronze path: {input_path}")

    if target_date:
        df_raw = spark.read.parquet(
            os.path.join(input_path, f"ingest_date={target_date}")
        )
        from pyspark.sql.functions import lit
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    # Bước 1: Trim + chuẩn hóa null
    df_clean = clean_string_columns(df_raw)

    # Bước 2: Cast String → Decimal (Bronze lưu là text để tránh NaN crash)
    #         "NaN" đã thành None sau clean_string_columns → cast an toàn
    df_clean = (
        df_clean
        .withColumn("quantity",   col("quantity").cast(DecimalType(18, 2)))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(18, 2)))
        .withColumn("unit_cost",  col("unit_cost").cast(DecimalType(18, 2)))
    )

    # Bước 3: Business rules
    #   - quantity âm hoặc null → 0
    #   - unit_price / unit_cost âm → null (dữ liệu lỗi, không tự điền)
    df_clean = (
        df_clean
        .withColumn("quantity",
            when(col("quantity").isNull() | (col("quantity") < 0), 0)
            .otherwise(col("quantity")))
        .withColumn("unit_price",
            when(col("unit_price") < 0, None)
            .otherwise(col("unit_price")))
        .withColumn("unit_cost",
            when(col("unit_cost") < 0, None)
            .otherwise(col("unit_cost")))
    )

    # Bước 4: Drop dòng thiếu FK
    df_clean = df_clean.dropna(subset=["order_id", "product_id"])

    # Bước 5: Bỏ duplicate theo PK composite
    df_clean = df_clean.dropDuplicates(["order_id", "product_id"])

    count_clean = df_clean.count()
    print(f"📊 Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    # Bước 6: Ghi Silver
    output_path = os.path.join(silver_dir, "order_details")
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"💾 [SILVER] Order Details đã ghi tại: {output_path}")

# ============================================================
# 6. ENTRY POINT
# ============================================================
def process_facts(is_incremental=False, target_date=None):
    if is_incremental:
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"\n🔄 CHẾ ĐỘ INCREMENTAL — ngày: {target_date}")
    else:
        print("\n🔄 CHẾ ĐỘ FULL LOAD")
        target_date = None

    transform_orders(target_date=target_date)
    transform_order_details(target_date=target_date)

if __name__ == "__main__":
    # Đọc tham số từ biến môi trường do Airflow truyền vào
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    # Lấy ngày từ biến môi trường nếu có
    target_date = os.getenv("TARGET_DATE", None)

    process_facts(is_incremental=is_incremental, target_date=target_date)
    spark.stop()
    print("\n✅ Hoàn tất Silver Facts!")