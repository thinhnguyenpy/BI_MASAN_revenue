import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when

# ============================================================
# 1. CẤU HÌNH ĐƯỜNG DẪN
# ============================================================
current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
silver_dir   = os.path.join(project_root, "datalake", "silver", "sales_db")

# ============================================================
# 2. KHỞI TẠO SPARK
# ============================================================
print("⚙️ Đang khởi tạo Spark [SILVER - DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("Silver_Dimensions_Sales")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. CONFIG: PK của từng bảng dimension
# ============================================================
DIM_CONFIG = {
    "categories": {"pk": "category_id",  "partition_by": None},
    "products":   {"pk": "product_id",   "partition_by": "category_id"},
    "branches":   {"pk": "branch_id",    "partition_by": None},
}

# ============================================================
# 4. HÀM CLEAN CHUNG
# ============================================================
def clean_string_columns(df):
    """Trim + chuẩn hóa các giá trị rác thành null thực sự."""
    DIRTY_VALUES = ["", "null", "NULL", "N/A", "n/a", "none", "None", "NaN"]
    for c_name, c_type in df.dtypes:
        if c_type == "string":
            df = df.withColumn(
                c_name,
                when(trim(col(c_name)).isin(DIRTY_VALUES), None)
                .otherwise(trim(col(c_name)))
            )
    return df

# ============================================================
# 5. HÀM TRANSFORM CHÍNH
# ============================================================
def transform_dimensions():
    for table, config in DIM_CONFIG.items():
        print(f"\n✨ [DIM] Đang làm sạch bảng {table.upper()}...")

        input_path = os.path.join(bronze_dir, table)
        if not os.path.exists(input_path):
            print(f"⚠️  Không tìm thấy Bronze path: {input_path}. Bỏ qua.")
            continue

        try:
            df_raw   = spark.read.parquet(input_path)
            
            metadata_cols = ["ingest_date"]
            for mc in metadata_cols:
                if mc in df_raw.columns:
                    df_raw = df_raw.drop(mc)
            
            count_raw = df_raw.count()

            # Bước 1: Trim + chuẩn hóa null
            df_clean = clean_string_columns(df_raw)

            # Bước 2: Drop dòng thiếu khóa chính
            df_clean = df_clean.dropna(subset=[config["pk"]])

            # Bước 3: Bỏ bản ghi trùng theo PK
            df_clean = df_clean.dropDuplicates([config["pk"]])

            count_clean = df_clean.count()
            print(f"📊 Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

            # Bước 4: Ghi ra Silver
            output_path = os.path.join(silver_dir, table)
            writer = df_clean.write.mode("overwrite")

            if config["partition_by"]:
                writer = writer.partitionBy(config["partition_by"])

            writer.parquet(output_path)
            print(f"💾 [SILVER] Đã ghi tại: {output_path}")

        except Exception as e:
            print(f"❌ Lỗi khi xử lý {table.upper()}: {e}")
            raise

# ============================================================
# 6. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    transform_dimensions()
    spark.stop()
    print("\n✅ Hoàn tất Transformation Dimensions!")