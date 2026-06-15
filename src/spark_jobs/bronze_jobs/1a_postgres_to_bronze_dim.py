import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# ============================================================
# 1. NẠP CẤU HÌNH
# ============================================================
load_dotenv()
DB_HOST     = os.getenv("POSTGRES_HOST", "stg_postgres_sales")
DB_PORT     = os.getenv("POSTGRES_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB",   "sales_db")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
JDBC_URL    = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

# File nằm ở: src/spark_jobs/bronze_jobs/1a_postgres_to_bronze_dim.py
# Lùi 3 cấp: bronze_jobs -> spark_jobs -> src -> project_root
current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")

# Debug đường dẫn — xóa sau khi confirm đúng
print(f"📁 project_root : {project_root}")
print(f"📁 bronze_dir   : {bronze_dir}")
print(f"📁 postgres_jar : {postgres_jar}")
print(f"✅ Jar exists   : {os.path.exists(postgres_jar)}")

# ============================================================
# 2. KHỞI TẠO SPARK
# ============================================================
print("\n🚀 Đang khởi tạo Spark [BRONZE - DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("Postgres_Bronze_Dimensions")
    .master("local[*]")
    .config("spark.jars", postgres_jar)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. HÀM ĐỌC JDBC
# ============================================================
def read_from_source(table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  table_name)
        .option("user",     DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .load()
    )

# ============================================================
# 4. HÀM INGEST CHÍNH
# ============================================================
def ingest_dimensions(ingest_date: str):
    """
    Full load toàn bộ dimension tables từ PostgreSQL vào Bronze.
    Dim tables không dùng incremental vì data nhỏ và ít thay đổi.
    
    Args:
        ingest_date: Ngày chạy job, dùng làm partition key (YYYY-MM-DD)
    """
    dim_tables = ["categories", "products", "branches"]

    print(f"\n📅 NGÀY THỰC THI (INGEST DATE): {ingest_date}")
    print(f"📋 Danh sách bảng: {dim_tables}")

    success_tables = []
    failed_tables  = []

    for table in dim_tables:
        try:
            print(f"\n📥 [DIM] Đang hút bảng {table.upper()}...")

            df_raw = read_from_source(table)
            count  = df_raw.count()

            # Gắn metadata partition
            df_partitioned = df_raw.withColumn("ingest_date", lit(ingest_date))

            output_path = os.path.join(bronze_dir, table)

            df_partitioned.write \
                .mode("overwrite") \
                .partitionBy("ingest_date") \
                .parquet(output_path)

            print(f"📂 Đã lưu {count} dòng tại: {output_path}/ingest_date={ingest_date}")
            success_tables.append(table)

        except Exception as e:
            print(f"❌ Lỗi khi xử lý bảng {table.upper()}: {e}")
            failed_tables.append(table)

    # Summary
    print(f"\n{'='*50}")
    print(f"✅ Thành công : {success_tables}")
    if failed_tables:
        print(f"❌ Thất bại   : {failed_tables}")
        raise RuntimeError(f"Một số bảng bị lỗi: {failed_tables}")

# ============================================================
# 5. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    ingest_dimensions(ingest_date=run_date)
    spark.stop()
    print("\n✅ Hoàn tất Ingestion Dimensions!")