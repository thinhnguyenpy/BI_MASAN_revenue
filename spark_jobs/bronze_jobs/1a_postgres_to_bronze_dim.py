import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# 1. Nạp cấu hình
load_dotenv()
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sales_db")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
jars_dir = os.path.join(project_root, "jars")
bronze_dir = os.path.join(project_root, "datalake", "bronze", "sales_db")
postgres_jar = os.path.join(jars_dir, "postgresql-42.7.3.jar")

print("🚀 Đang khởi tạo Spark [BRONZE - DIMENSIONS]...")
spark = SparkSession.builder \
    .appName("Postgres_Bronze_Dimensions") \
    .master("local[*]") \
    .config("spark.jars", postgres_jar) \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

def read_from_source(table_name):
    return spark.read.format("jdbc") \
        .option("url", JDBC_URL).option("dbtable", table_name) \
        .option("user", DB_USER).option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver").load()

def ingest_dimensions():
    dim_tables = ["categories", "products", "branches"]
    for table in dim_tables:
        print(f"📥 [DIM] Đang hút bảng {table.upper()}...")
        df_raw = read_from_source(table)
        
        output_path = os.path.join(bronze_dir, table)
        df_raw.write.mode("overwrite").parquet(output_path)
        print(f"📂 Đã lưu Bronze Dimension tại: {output_path}")

if __name__ == "__main__":
    ingest_dimensions()
    spark.stop()
    print("\n✅ Hoàn tất Ingestion Dimensions!")