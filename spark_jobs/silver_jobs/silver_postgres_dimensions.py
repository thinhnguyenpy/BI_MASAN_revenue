import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")
DB_NAME = os.getenv("POSTGRES_DB", "sales_db")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
jars_dir = os.path.join(project_root, "jars")
silver_dir = os.path.join(project_root, "datalake", "silver", "sales_db")
postgres_jar = os.path.join(jars_dir, "postgresql-42.7.3.jar")

print("Đang khởi tạo Spark [DIMENSIONS JOB]...")
spark = SparkSession.builder \
    .appName("Silver_Dimensions_Postgres") \
    .master("local[*]") \
    .config("spark.jars", postgres_jar) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def read_postgres(table_name):
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", table_name) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def process_dimensions():
    dim_tables = ["categories", "products", "branches"]
    
    for table in dim_tables:
        print(f"\n--- Đang xử lý bảng Dimension: {table.upper()} ---")
        df_raw = read_postgres(table)
        
        if table == "products":
            df_clean = df_raw.dropna(subset=["product_id"])
        elif table == "branches":
            df_clean = df_raw.dropna(subset=["branch_id"])
        else:
            df_clean = df_raw
            
        output_path = os.path.join(silver_dir, table)
        df_clean.write.mode("overwrite").parquet(output_path)
        print(f"[THÀNH CÔNG] Đã ghi đè Parquet tại: {output_path}")

if __name__ == "__main__":
    process_dimensions()
    spark.stop()
    print("\n✅ Hoàn tất Job Silver PostgreSQL (Dimensions)!")