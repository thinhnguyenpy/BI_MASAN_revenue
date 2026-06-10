import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, when

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

print("Đang khởi tạo Spark [FACTS JOB]...")
spark = SparkSession.builder \
    .appName("Silver_Facts_Postgres") \
    .master("local[*]") \
    .config("spark.jars", postgres_jar) \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def read_postgres(query):
    return spark.read \
        .format("jdbc") \
        .option("url", JDBC_URL) \
        .option("dbtable", query) \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .load()

def process_facts(is_incremental=False, target_date=None):
    if is_incremental:
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            
        print(f"\n[CHẾ ĐỘ INCREMENTAL] Đang kéo dữ liệu cho ngày: {target_date}")
        orders_query = f"(SELECT * FROM orders WHERE TO_DATE(order_date, 'DD/MM/YYYY') = '{target_date}') AS q_orders"
        details_query = f"""(
            SELECT d.* FROM order_details d
            JOIN orders o ON d.order_id = o.order_id
            WHERE TO_DATE(o.order_date, 'DD/MM/YYYY') = '{target_date}'
        ) AS q_details"""
        write_mode = "append"
    else:
        print("\n[CHẾ ĐỘ FULL LOAD] Đang kéo toàn bộ lịch sử dữ liệu...")
        orders_query = "orders"
        details_query = "order_details"
        write_mode = "overwrite"
        
    print("\n--- Đang xử lý bảng Transaction: ORDERS ---")
    df_orders = read_postgres(orders_query)
    
    df_orders_clean = df_orders.withColumn("order_date_clean", to_date(col("order_date"), "dd/MM/yyyy"))
    df_orders_clean = df_orders_clean.drop("order_date").withColumnRenamed("order_date_clean", "order_date")
    
    output_orders = os.path.join(silver_dir, "orders")
    df_orders_clean.write.mode(write_mode).parquet(output_orders)
    print(f"[THÀNH CÔNG] Đã lưu Parquet Orders tại: {output_orders}")

    print("\n--- Đang xử lý bảng Transaction: ORDER_DETAILS ---")
    df_details = read_postgres(details_query)
    
    df_details_clean = df_details.withColumn(
        "quantity", 
        when(col("quantity").isNull() | (col("quantity") < 0), 0).otherwise(col("quantity"))
    )
    
    output_details = os.path.join(silver_dir, "order_details")
    df_details_clean.write.mode(write_mode).parquet(output_details)
    print(f"[THÀNH CÔNG] Đã lưu Parquet Order Details tại: {output_details}")

if __name__ == "__main__":
    process_facts(is_incremental=False)
    spark.stop()
    print("\n✅ Hoàn tất Job Silver PostgreSQL (Facts)!")