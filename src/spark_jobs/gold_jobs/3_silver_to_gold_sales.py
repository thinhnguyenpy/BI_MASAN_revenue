import os
import re
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, date_format,
    year, month, dayofmonth, quarter
)

# ============================================================
# 1. CẤU HÌNH
# ============================================================
load_dotenv()
DW_HOST     = os.getenv("DW_HOST", "localhost")
DW_PORT     = os.getenv("DW_PORT", "5434")
DW_NAME     = os.getenv("DW_DB", "datawarehouse")
DW_USER     = os.getenv("DW_USER")
DW_PASSWORD = os.getenv("DW_PASSWORD")
DW_JDBC_URL = f"jdbc:postgresql://{DW_HOST}:{DW_PORT}/{DW_NAME}"

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
silver_dir   = os.path.join(project_root, "datalake", "silver", "sales_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")

# ============================================================
# 2. KHỞI TẠO SPARK
# ============================================================
print("🌟 Đang khởi tạo Spark [GOLD - DATA WAREHOUSE]...")
spark = (
    SparkSession.builder
    .appName("Silver_To_Gold_Sales")
    .master("local[*]")
    .config("spark.jars", postgres_jar)
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ============================================================
# 3. HÀM HỖ TRỢ JDBC
# ============================================================
def get_dw_conn():
    """Tạo kết nối psycopg2 tới Data Warehouse."""
    return psycopg2.connect(
        host=DW_HOST, port=DW_PORT,
        dbname=DW_NAME, user=DW_USER, password=DW_PASSWORD
    )

def read_gold_table(table_name):
    """Đọc bảng từ Gold DW về Spark DataFrame."""
    return (
        spark.read.format("jdbc")
        .option("url",      DW_JDBC_URL)
        .option("dbtable",  table_name)
        .option("user",     DW_USER)
        .option("password", DW_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .load()
    )

def write_staging(df, staging_table):
    """
    Ghi DataFrame vào staging table (overwrite toàn bộ).
    Staging là bảng tạm, không có FK constraint.
    """
    (
        df.write.format("jdbc")
        .option("url",      DW_JDBC_URL)
        .option("dbtable",  staging_table)
        .option("user",     DW_USER)
        .option("password", DW_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )

def upsert_to_gold(df, target_table, staging_table, conflict_keys: list, update_cols: list):
    """
    Pattern: Ghi vào staging → INSERT ... ON CONFLICT DO UPDATE vào target.
    Idempotent: chạy lại nhiều lần không tạo duplicate.
    
    Args:
        df            : DataFrame cần upsert
        target_table  : Bảng đích trong Gold (vd: gold.dim_branch)
        staging_table : Bảng tạm (vd: gold.stg_dim_branch)
        conflict_keys : Cột dùng để detect duplicate (vd: ["branch_id"])
        update_cols   : Cột cần update khi conflict (vd: ["branch_name", "region"])
    """
    # Bước 1: Ghi vào staging
    write_staging(df, staging_table)
    print(f"   => 📥 Đã ghi {df.count()} dòng vào staging: {staging_table}")

    # Bước 2: Upsert từ staging vào target
    col_list      = ", ".join(df.columns)
    conflict_str  = ", ".join(conflict_keys)
    update_str    = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
    """

    conn = get_dw_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        inserted = cur.rowcount
        conn.commit()
        cur.close()
        print(f"   => 💾 Upsert thành công vào {target_table} ({inserted} dòng affected)")
    except Exception as e:
        conn.rollback()
        print(f"   => ❌ Lỗi upsert vào {target_table}: {e}")
        raise
    finally:
        conn.close()

# ============================================================
# 4. TẠO STAGING TABLES (chạy 1 lần khi setup)
# Staging không cần PK/FK, chỉ là bảng tạm trung chuyển
# ============================================================
def create_staging_tables():
    """Tạo staging tables nếu chưa có."""
    sqls = [
        # Staging cho Dimensions
        """CREATE TABLE IF NOT EXISTS gold.stg_dim_date (
            date_key     INT,
            full_date    DATE,
            day_of_week  VARCHAR(20),
            day_of_month INT,
            month_number INT,
            month_name   VARCHAR(20),
            quarter      INT,
            year         INT
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_dim_branch (
            branch_id   VARCHAR(50),
            branch_name VARCHAR(255),
            region      VARCHAR(100)
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_dim_product (
            product_id    VARCHAR(50),
            product_name  VARCHAR(255),
            category_name VARCHAR(255),
            start_date    DATE,
            end_date      DATE,
            is_current    SMALLINT
        )""",
        # Staging cho Fact
        """CREATE TABLE IF NOT EXISTS gold.stg_fact_sales (
            date_key         INT,
            product_key      INT,
            branch_key       INT,
            order_id         BIGINT,
            sales_channel    VARCHAR(100),
            customer_segment VARCHAR(100),
            quantity         DECIMAL(18,2),
            unit_price       DECIMAL(18,2),
            unit_cost        DECIMAL(18,2),
            revenue          DECIMAL(18,2),
            total_cost       DECIMAL(18,2),
            profit           DECIMAL(18,2)
        )""",
    ]

    conn = get_dw_conn()
    try:
        cur = conn.cursor()
        for sql in sqls:
            cur.execute(sql)
        conn.commit()
        cur.close()
        print("   => ✅ Staging tables đã sẵn sàng.")
    finally:
        conn.close()

# ============================================================
# 5. DIM_DATE
# ============================================================
def load_dim_date():
    print("\n✨ [DIM_DATE] Đang kiểm tra & nạp...")

    if read_gold_table("gold.dim_date").limit(1).count() > 0:
        print("   => Dim Date đã có dữ liệu. Bỏ qua.")
        return

    df_dates = spark.sql("""
        SELECT explode(sequence(
            to_date('2020-01-01'),
            to_date('2030-12-31'),
            interval 1 day
        )) AS full_date
    """)

    df_dim_date = df_dates.select(
        date_format(col("full_date"), "yyyyMMdd").cast("int").alias("date_key"),
        col("full_date"),
        date_format(col("full_date"), "EEEE").alias("day_of_week"),
        dayofmonth(col("full_date")).alias("day_of_month"),
        month(col("full_date")).alias("month_number"),
        date_format(col("full_date"), "MMMM").alias("month_name"),
        quarter(col("full_date")).alias("quarter"),
        year(col("full_date")).alias("year")
    )

    upsert_to_gold(
        df            = df_dim_date,
        target_table  = "gold.dim_date",
        staging_table = "gold.stg_dim_date",
        conflict_keys = ["date_key"],
        update_cols   = ["full_date", "day_of_week", "day_of_month",
                         "month_number", "month_name", "quarter", "year"]
    )

# ============================================================
# 6. DIM_BRANCH
# ============================================================
def load_dim_branch():
    print("\n✨ [DIM_BRANCH] Đang nạp...")

    df_silver = spark.read.parquet(os.path.join(silver_dir, "branches")) \
        .select("branch_id", "branch_name", "region")

    upsert_to_gold(
        df            = df_silver,
        target_table  = "gold.dim_branch",
        staging_table = "gold.stg_dim_branch",
        conflict_keys = ["branch_id"],
        update_cols   = ["branch_name", "region"]
    )

# ============================================================
# 7. DIM_PRODUCT (SCD Type 2)
# ============================================================
def load_dim_product():
    print("\n✨ [DIM_PRODUCT] Đang nạp (SCD Type 2)...")

    df_products   = spark.read.parquet(os.path.join(silver_dir, "products"))
    df_categories = spark.read.parquet(os.path.join(silver_dir, "categories"))

    df_prod_cat = df_products.join(df_categories, "category_id", "left")

    # Chỉ so sánh với bản ghi đang hiệu lực
    existing_active = read_gold_table("gold.dim_product") \
        .filter(col("is_current") == 1) \
        .select("product_id")

    # Chỉ insert sản phẩm mới, không update SCD ở đây
    # (Update SCD Type 2 cần logic expire bản ghi cũ — để riêng nếu cần)
    df_new = df_prod_cat.join(existing_active, "product_id", "left_anti") \
        .select(
            col("product_id"),
            col("product_name"),
            col("category_name"),
            lit(datetime.now().strftime("%Y-%m-%d")).cast("date").alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(1).cast("smallint").alias("is_current")
        )

    df_new.cache()
    count = df_new.count()
    if count == 0:
        print("   => Không có sản phẩm mới.")
        df_new.unpersist()
        return

    upsert_to_gold(
        df            = df_new,
        target_table  = "gold.dim_product",
        staging_table = "gold.stg_dim_product",
        conflict_keys = ["product_id"],          # thêm UNIQUE constraint trên product_id ở Gold DDL
        update_cols   = ["product_name", "category_name"]
    )
    df_new.unpersist()

# ============================================================
# 8. FACT_SALES
# ============================================================
def load_fact_sales():
    print("\n✨ [FACT_SALES] Đang xử lý & nạp...")

    df_orders  = spark.read.parquet(os.path.join(silver_dir, "orders"))
    df_details = spark.read.parquet(os.path.join(silver_dir, "order_details"))

    # Chỉ lấy bản ghi hiện tại của dim_product để tránh fan-out SCD
    dim_branch  = read_gold_table("gold.dim_branch") \
        .select("branch_key", "branch_id")
    dim_product = read_gold_table("gold.dim_product") \
        .filter(col("is_current") == 1) \
        .select("product_key", "product_id")

    # Join và tính measures
    df_fact = df_orders.join(df_details, "order_id", "inner") \
        .withColumn("revenue",    col("quantity") * col("unit_price")) \
        .withColumn("total_cost", col("quantity") * col("unit_cost")) \
        .withColumn("profit",     col("revenue")  - col("total_cost")) \
        .withColumn("date_key",   date_format(col("order_date"), "yyyyMMdd").cast("int")) \
        .join(dim_branch,  "branch_id",  "left") \
        .join(dim_product, "product_id", "left")

    # Kiểm tra và log các dòng thiếu surrogate key
    df_lost = df_fact.filter(
        col("branch_key").isNull() | col("product_key").isNull()
    )
    df_lost.cache()
    lost_count = df_lost.count()
    if lost_count > 0:
        print(f"   ⚠️  Bỏ qua {lost_count} dòng do thiếu surrogate key!")
        print("   --- Sample dòng bị bỏ qua ---")
        df_lost.select("order_id", "branch_id", "product_id",
                       "branch_key", "product_key").show(5, truncate=False)
    df_lost.unpersist()

    df_final = df_fact.filter(
        col("branch_key").isNotNull() & col("product_key").isNotNull()
    ).select(
        "date_key", "product_key", "branch_key", "order_id",
        "sales_channel", "customer_segment",
        "quantity", "unit_price", "unit_cost",
        "revenue", "total_cost", "profit"
    )

    upsert_to_gold(
        df            = df_final,
        target_table  = "gold.fact_sales",
        staging_table = "gold.stg_fact_sales",
        conflict_keys = ["order_id", "product_key"],
        update_cols   = ["date_key", "branch_key", "sales_channel",
                         "customer_segment", "quantity", "unit_price",
                         "unit_cost", "revenue", "total_cost", "profit"]
    )

# ============================================================
# 9. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    print("\n🔧 Khởi tạo Staging Tables...")
    create_staging_tables()

    load_dim_date()
    load_dim_branch()
    load_dim_product()
    load_fact_sales()

    spark.stop()
    print("\n✅ Hoàn tất Silver → Gold!")