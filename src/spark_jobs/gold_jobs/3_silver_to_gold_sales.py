import os
from datetime import datetime

from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, date_format,
    year, month, dayofmonth, quarter
)


# ============================================================
# 1. CONFIG
# ============================================================
load_dotenv()
DW_HOST = os.getenv("DW_HOST", "dwh_postgres_gold")
DW_PORT = os.getenv("DW_PORT", "5434")
DW_NAME = os.getenv("DW_DB", "datawarehouse")
DW_USER = os.getenv("DW_USER")
DW_PASSWORD = os.getenv("DW_PASSWORD")
DW_JDBC_URL = f"jdbc:postgresql://{DW_HOST}:{DW_PORT}/{DW_NAME}"

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
silver_dir = os.path.join(project_root, "datalake", "silver", "sales_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")


# ============================================================
# 2. INIT SPARK
# ============================================================
print("Starting Spark [GOLD - SALES DATA WAREHOUSE]...")
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
# 3. JDBC HELPERS
# ============================================================
def get_dw_conn():
    """Create a psycopg2 connection to the Data Warehouse."""
    return psycopg2.connect(
        host=DW_HOST, port=DW_PORT,
        dbname=DW_NAME, user=DW_USER, password=DW_PASSWORD
    )


def read_gold_table(table_name):
    """Read a Gold table into a Spark DataFrame."""
    return (
        spark.read.format("jdbc")
        .option("url", DW_JDBC_URL)
        .option("dbtable", table_name)
        .option("user", DW_USER)
        .option("password", DW_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def write_staging(df, staging_table):
    """Overwrite a staging table before the final upsert."""
    (
        df.write.format("jdbc")
        .option("url", DW_JDBC_URL)
        .option("dbtable", staging_table)
        .option("user", DW_USER)
        .option("password", DW_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("overwrite")
        .save()
    )


def upsert_to_gold(df, target_table, staging_table, conflict_keys: list, update_cols: list):
    """
    Pattern: write to staging, then INSERT ... ON CONFLICT DO UPDATE.
    This keeps the job idempotent when it is re-run.
    """
    write_staging(df, staging_table)
    print(f"   => Wrote {df.count()} rows to staging: {staging_table}")

    col_list = ", ".join(df.columns)
    conflict_str = ", ".join(conflict_keys)
    update_str = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])

    sql = f"""
        INSERT INTO {target_table} ({col_list})
        SELECT {col_list} FROM {staging_table}
        ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}
    """

    conn = get_dw_conn()
    try:
        cur = conn.cursor()
        cur.execute(sql)
        affected = cur.rowcount
        conn.commit()
        cur.close()
        print(f"   => Upserted into {target_table} ({affected} rows affected)")
    except Exception as e:
        conn.rollback()
        print(f"   => Failed to upsert into {target_table}: {e}")
        raise
    finally:
        conn.close()


# ============================================================
# 4. STAGING TABLES
# ============================================================
def create_staging_tables():
    """Create sales staging tables if they do not exist."""
    sqls = [
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
        print("   => Sales staging tables are ready.")
    finally:
        conn.close()


# ============================================================
# 5. DIM_DATE
# ============================================================
def load_dim_date():
    print("\n[DIM_DATE] Checking and loading...")

    if read_gold_table("gold.dim_date").limit(1).count() > 0:
        print("   => Dim Date already has data. Skipping.")
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
        df=df_dim_date,
        target_table="gold.dim_date",
        staging_table="gold.stg_dim_date",
        conflict_keys=["date_key"],
        update_cols=["full_date", "day_of_week", "day_of_month",
                     "month_number", "month_name", "quarter", "year"]
    )


# ============================================================
# 6. DIM_BRANCH
# ============================================================
def load_dim_branch():
    print("\n[DIM_BRANCH] Loading...")

    df_silver = (
        spark.read.parquet(os.path.join(silver_dir, "branches"))
        .select("branch_id", "branch_name", "region")
    )

    upsert_to_gold(
        df=df_silver,
        target_table="gold.dim_branch",
        staging_table="gold.stg_dim_branch",
        conflict_keys=["branch_id"],
        update_cols=["branch_name", "region"]
    )


# ============================================================
# 7. DIM_PRODUCT
# ============================================================
def load_dim_product():
    print("\n[DIM_PRODUCT] Loading...")

    df_products = spark.read.parquet(os.path.join(silver_dir, "products"))
    df_categories = spark.read.parquet(os.path.join(silver_dir, "categories"))

    df_prod_cat = df_products.join(df_categories, "category_id", "left")

    existing_active = (
        read_gold_table("gold.dim_product")
        .filter(col("is_current") == 1)
        .select("product_id")
    )

    df_new = (
        df_prod_cat.join(existing_active, "product_id", "left_anti")
        .select(
            col("product_id"),
            col("product_name"),
            col("category_name"),
            lit(datetime.now().strftime("%Y-%m-%d")).cast("date").alias("start_date"),
            lit(None).cast("date").alias("end_date"),
            lit(1).cast("smallint").alias("is_current")
        )
    )

    df_new.cache()
    count = df_new.count()
    if count == 0:
        print("   => No new products.")
        df_new.unpersist()
        return

    upsert_to_gold(
        df=df_new,
        target_table="gold.dim_product",
        staging_table="gold.stg_dim_product",
        conflict_keys=["product_id"],
        update_cols=["product_name", "category_name"]
    )
    df_new.unpersist()


# ============================================================
# 8. FACT_SALES
# ============================================================
def load_fact_sales():
    print("\n[FACT_SALES] Processing and loading...")

    # Nếu chạy incremental từ Airflow, chỉ đọc partition của `target_date`
    target_date = os.getenv("TARGET_DATE", None)
    if target_date:
        df_orders = spark.read.parquet(os.path.join(silver_dir, "orders", f"ingest_date={target_date}"))
        df_details = spark.read.parquet(os.path.join(silver_dir, "order_details", f"ingest_date={target_date}"))
    else:
        df_orders = spark.read.parquet(os.path.join(silver_dir, "orders"))
        df_details = spark.read.parquet(os.path.join(silver_dir, "order_details"))

    dim_branch = read_gold_table("gold.dim_branch").select("branch_key", "branch_id")
    dim_product = (
        read_gold_table("gold.dim_product")
        .filter(col("is_current") == 1)
        .select("product_key", "product_id")
    )

    df_fact = (
        df_orders.join(df_details, "order_id", "inner")
        .withColumn("revenue", col("quantity") * col("unit_price"))
        .withColumn("total_cost", col("quantity") * col("unit_cost"))
        .withColumn("profit", col("revenue") - col("total_cost"))
        .withColumn("date_key", date_format(col("order_date"), "yyyyMMdd").cast("int"))
        .join(dim_branch, "branch_id", "left")
        .join(dim_product, "product_id", "left")
    )

    df_lost = df_fact.filter(
        col("branch_key").isNull() | col("product_key").isNull()
    )
    df_lost.cache()
    lost_count = df_lost.count()
    if lost_count > 0:
        print(f"   => Skipping {lost_count} rows missing surrogate keys.")
        df_lost.select(
            "order_id", "branch_id", "product_id",
            "branch_key", "product_key"
        ).show(5, truncate=False)
    df_lost.unpersist()

    df_final = (
        df_fact
        .filter(col("branch_key").isNotNull() & col("product_key").isNotNull())
        .select(
            "date_key", "product_key", "branch_key", "order_id",
            "sales_channel", "customer_segment",
            "quantity", "unit_price", "unit_cost",
            "revenue", "total_cost", "profit"
        )
        .dropDuplicates(["order_id", "product_key"])
    )

    upsert_to_gold(
        df=df_final,
        target_table="gold.fact_sales",
        staging_table="gold.stg_fact_sales",
        conflict_keys=["order_id", "product_key"],
        update_cols=["date_key", "branch_key", "sales_channel",
                     "customer_segment", "quantity", "unit_price",
                     "unit_cost", "revenue", "total_cost", "profit"]
    )


# ============================================================
# 9. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    _is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)

    print("\nInitializing Sales staging tables...")
    create_staging_tables()

    load_dim_date()
    load_dim_branch()
    load_dim_product()
    load_fact_sales()

    spark.stop()
    print("\nFinished Silver to Gold Sales!")
