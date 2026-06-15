import os
import sys

from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce as spark_coalesce
from pyspark.sql.functions import (
    col, date_format, dayofmonth, lit, month, quarter,
    sum as spark_sum, year
)
from pyspark.sql.types import DecimalType


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
production_silver_dir = os.path.join(project_root, "datalake", "silver", "production_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ============================================================
# 2. INIT SPARK
# ============================================================
print("Starting Spark [GOLD - MONGO PRODUCTION/LOGISTICS]...")
spark = (
    SparkSession.builder
    .appName("Silver_To_Gold_Mongo_Production")
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
    return psycopg2.connect(
        host=DW_HOST, port=DW_PORT,
        dbname=DW_NAME, user=DW_USER, password=DW_PASSWORD
    )


def read_gold_table(table_name):
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
        """CREATE TABLE IF NOT EXISTS gold.stg_dim_department (
            department_id   INT,
            department_name VARCHAR(255)
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_fact_production_logs (
            date_key          INT,
            product_key       INT,
            department_key    INT,
            machine_id        VARCHAR(100),
            inventory_level   DECIMAL(18,2),
            raw_material_cost DECIMAL(18,2),
            labor_cost        DECIMAL(18,2)
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_fact_logistics_costs (
            date_key       INT,
            branch_key     INT,
            logistics_cost DECIMAL(18,2)
        )""",
        """CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_production_logs_pk
            ON gold.fact_production_logs(date_key, product_key, department_key, machine_id)""",
        """CREATE UNIQUE INDEX IF NOT EXISTS uq_fact_logistics_costs_idx
            ON gold.fact_logistics_costs(date_key, branch_key)""",
    ]

    conn = get_dw_conn()
    try:
        cur = conn.cursor()
        for sql in sqls:
            cur.execute(sql)
        conn.commit()
        cur.close()
        print("   => Mongo staging tables and indexes are ready.")
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
# 6. DIM_DEPARTMENT
# ============================================================
def load_dim_department():
    print("\n[DIM_DEPARTMENT] Loading...")

    input_path = os.path.join(production_silver_dir, "departments")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_silver = (
        spark.read.parquet(input_path)
        .select("department_id", "department_name")
    )

    upsert_to_gold(
        df=df_silver,
        target_table="gold.dim_department",
        staging_table="gold.stg_dim_department",
        conflict_keys=["department_id"],
        update_cols=["department_name"]
    )


def zero_if_null(c_name: str):
    return spark_coalesce(col(c_name), lit(0).cast(DecimalType(18, 2)))


# ============================================================
# 7. FACT_PRODUCTION_LOGS
# ============================================================
def load_fact_production_logs():
    print("\n[FACT_PRODUCTION_LOGS] Processing and loading...")
    # read partition when incremental
    target_date = os.getenv("TARGET_DATE", None)
    if target_date:
        input_path = os.path.join(production_silver_dir, "production_logs", f"ingest_date={target_date}")
    else:
        input_path = os.path.join(production_silver_dir, "production_logs")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_logs = spark.read.parquet(input_path)
    dim_department = read_gold_table("gold.dim_department").select(
        "department_key", "department_id"
    )
    dim_product = (
        read_gold_table("gold.dim_product")
        .filter(col("is_current") == 1)
        .select("product_key", "product_id")
    )

    df_fact = (
        df_logs
        .withColumn("date_key", date_format(col("log_date"), "yyyyMMdd").cast("int"))
        .join(dim_product, "product_id", "left")
        .join(dim_department, "department_id", "left")
    )

    df_lost = df_fact.filter(
        col("date_key").isNull()
        | col("product_key").isNull()
        | col("department_key").isNull()
        | col("machine_id").isNull()
    )
    df_lost.cache()
    lost_count = df_lost.count()
    if lost_count > 0:
        print(f"   => Skipping {lost_count} rows missing date/product/department/machine key.")
        df_lost.select(
            "log_date", "product_id", "department_id", "machine_id",
            "date_key", "product_key", "department_key"
        ).show(5, truncate=False)
    df_lost.unpersist()

    df_final = (
        df_fact
        .filter(
            col("date_key").isNotNull()
            & col("product_key").isNotNull()
            & col("department_key").isNotNull()
            & col("machine_id").isNotNull()
        )
        .select(
            "date_key",
            "product_key",
            "department_key",
            "machine_id",
            zero_if_null("inventory_level").alias("inventory_level"),
            zero_if_null("raw_material_cost").alias("raw_material_cost"),
            zero_if_null("labor_cost").alias("labor_cost"),
        )
        .groupBy("date_key", "product_key", "department_key", "machine_id")
        .agg(
            spark_sum("inventory_level").alias("inventory_level"),
            spark_sum("raw_material_cost").alias("raw_material_cost"),
            spark_sum("labor_cost").alias("labor_cost"),
        )
    )

    upsert_to_gold(
        df=df_final,
        target_table="gold.fact_production_logs",
        staging_table="gold.stg_fact_production_logs",
        conflict_keys=["date_key", "product_key", "department_key", "machine_id"],
        update_cols=["inventory_level", "raw_material_cost", "labor_cost"]
    )


# ============================================================
# 8. FACT_LOGISTICS_COSTS
# ============================================================
def load_fact_logistics_costs():
    print("\n[FACT_LOGISTICS_COSTS] Processing and loading...")
    # read partition when incremental
    target_date = os.getenv("TARGET_DATE", None)
    if target_date:
        input_path = os.path.join(production_silver_dir, "logistics_costs", f"ingest_date={target_date}")
    else:
        input_path = os.path.join(production_silver_dir, "logistics_costs")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_costs = spark.read.parquet(input_path)
    dim_branch = read_gold_table("gold.dim_branch").select("branch_key", "branch_name")

    df_fact = (
        df_costs
        .withColumn("date_key", date_format(col("log_date"), "yyyyMMdd").cast("int"))
        .join(dim_branch, "branch_name", "left")
    )

    df_lost = df_fact.filter(col("date_key").isNull() | col("branch_key").isNull())
    df_lost.cache()
    lost_count = df_lost.count()
    if lost_count > 0:
        print(f"   => Skipping {lost_count} rows missing date/branch key.")
        df_lost.select(
            "log_date", "branch_name", "date_key", "branch_key"
        ).show(5, truncate=False)
    df_lost.unpersist()

    df_final = (
        df_fact
        .filter(col("date_key").isNotNull() & col("branch_key").isNotNull())
        .select(
            "date_key",
            "branch_key",
            zero_if_null("logistics_cost").alias("logistics_cost"),
        )
        .groupBy("date_key", "branch_key")
        .agg(spark_sum("logistics_cost").alias("logistics_cost"))
    )

    upsert_to_gold(
        df=df_final,
        target_table="gold.fact_logistics_costs",
        staging_table="gold.stg_fact_logistics_costs",
        conflict_keys=["date_key", "branch_key"],
        update_cols=["logistics_cost"]
    )


# ============================================================
# 9. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    _is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)

    print("\nInitializing Mongo staging tables...")
    create_staging_tables()

    load_dim_date()
    load_dim_department()
    load_fact_production_logs()
    load_fact_logistics_costs()

    spark.stop()
    print("\nFinished Silver to Gold MongoDB Production/Logistics!")
