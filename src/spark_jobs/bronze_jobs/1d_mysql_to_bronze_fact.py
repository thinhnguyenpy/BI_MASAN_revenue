import os
import sys
from datetime import datetime

import mysql.connector
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, StringType, StructField, StructType


# ============================================================
# 1. LOAD CONFIG
# ============================================================
load_dotenv()
DB_HOST = os.getenv("MYSQL_HOST", "stg_mysql_finance")
DB_PORT = int(os.getenv("MYSQL_PORT", "3307"))
DB_NAME = os.getenv("MYSQL_DB", "finance_db")
DB_USER = os.getenv("MYSQL_USER", "admin")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD", "admin_password")

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir = os.path.join(project_root, "datalake", "bronze", "finance_db")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ============================================================
# 2. SOURCE SCHEMAS
# Numeric measure columns are read as strings so Silver can clean and cast them.
# ============================================================
SCHEMAS = {
    "daily_marketing_spend": StructType([
        StructField("spend_id", LongType(), True),
        StructField("campaign_id", LongType(), True),
        StructField("spend_date", StringType(), True),
        StructField("region", StringType(), True),
        StructField("amount_spent", StringType(), True),
    ]),
    "monthly_budgets": StructType([
        StructField("budget_id", LongType(), True),
        StructField("month_year", StringType(), True),
        StructField("region", StringType(), True),
        StructField("budget_amount", StringType(), True),
        StructField("target_revenue", StringType(), True),
        StructField("market_size", StringType(), True),
    ]),
}


# ============================================================
# 3. INIT SPARK
# ============================================================
print("Starting Spark [BRONZE - MYSQL FACTS]...")
spark = (
    SparkSession.builder
    .appName("MySQL_Bronze_Facts")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 4. MYSQL READER
# ============================================================
def read_from_source(query: str, schema: StructType):
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
    )
    try:
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return spark.createDataFrame(rows, schema=schema)


# ============================================================
# 5. INGEST
# ============================================================
def ingest_facts(target_date: str, is_incremental: bool = True):
    print(f"\nINGEST DATE: {target_date}")
    print(f"RUN MODE: {'INCREMENTAL LOAD' if is_incremental else 'FULL LOAD'}")

    if is_incremental:
        target_month = target_date[:7]
        queries = {
            "daily_marketing_spend": f"""
                SELECT
                    spend_id,
                    campaign_id,
                    spend_date,
                    region,
                    CAST(amount_spent AS CHAR) AS amount_spent
                FROM daily_marketing_spend
                WHERE spend_date LIKE '%{target_date}%'
            """,
            "monthly_budgets": f"""
                SELECT
                    budget_id,
                    month_year,
                    region,
                    CAST(budget_amount AS CHAR) AS budget_amount,
                    CAST(target_revenue AS CHAR) AS target_revenue,
                    CAST(market_size AS CHAR) AS market_size
                FROM monthly_budgets
                WHERE month_year = '{target_month}'
            """,
        }
    else:
        queries = {
            "daily_marketing_spend": """
                SELECT
                    spend_id,
                    campaign_id,
                    spend_date,
                    region,
                    CAST(amount_spent AS CHAR) AS amount_spent
                FROM daily_marketing_spend
            """,
            "monthly_budgets": """
                SELECT
                    budget_id,
                    month_year,
                    region,
                    CAST(budget_amount AS CHAR) AS budget_amount,
                    CAST(target_revenue AS CHAR) AS target_revenue,
                    CAST(market_size AS CHAR) AS market_size
                FROM monthly_budgets
            """,
        }

    for table, query in queries.items():
        try:
            print(f"\n[FACT] Reading {table.upper()}...")
            df_raw = read_from_source(query, SCHEMAS[table])
            count = df_raw.count()

            df_partitioned = df_raw.withColumn("ingest_date", lit(target_date))
            output_path = os.path.join(bronze_dir, table)

            (
                df_partitioned.coalesce(1).write
                .mode("overwrite")
                .partitionBy("ingest_date")
                .parquet(output_path)
            )

            print(f"Saved {count} rows to: {output_path}/ingest_date={target_date}")
        except Exception as e:
            print(f"Failed to process {table.upper()}: {e}")
            raise


if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    ingest_facts(target_date=target_date, is_incremental=is_incremental)
    spark.stop()
    print("\nFinished MySQL Fact Ingestion!")
