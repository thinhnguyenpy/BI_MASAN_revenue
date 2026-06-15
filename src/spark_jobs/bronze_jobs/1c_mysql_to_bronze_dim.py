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

print(f"project_root : {project_root}")
print(f"bronze_dir   : {bronze_dir}")


# ============================================================
# 2. INIT SPARK
# ============================================================
print("\nStarting Spark [BRONZE - MYSQL DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("MySQL_Bronze_Dimensions")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 3. MYSQL READER
# ============================================================
SCHEMAS = {
    "marketing_campaigns": StructType([
        StructField("campaign_id", LongType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("platform", StringType(), True),
    ]),
}


def read_from_source(query: str, schema: StructType):
    """Read MySQL rows with mysql-connector and convert them to a Spark DataFrame."""
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
# 4. INGEST
# ============================================================
def ingest_dimensions(ingest_date: str):
    dim_queries = {
        "marketing_campaigns": """
            SELECT
                campaign_id,
                campaign_name,
                platform
            FROM marketing_campaigns
        """,
    }

    print(f"\nINGEST DATE: {ingest_date}")
    print(f"Tables: {list(dim_queries.keys())}")

    success_tables = []
    failed_tables = []

    for table, query in dim_queries.items():
        try:
            print(f"\n[DIM] Reading {table.upper()}...")
            df_raw = read_from_source(query, SCHEMAS[table])
            count = df_raw.count()

            df_partitioned = df_raw.withColumn("ingest_date", lit(ingest_date))
            output_path = os.path.join(bronze_dir, table)

            (
                df_partitioned.coalesce(1).write
                .mode("overwrite")
                .partitionBy("ingest_date")
                .parquet(output_path)
            )

            print(f"Saved {count} rows to: {output_path}/ingest_date={ingest_date}")
            success_tables.append(table)
        except Exception as e:
            print(f"Failed to process {table.upper()}: {e}")
            failed_tables.append(table)

    print(f"\n{'=' * 50}")
    print(f"Success: {success_tables}")
    if failed_tables:
        print(f"Failed : {failed_tables}")
        raise RuntimeError(f"Some tables failed: {failed_tables}")


# ============================================================
# 5. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    ingest_dimensions(ingest_date=run_date)
    spark.stop()
    print("\nFinished MySQL Dimension Ingestion!")
