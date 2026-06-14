import os
import sys
from datetime import datetime

from dotenv import load_dotenv
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import LongType, StringType, StructField, StructType


# ============================================================
# 1. LOAD CONFIG
# ============================================================
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI", "mongodb://admin:admin_password@localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "production_db")

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir = os.path.join(project_root, "datalake", "bronze", "production_db")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

print(f"project_root : {project_root}")
print(f"bronze_dir   : {bronze_dir}")


# ============================================================
# 2. INIT SPARK
# ============================================================
print("\nStarting Spark [BRONZE - MONGO DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("Mongo_Bronze_Dimensions")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 3. SOURCE SCHEMAS
# ============================================================
SCHEMAS = {
    "departments": StructType([
        StructField("department_id", LongType(), True),
        StructField("department_name", StringType(), True),
    ]),
}


def read_collection(collection_name: str, schema: StructType):
    """Read MongoDB documents and convert them into a Spark DataFrame."""
    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        docs = list(db[collection_name].find({}, {"_id": 0}))
    finally:
        client.close()

    rows = []
    for doc in docs:
        if collection_name == "Departments":
            rows.append({
                "department_id": doc.get("DepartmentID"),
                "department_name": doc.get("DepartmentName"),
            })

    return spark.createDataFrame(rows, schema=schema)


# ============================================================
# 4. INGEST
# ============================================================
def ingest_dimensions(ingest_date: str):
    dim_collections = {
        "departments": "Departments",
    }

    print(f"\nINGEST DATE: {ingest_date}")
    print(f"Collections: {list(dim_collections.values())}")

    success_tables = []
    failed_tables = []

    for table, collection in dim_collections.items():
        try:
            print(f"\n[DIM] Reading Mongo collection {collection}...")
            df_raw = read_collection(collection, SCHEMAS[table])
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
            print(f"Failed to process {collection}: {e}")
            failed_tables.append(table)

    print(f"\n{'=' * 50}")
    print(f"Success: {success_tables}")
    if failed_tables:
        print(f"Failed : {failed_tables}")
        raise RuntimeError(f"Some collections failed: {failed_tables}")


# ============================================================
# 5. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    ingest_dimensions(ingest_date=run_date)
    spark.stop()
    print("\nFinished MongoDB Dimension Ingestion!")
