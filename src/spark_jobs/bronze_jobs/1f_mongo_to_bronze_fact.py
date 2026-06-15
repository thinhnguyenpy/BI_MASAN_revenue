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


# ============================================================
# 2. SOURCE SCHEMAS
# Numeric measure columns are read as strings so Silver can clean and cast them.
# ============================================================
SCHEMAS = {
    "production_logs": StructType([
        StructField("log_date", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("department_id", LongType(), True),
        StructField("stage", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("inventory_level", StringType(), True),
        StructField("raw_material_cost", StringType(), True),
        StructField("labor_cost", StringType(), True),
    ]),
    "logistics_costs": StructType([
        StructField("log_date", StringType(), True),
        StructField("branch_name", StringType(), True),
        StructField("logistics_cost", StringType(), True),
    ]),
}


# ============================================================
# 3. INIT SPARK
# ============================================================
print("Starting Spark [BRONZE - MONGO FACTS]...")
spark = (
    SparkSession.builder
    .appName("Mongo_Bronze_Facts")
    .master("local[*]")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 4. MONGO READER
# ============================================================
def to_string(value):
    return None if value is None else str(value)


def read_collection(collection_name: str, schema: StructType, target_date: str = None):
    client = MongoClient(MONGO_URI)
    try:
        db = client[MONGO_DB]
        # Filter by LogDate if target_date provided for incremental load
        query_filter = {}
        if target_date:
            query_filter = {"LogDate": target_date}
        docs = list(db[collection_name].find(query_filter, {"_id": 0}))
    finally:
        client.close()

    rows = []
    for doc in docs:
        if collection_name == "ProductionLogs":
            rows.append({
                "log_date": to_string(doc.get("LogDate")),
                "product_id": to_string(doc.get("ProductID")),
                "department_id": doc.get("DepartmentID"),
                "stage": to_string(doc.get("Stage")),
                "machine_id": to_string(doc.get("Machine")),
                "inventory_level": to_string(doc.get("InventoryLevel")),
                "raw_material_cost": to_string(doc.get("RawMaterialCost")),
                "labor_cost": to_string(doc.get("LaborCost")),
            })
        elif collection_name == "LogisticsCosts":
            rows.append({
                "log_date": to_string(doc.get("LogDate")),
                "branch_name": to_string(doc.get("BranchName")),
                "logistics_cost": to_string(doc.get("LogisticsCost")),
            })

    return spark.createDataFrame(rows, schema=schema)


# ============================================================
# 5. INGEST
# ============================================================
def ingest_facts(target_date: str):
    fact_collections = {
        "production_logs": "ProductionLogs",
        "logistics_costs": "LogisticsCosts",
    }

    print(f"\nINGEST DATE: {target_date}")
    print(f"Collections: {list(fact_collections.values())}")

    for table, collection in fact_collections.items():
        try:
            print(f"\n[FACT] Reading Mongo collection {collection}...")
            df_raw = read_collection(collection, SCHEMAS[table], target_date=target_date)
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
            print(f"Failed to process {collection}: {e}")
            raise


if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    _is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    ingest_facts(target_date=target_date)
    spark.stop()
    print("\nFinished MongoDB Fact Ingestion!")
