import os
import sys
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce as spark_coalesce
from pyspark.sql.functions import col, lit, to_date, trim, when
from pyspark.sql.types import DecimalType


# ============================================================
# 1. PATH CONFIG
# ============================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir = os.path.join(project_root, "datalake", "bronze", "production_db")
silver_dir = os.path.join(project_root, "datalake", "silver", "production_db")

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)


# ============================================================
# 2. INIT SPARK
# ============================================================
print("Starting Spark [SILVER - MONGO FACTS]...")
spark = (
    SparkSession.builder
    .appName("Silver_Mongo_Facts")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 3. CLEAN HELPERS
# ============================================================
DIRTY_VALUES = ["", "null", "NULL", "N/A", "n/a", "none", "None", "NaN", "NaT"]


def clean_string_columns(df):
    for c_name, c_type in df.dtypes:
        if c_type == "string":
            df = df.withColumn(
                c_name,
                when(trim(col(c_name)).isin(DIRTY_VALUES), None)
                .otherwise(trim(col(c_name))),
            )
    return df


def parse_date_column(c_name: str):
    return spark_coalesce(
        to_date(col(c_name)),
        to_date(col(c_name), "yyyy-MM-dd HH:mm:ss"),
        to_date(col(c_name), "yyyy-MM-dd'T'HH:mm:ss"),
        to_date(col(c_name), "MM/dd/yyyy"),
        to_date(col(c_name), "M/d/yyyy"),
    )


def normalize_non_negative_decimal(df, c_name: str):
    return df.withColumn(c_name, col(c_name).cast(DecimalType(18, 2))).withColumn(
        c_name,
        when(col(c_name) < 0, None).otherwise(col(c_name)),
    )


# ============================================================
# 4. TRANSFORM PRODUCTION LOGS
# ============================================================
def transform_production_logs(target_date=None):
    print("\n[FACT] Cleaning PRODUCTION_LOGS...")

    input_path = os.path.join(bronze_dir, "production_logs")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Bronze path not found: {input_path}")

    if target_date:
        df_raw = spark.read.parquet(os.path.join(input_path, f"ingest_date={target_date}"))
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    df_clean = clean_string_columns(df_raw)
    df_clean = df_clean.withColumn("log_date", parse_date_column("log_date"))

    for measure_col in ["inventory_level", "raw_material_cost", "labor_cost"]:
        df_clean = normalize_non_negative_decimal(df_clean, measure_col)

    df_clean = df_clean.dropna(subset=["log_date", "product_id", "department_id", "machine_id"])
    df_clean = df_clean.dropDuplicates()

    count_clean = df_clean.count()
    print(f"Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    output_path = os.path.join(silver_dir, "production_logs")
    (
        df_clean.coalesce(1).write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Production Logs saved to: {output_path}")


# ============================================================
# 5. TRANSFORM LOGISTICS COSTS
# ============================================================
def transform_logistics_costs(target_date=None):
    print("\n[FACT] Cleaning LOGISTICS_COSTS...")

    input_path = os.path.join(bronze_dir, "logistics_costs")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Bronze path not found: {input_path}")

    if target_date:
        df_raw = spark.read.parquet(os.path.join(input_path, f"ingest_date={target_date}"))
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    df_clean = clean_string_columns(df_raw)
    df_clean = df_clean.withColumn("log_date", parse_date_column("log_date"))
    df_clean = normalize_non_negative_decimal(df_clean, "logistics_cost")

    df_clean = df_clean.dropna(subset=["log_date", "branch_name"])
    df_clean = df_clean.dropDuplicates()

    count_clean = df_clean.count()
    print(f"Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    output_path = os.path.join(silver_dir, "logistics_costs")
    (
        df_clean.coalesce(1).write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Logistics Costs saved to: {output_path}")


# ============================================================
# 6. ENTRY POINT
# ============================================================
def process_facts(is_incremental=False, target_date=None):
    if is_incremental:
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"\nINCREMENTAL LOAD - date: {target_date}")
    else:
        print("\nFULL LOAD")
        target_date = None

    transform_production_logs(target_date=target_date)
    transform_logistics_costs(target_date=target_date)


if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)

    process_facts(is_incremental=is_incremental, target_date=target_date)
    spark.stop()
    print("\nFinished MongoDB Fact Transformation!")
