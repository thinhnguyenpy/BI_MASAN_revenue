import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, when, to_date,
    year, month, dayofmonth
)
from pyspark.sql.types import DecimalType

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
silver_dir   = os.path.join(project_root, "datalake", "silver", "sales_db")

print("Starting Spark [SILVER - FACTS]...")
spark = (
    SparkSession.builder
    .appName("Silver_Facts_Sales")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

DIRTY_VALUES = ["", "null", "NULL", "N/A", "n/a", "none", "None", "NaN"]

def clean_string_columns(df):
    for c_name, c_type in df.dtypes:
        if c_type == "string":
            df = df.withColumn(
                c_name,
                when(trim(col(c_name)).isin(DIRTY_VALUES), None)
                .otherwise(trim(col(c_name)))
            )
    return df

def transform_orders(target_date=None):
    print("\n[FACT] Cleaning ORDERS...")

    input_path = os.path.join(bronze_dir, "orders")
    if not os.path.exists(input_path):
        print(f"⚠️  Bronze path not found: {input_path}. Skipping orders transformation.")
        return

    try:

        if target_date:
            df_raw = spark.read.parquet(
                os.path.join(input_path, f"ingest_date={target_date}")
            )

            from pyspark.sql.functions import lit
            df_raw = df_raw.withColumn("ingest_date", lit(target_date))
        else:
            df_raw = spark.read.parquet(input_path)
    except Exception as e:
        print(f"⚠️  Failed to read orders: {e}. Bronze data may be empty or invalid. Skipping.")
        return

    count_raw = df_raw.count()


    df_clean = clean_string_columns(df_raw)


    df_clean = df_clean.withColumn(
        "order_date",
        to_date(col("order_date"), "yyyy-MM-dd")
    )


    df_clean = df_clean.dropna(subset=["order_id", "order_date"])


    df_clean = df_clean.dropDuplicates(["order_id"])

    count_clean = df_clean.count()
    print(f"📊 Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")


    output_path = os.path.join(silver_dir, "orders")
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Orders saved to: {output_path}")


def transform_order_details(target_date=None):
    print("\n[FACT] Cleaning ORDER_DETAILS...")

    input_path = os.path.join(bronze_dir, "order_details")
    if not os.path.exists(input_path):
        print(f"⚠️  Bronze path not found: {input_path}. Skipping order_details transformation.")
        return

    try:
        if target_date:
            df_raw = spark.read.parquet(
                os.path.join(input_path, f"ingest_date={target_date}")
            )
            from pyspark.sql.functions import lit
            df_raw = df_raw.withColumn("ingest_date", lit(target_date))
        else:
            df_raw = spark.read.parquet(input_path)
    except Exception as e:
        print(f"⚠️  Failed to read order_details: {e}. Bronze data may be empty or invalid. Skipping.")
        return

    count_raw = df_raw.count()


    df_clean = clean_string_columns(df_raw)



    df_clean = (
        df_clean
        .withColumn("quantity",   col("quantity").cast(DecimalType(18, 2)))
        .withColumn("unit_price", col("unit_price").cast(DecimalType(18, 2)))
        .withColumn("unit_cost",  col("unit_cost").cast(DecimalType(18, 2)))
    )


    df_clean = (
        df_clean
        .withColumn("quantity",
            when(col("quantity").isNull() | (col("quantity") < 0), 0)
            .otherwise(col("quantity")))
        .withColumn("unit_price",
            when(col("unit_price") < 0, None)
            .otherwise(col("unit_price")))
        .withColumn("unit_cost",
            when(col("unit_cost") < 0, None)
            .otherwise(col("unit_cost")))
    )


    df_clean = df_clean.dropna(subset=["order_id", "product_id"])


    df_clean = df_clean.dropDuplicates(["order_id", "product_id"])

    count_clean = df_clean.count()
    print(f"📊 Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")


    output_path = os.path.join(silver_dir, "order_details")
    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Order Details saved to: {output_path}")


def process_facts(is_incremental=False, target_date=None):
    if is_incremental:
        if target_date is None:
            target_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"\nINCREMENTAL LOAD - date: {target_date}")
    else:
        print("\nFULL LOAD")
        target_date = None

    transform_orders(target_date=target_date)
    transform_order_details(target_date=target_date)

if __name__ == "__main__":

    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)
    if target_date and target_date.lower() == "none":
        target_date = None

    process_facts(is_incremental=is_incremental, target_date=target_date)
    spark.stop()
    print("\nFinished Sales Fact Transformation!")
