import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, when


# ============================================================
# 1. PATH CONFIG
# ============================================================
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir = os.path.join(project_root, "datalake", "bronze", "finance_db")
silver_dir = os.path.join(project_root, "datalake", "silver", "finance_db")


# ============================================================
# 2. INIT SPARK
# ============================================================
print("Starting Spark [SILVER - MYSQL DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("Silver_MySQL_Dimensions")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 3. CLEAN HELPERS
# ============================================================
DIM_CONFIG = {
    "marketing_campaigns": {"pk": "campaign_id", "partition_by": None},
}

DIRTY_VALUES = ["", "null", "NULL", "N/A", "n/a", "none", "None", "NaN"]


def clean_string_columns(df):
    for c_name, c_type in df.dtypes:
        if c_type == "string":
            df = df.withColumn(
                c_name,
                when(trim(col(c_name)).isin(DIRTY_VALUES), None)
                .otherwise(trim(col(c_name))),
            )
    return df


# ============================================================
# 4. TRANSFORM
# ============================================================
def transform_dimensions():
    for table, config in DIM_CONFIG.items():
        print(f"\n[DIM] Cleaning {table.upper()}...")

        input_path = os.path.join(bronze_dir, table)
        if not os.path.exists(input_path):
            print(f"Bronze path not found, skipping: {input_path}")
            continue

        try:
            df_raw = spark.read.parquet(input_path)

            if "ingest_date" in df_raw.columns:
                df_raw = df_raw.drop("ingest_date")

            count_raw = df_raw.count()

            df_clean = clean_string_columns(df_raw)
            df_clean = df_clean.dropna(subset=[config["pk"]])
            df_clean = df_clean.dropDuplicates([config["pk"]])

            count_clean = df_clean.count()
            print(f"Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

            output_path = os.path.join(silver_dir, table)
            writer = df_clean.coalesce(1).write.mode("overwrite")
            if config["partition_by"]:
                writer = writer.partitionBy(config["partition_by"])

            writer.parquet(output_path)
            print(f"[SILVER] Saved to: {output_path}")
        except Exception as e:
            print(f"Failed to process {table.upper()}: {e}")
            raise


if __name__ == "__main__":
    transform_dimensions()
    spark.stop()
    print("\nFinished MySQL Dimension Transformation!")
