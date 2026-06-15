import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit, to_date, trim, when
from pyspark.sql.types import DecimalType


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
print("Starting Spark [SILVER - MYSQL FACTS]...")
spark = (
    SparkSession.builder
    .appName("Silver_MySQL_Facts")
    .master("local[*]")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# 3. CLEAN HELPERS
# ============================================================
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


def parse_date_column(c_name: str):
    return to_date(col(c_name))


# ============================================================
# 4. TRANSFORM DAILY MARKETING SPEND
# ============================================================
def transform_daily_marketing_spend(target_date=None):
    print("\n[FACT] Cleaning DAILY_MARKETING_SPEND...")

    input_path = os.path.join(bronze_dir, "daily_marketing_spend")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Bronze path not found: {input_path}")

    if target_date:
        df_raw = spark.read.parquet(os.path.join(input_path, f"ingest_date={target_date}"))
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    df_clean = clean_string_columns(df_raw)
    df_clean = df_clean.withColumn("spend_date", parse_date_column("spend_date"))
    df_clean = df_clean.withColumn("amount_spent", col("amount_spent").cast(DecimalType(18, 2)))

    df_clean = df_clean.withColumn(
        "amount_spent",
        when(col("amount_spent") < 0, None).otherwise(col("amount_spent")),
    )

    df_clean = df_clean.dropna(subset=["spend_id", "campaign_id", "spend_date", "region"])
    df_clean = df_clean.dropDuplicates(["spend_id"])

    count_clean = df_clean.count()
    print(f"Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    output_path = os.path.join(silver_dir, "daily_marketing_spend")
    (
        df_clean.coalesce(1).write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Daily Marketing Spend saved to: {output_path}")


# ============================================================
# 5. TRANSFORM MONTHLY BUDGETS
# ============================================================
def transform_monthly_budgets(target_date=None):
    print("\n[FACT] Cleaning MONTHLY_BUDGETS...")

    input_path = os.path.join(bronze_dir, "monthly_budgets")
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Bronze path not found: {input_path}")

    if target_date:
        df_raw = spark.read.parquet(os.path.join(input_path, f"ingest_date={target_date}"))
        df_raw = df_raw.withColumn("ingest_date", lit(target_date))
    else:
        df_raw = spark.read.parquet(input_path)

    count_raw = df_raw.count()

    df_clean = clean_string_columns(df_raw)
    df_clean = (
        df_clean
        .withColumn("budget_month", to_date(concat(col("month_year"), lit("-01")), "yyyy-MM-dd"))
        .withColumn("budget_amount", col("budget_amount").cast(DecimalType(18, 2)))
        .withColumn("target_revenue", col("target_revenue").cast(DecimalType(18, 2)))
        .withColumn("market_size", col("market_size").cast(DecimalType(18, 2)))
    )

    for measure_col in ["budget_amount", "target_revenue", "market_size"]:
        df_clean = df_clean.withColumn(
            measure_col,
            when(col(measure_col) < 0, None).otherwise(col(measure_col)),
        )

    df_clean = df_clean.dropna(subset=["budget_id", "budget_month", "region"])
    df_clean = df_clean.dropDuplicates(["month_year", "region"])

    count_clean = df_clean.count()
    print(f"Raw: {count_raw} | Clean: {count_clean} | Dropped: {count_raw - count_clean}")

    output_path = os.path.join(silver_dir, "monthly_budgets")
    (
        df_clean.coalesce(1).write
        .mode("overwrite")
        .partitionBy("ingest_date")
        .parquet(output_path)
    )
    print(f"[SILVER] Monthly Budgets saved to: {output_path}")


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

    transform_daily_marketing_spend(target_date=target_date)
    transform_monthly_budgets(target_date=target_date)


if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)

    process_facts(is_incremental=is_incremental, target_date=target_date)
    spark.stop()
    print("\nFinished MySQL Fact Transformation!")
