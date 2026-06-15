import os

from dotenv import load_dotenv
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, sum as spark_sum,
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
finance_silver_dir = os.path.join(project_root, "datalake", "silver", "finance_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")


# ============================================================
# 2. INIT SPARK
# ============================================================
print("Starting Spark [GOLD - MYSQL FINANCE/MARKETING]...")
spark = (
    SparkSession.builder
    .appName("Silver_To_Gold_MySQL_Finance")
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
        """CREATE TABLE IF NOT EXISTS gold.stg_dim_campaign (
            campaign_id   INT,
            campaign_name VARCHAR(255),
            platform      VARCHAR(100)
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_fact_marketing_spend (
            date_key     INT,
            campaign_key INT,
            region       VARCHAR(100),
            daily_spend  DECIMAL(18,2)
        )""",
        """CREATE TABLE IF NOT EXISTS gold.stg_fact_monthly_budget (
            date_key       INT,
            region         VARCHAR(100),
            budget_amount  DECIMAL(18,2),
            target_revenue DECIMAL(18,2),
            market_size    DECIMAL(18,2)
        )""",
    ]

    conn = get_dw_conn()
    try:
        cur = conn.cursor()
        for sql in sqls:
            cur.execute(sql)
        conn.commit()
        cur.close()
        print("   => MySQL staging tables are ready.")
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
# 6. DIM_CAMPAIGN
# ============================================================
def load_dim_campaign():
    print("\n[DIM_CAMPAIGN] Loading...")

    input_path = os.path.join(finance_silver_dir, "marketing_campaigns")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_silver = (
        spark.read.parquet(input_path)
        .select("campaign_id", "campaign_name", "platform")
    )

    upsert_to_gold(
        df=df_silver,
        target_table="gold.dim_campaign",
        staging_table="gold.stg_dim_campaign",
        conflict_keys=["campaign_id"],
        update_cols=["campaign_name", "platform"]
    )


# ============================================================
# 7. FACT_MARKETING_SPEND
# ============================================================
def load_fact_marketing_spend():
    print("\n[FACT_MARKETING_SPEND] Processing and loading...")

    # read partition when incremental
    target_date = os.getenv("TARGET_DATE", None)
    if target_date:
        input_path = os.path.join(finance_silver_dir, "daily_marketing_spend", f"ingest_date={target_date}")
    else:
        input_path = os.path.join(finance_silver_dir, "daily_marketing_spend")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_spend = spark.read.parquet(input_path)
    dim_campaign = read_gold_table("gold.dim_campaign").select("campaign_key", "campaign_id")

    df_fact = (
        df_spend
        .withColumn("date_key", date_format(col("spend_date"), "yyyyMMdd").cast("int"))
        .join(dim_campaign, "campaign_id", "left")
    )

    df_lost = df_fact.filter(
        col("date_key").isNull() | col("campaign_key").isNull() | col("region").isNull()
    )
    df_lost.cache()
    lost_count = df_lost.count()
    if lost_count > 0:
        print(f"   => Skipping {lost_count} rows missing date_key/campaign_key/region.")
        df_lost.select(
            "spend_id", "campaign_id", "spend_date", "region",
            "date_key", "campaign_key"
        ).show(5, truncate=False)
    df_lost.unpersist()

    df_final = (
        df_fact
        .filter(
            col("date_key").isNotNull()
            & col("campaign_key").isNotNull()
            & col("region").isNotNull()
        )
        .select(
            "date_key",
            "campaign_key",
            "region",
            col("amount_spent").alias("daily_spend")
        )
        .groupBy("date_key", "campaign_key", "region")
        .agg(spark_sum("daily_spend").alias("daily_spend"))
    )

    upsert_to_gold(
        df=df_final,
        target_table="gold.fact_marketing_spend",
        staging_table="gold.stg_fact_marketing_spend",
        conflict_keys=["date_key", "campaign_key", "region"],
        update_cols=["daily_spend"]
    )


# ============================================================
# 8. FACT_MONTHLY_BUDGET
# ============================================================
def load_fact_monthly_budget():
    print("\n[FACT_MONTHLY_BUDGET] Processing and loading...")
    # read partition when incremental
    target_date = os.getenv("TARGET_DATE", None)
    if target_date:
        input_path = os.path.join(finance_silver_dir, "monthly_budgets", f"ingest_date={target_date}")
    else:
        input_path = os.path.join(finance_silver_dir, "monthly_budgets")
    if not os.path.exists(input_path):
        print(f"   => Silver path not found: {input_path}. Skipping.")
        return

    df_budget = spark.read.parquet(input_path)

    df_final = (
        df_budget
        .withColumn("date_key", date_format(col("budget_month"), "yyyyMMdd").cast("int"))
        .filter(col("date_key").isNotNull() & col("region").isNotNull())
        .select(
            "date_key",
            "region",
            "budget_amount",
            "target_revenue",
            "market_size"
        )
        .dropDuplicates(["date_key", "region"])
    )

    upsert_to_gold(
        df=df_final,
        target_table="gold.fact_monthly_budget",
        staging_table="gold.stg_fact_monthly_budget",
        conflict_keys=["date_key", "region"],
        update_cols=["budget_amount", "target_revenue", "market_size"]
    )


# ============================================================
# 9. ENTRY POINT
# ============================================================
if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    _is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)

    print("\nInitializing MySQL staging tables...")
    create_staging_tables()

    load_dim_date()
    load_dim_campaign()
    load_fact_marketing_spend()
    load_fact_monthly_budget()

    spark.stop()
    print("\nFinished Silver to Gold MySQL Finance/Marketing!")
