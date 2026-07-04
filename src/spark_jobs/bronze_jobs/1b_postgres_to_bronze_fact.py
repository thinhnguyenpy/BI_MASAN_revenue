import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DecimalType
)


load_dotenv()
DB_HOST     = os.getenv("POSTGRES_HOST", "stg_postgres_sales")
DB_PORT     = os.getenv("POSTGRES_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB",   "sales_db")
DB_USER     = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
JDBC_URL    = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
jars_dir     = os.path.join(project_root, "jars")
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
postgres_jar = os.path.join(jars_dir, "postgresql-42.7.3.jar")


SCHEMAS = {
    "orders": StructType([
        StructField("order_id",         LongType(),   True),
        StructField("order_date",        StringType(), True),
        StructField("branch_id",         StringType(), True),
        StructField("sales_channel",     StringType(), True),
        StructField("customer_segment",  StringType(), True),
    ]),

    "order_details": StructType([
        StructField("order_id",   LongType(),   True),
        StructField("product_id", StringType(), True),
        StructField("quantity",   StringType(), True),
        StructField("unit_price", StringType(), True),
        StructField("unit_cost",  StringType(), True),
    ]),
}


print("Starting Spark [BRONZE - FACTS]...")
spark = (
    SparkSession.builder
    .appName("Postgres_Bronze_Facts")
    .master("local[*]")
    .config("spark.jars", postgres_jar)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def read_from_source(query: str, schema: StructType):
    return (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  query)
        .option("user",     DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .schema(schema)
        .load()
    )


def ingest_facts(target_date: str, is_incremental: bool = True):
    print(f"\nINGEST DATE: {target_date}")
    print(f"RUN MODE: {'INCREMENTAL LOAD' if is_incremental else 'FULL LOAD'}")

    if is_incremental:
        queries = {
            "orders": f"""(
                SELECT
                    order_id,
                    order_date,
                    branch_id,
                    sales_channel,
                    customer_segment
                FROM orders
                WHERE order_date LIKE '%{target_date}%'
            ) AS q_orders""",

            "order_details": f"""(
                SELECT
                    d.order_id,
                    d.product_id,
                    d.quantity::text   AS quantity,
                    d.unit_price::text AS unit_price,
                    d.unit_cost::text  AS unit_cost
                FROM order_details d
                JOIN orders o ON d.order_id = o.order_id
                WHERE o.order_date LIKE '%{target_date}%'
            ) AS q_details""",
        }
    else:
        queries = {
            "orders": """(
                SELECT
                    order_id,
                    order_date,
                    branch_id,
                    sales_channel,
                    customer_segment
                FROM orders
            ) AS q_orders_full""",

            "order_details": """(
                SELECT
                    order_id,
                    product_id,
                    quantity::text   AS quantity,
                    unit_price::text AS unit_price,
                    unit_cost::text  AS unit_cost
                FROM order_details
            ) AS q_details_full""",
        }


    for table, query in queries.items():
        try:
            print(f"[FACT] Reading {table.upper()}...")

            df_raw = read_from_source(query, schema=SCHEMAS[table])
            count = df_raw.count()

            if count == 0:
                print(f"⚠️  No data found for table {table}. Skipping write.")
                continue

            df_partitioned = df_raw.withColumn("ingest_date", lit(target_date))

            output_path = os.path.join(bronze_dir, table)

            (
                df_partitioned.write
                .mode("overwrite")
                .partitionBy("ingest_date")
                .parquet(output_path)
            )

            print(f"Saved Bronze partition to: {output_path}/ingest_date={target_date}")

        except Exception as e:
            print(f"Failed to process {table.upper()}: {e}")
            raise

if __name__ == "__main__":
    is_inc_str = os.getenv("IS_INCREMENTAL", "False")
    is_incremental = is_inc_str.lower() == "true"

    target_date = os.getenv("TARGET_DATE", None)
    if target_date and target_date.lower() == "none":
        target_date = None
    if target_date is None:
        target_date = datetime.now().strftime("%Y-%m-%d")

    ingest_facts(target_date=target_date, is_incremental=is_incremental)
    spark.stop()
    print("\nFinished Postgres Fact Ingestion!")
