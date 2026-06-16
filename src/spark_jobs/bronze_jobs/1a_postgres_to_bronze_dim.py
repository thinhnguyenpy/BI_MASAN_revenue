import os
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit




load_dotenv()
DB_HOST     = os.getenv("POSTGRES_HOST", "stg_postgres_sales")
DB_PORT     = os.getenv("POSTGRES_PORT", "5432")
DB_NAME     = os.getenv("POSTGRES_DB",   "sales_db")
DB_USER     = os.getenv("POSTGRES_USER", "admin")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD", "admin_password")
JDBC_URL    = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"



current_dir  = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_dir)))
bronze_dir   = os.path.join(project_root, "datalake", "bronze", "sales_db")
postgres_jar = os.path.join(project_root, "jars", "postgresql-42.7.3.jar")


print(f"📁 project_root : {project_root}")
print(f"📁 bronze_dir   : {bronze_dir}")
print(f"📁 postgres_jar : {postgres_jar}")
print(f"✅ Jar exists   : {os.path.exists(postgres_jar)}")




print("\nStarting Spark [BRONZE - DIMENSIONS]...")
spark = (
    SparkSession.builder
    .appName("Postgres_Bronze_Dimensions")
    .master("local[*]")
    .config("spark.jars", postgres_jar)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")




def read_from_source(table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url",      JDBC_URL)
        .option("dbtable",  table_name)
        .option("user",     DB_USER)
        .option("password", DB_PASSWORD)
        .option("driver",   "org.postgresql.Driver")
        .load()
    )




def ingest_dimensions(ingest_date: str):
    dim_tables = ["categories", "products", "branches"]

    print(f"\nINGEST DATE: {ingest_date}")
    print(f"Tables: {dim_tables}")

    success_tables = []
    failed_tables  = []

    for table in dim_tables:
        try:
            print(f"\n[DIM] Reading {table.upper()}...")

            df_raw = read_from_source(table)
            count  = df_raw.count()


            df_partitioned = df_raw.withColumn("ingest_date", lit(ingest_date))

            output_path = os.path.join(bronze_dir, table)

            df_partitioned.write\
                .mode("overwrite")\
                .partitionBy("ingest_date")\
                .parquet(output_path)

            print(f"Saved {count} rows to: {output_path}/ingest_date={ingest_date}")
            success_tables.append(table)

        except Exception as e:
            print(f"Failed to process {table.upper()}: {e}")
            failed_tables.append(table)


    print(f"\n{'='*50}")
    print(f"Success: {success_tables}")
    if failed_tables:
        print(f"Failed : {failed_tables}")
        raise RuntimeError(f"Some tables failed: {failed_tables}")




if __name__ == "__main__":
    run_date = datetime.now().strftime("%Y-%m-%d")
    ingest_dimensions(ingest_date=run_date)
    spark.stop()
    print("\nFinished Postgres Dimension Ingestion!")
