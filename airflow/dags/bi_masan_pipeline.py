from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

PROJECT_ROOT = "/opt/airflow/project"
PYTHON = "python"


def run_script(script_path: str) -> str:
    return f"cd {PROJECT_ROOT} && {PYTHON} {script_path}"


def make_task(task_id: str, script_path: str, dag: DAG) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=run_script(script_path),
        dag=dag,
    )


def make_fact_task(task_id: str, script_path: str, dag: DAG) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"""
            cd {PROJECT_ROOT} && \
            export IS_INCREMENTAL="{{{{ params.is_incremental }}}}" && \
            export TARGET_DATE="{{{{ params.target_date }}}}" && \
            {PYTHON} {script_path}
        """,
        dag=dag,
    )

def make_silver_sales_fact_task(dag: DAG) -> BashOperator:
    return BashOperator(
        task_id="silver_sales_fact",
        bash_command=f"""
            cd {PROJECT_ROOT} && \
            export IS_INCREMENTAL="{{{{ params.is_incremental }}}}" && \
            export TARGET_DATE="{{{{ params.target_date }}}}" && \
            {PYTHON} src/spark_jobs/silver_jobs/2b_bronze_to_silver_fact.py
        """,
        dag=dag,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bi_masan_revenue_pipeline",
    default_args=default_args,
    params={
        "is_incremental": "False",
        "target_date": ""
    },
    description="Dev DAG for BI MASAN revenue pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bi-masan", "spark", "etl"],
) as dag:
    start = EmptyOperator(task_id="start")
    finish = EmptyOperator(task_id="finish")


    bronze_sales_dim = make_task(
        "bronze_sales_dim",
        "src/spark_jobs/bronze_jobs/1a_postgres_to_bronze_dim.py",
        dag,
    )
    bronze_sales_fact = make_fact_task(
        "bronze_sales_fact",
        "src/spark_jobs/bronze_jobs/1b_postgres_to_bronze_fact.py",
        dag,
    )
    silver_sales_dim = make_task(
        "silver_sales_dim",
        "src/spark_jobs/silver_jobs/2a_bronze_to_silver_dim.py",
        dag,
    )
    silver_sales_fact = make_silver_sales_fact_task(dag)
    gold_sales = make_fact_task(
        "gold_sales",
        "src/spark_jobs/gold_jobs/3_silver_to_gold_sales.py",
        dag,
    )

    bronze_mysql_dim = make_task(
        "bronze_mysql_dim",
        "src/spark_jobs/bronze_jobs/1c_mysql_to_bronze_dim.py",
        dag,
    )
    bronze_mysql_fact = make_fact_task(
        "bronze_mysql_fact",
        "src/spark_jobs/bronze_jobs/1d_mysql_to_bronze_fact.py",
        dag,
    )
    silver_mysql_dim = make_task(
        "silver_mysql_dim",
        "src/spark_jobs/silver_jobs/2c_bronze_to_silver_mysql_dim.py",
        dag,
    )
    silver_mysql_fact = make_fact_task(
        "silver_mysql_fact",
        "src/spark_jobs/silver_jobs/2d_bronze_to_silver_mysql_fact.py",
        dag,
    )
    gold_mysql = make_fact_task(
        "gold_mysql",
        "src/spark_jobs/gold_jobs/3_silver_to_gold_mysql.py",
        dag,
    )

    bronze_mongo_dim = make_task(
        "bronze_mongo_dim",
        "src/spark_jobs/bronze_jobs/1e_mongo_to_bronze_dim.py",
        dag,
    )
    bronze_mongo_fact = make_fact_task(
        "bronze_mongo_fact",
        "src/spark_jobs/bronze_jobs/1f_mongo_to_bronze_fact.py",
        dag,
    )
    silver_mongo_dim = make_task(
        "silver_mongo_dim",
        "src/spark_jobs/silver_jobs/2e_bronze_to_silver_mongo_dim.py",
        dag,
    )
    silver_mongo_fact = make_fact_task(
        "silver_mongo_fact",
        "src/spark_jobs/silver_jobs/2f_bronze_to_silver_mongo_fact.py",
        dag,
    )
    gold_mongo = make_fact_task(
        "gold_mongo",
        "src/spark_jobs/gold_jobs/3_silver_to_gold_mongo.py",
        dag,
    )

    start >> bronze_sales_dim >> bronze_sales_fact >> silver_sales_dim >> silver_sales_fact >> gold_sales
    
    gold_sales >> bronze_mysql_dim >> bronze_mysql_fact >> silver_mysql_dim >> silver_mysql_fact >> gold_mysql
    
    gold_mysql >> bronze_mongo_dim >> bronze_mongo_fact >> silver_mongo_dim >> silver_mongo_fact >> gold_mongo
    
    gold_mongo >> finish
