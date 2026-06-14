# Spark Jobs Update Summary

## 1. Tong Quan

Truoc do project da co pipeline Spark cho PostgreSQL Sales:

- Bronze: doc du lieu tu PostgreSQL vao `datalake/bronze/sales_db`
- Silver: lam sach du lieu vao `datalake/silver/sales_db`
- Gold: nap du lieu vao PostgreSQL Data Warehouse schema `gold`

Trong phan vua lam, da bo sung them pipeline Spark cho:

- MySQL Finance/Marketing
- MongoDB Production/Logistics

Dong thoi tach Gold jobs thanh 3 file rieng theo tung domain:

- `3_silver_to_gold_sales.py`
- `3_silver_to_gold_mysql.py`
- `3_silver_to_gold_mongo.py`

## 2. Cac File Spark Jobs Moi

### MySQL Finance/Marketing

Da tao cac file:

```text
src/spark_jobs/bronze_jobs/1c_mysql_to_bronze_dim.py
src/spark_jobs/bronze_jobs/1d_mysql_to_bronze_fact.py
src/spark_jobs/silver_jobs/2c_bronze_to_silver_mysql_dim.py
src/spark_jobs/silver_jobs/2d_bronze_to_silver_mysql_fact.py
src/spark_jobs/gold_jobs/3_silver_to_gold_mysql.py
```

Luong du lieu:

```text
MySQL finance_db
        |
        v
datalake/bronze/finance_db
        |
        v
datalake/silver/finance_db
        |
        v
gold.dim_campaign
gold.fact_marketing_spend
gold.fact_monthly_budget
```

### MongoDB Production/Logistics

Da tao cac file:

```text
src/spark_jobs/bronze_jobs/1e_mongo_to_bronze_dim.py
src/spark_jobs/bronze_jobs/1f_mongo_to_bronze_fact.py
src/spark_jobs/silver_jobs/2e_bronze_to_silver_mongo_dim.py
src/spark_jobs/silver_jobs/2f_bronze_to_silver_mongo_fact.py
src/spark_jobs/gold_jobs/3_silver_to_gold_mongo.py
```

Luong du lieu:

```text
MongoDB production_db
        |
        v
datalake/bronze/production_db
        |
        v
datalake/silver/production_db
        |
        v
gold.dim_department
gold.fact_production_logs
gold.fact_logistics_costs
```

## 3. Cac Bang Gold Duoc Nap

### PostgreSQL Sales

File:

```text
src/spark_jobs/gold_jobs/3_silver_to_gold_sales.py
```

Nap cac bang:

```text
gold.dim_date
gold.dim_branch
gold.dim_product
gold.fact_sales
```

### MySQL Finance/Marketing

File:

```text
src/spark_jobs/gold_jobs/3_silver_to_gold_mysql.py
```

Nap cac bang:

```text
gold.dim_campaign
gold.fact_marketing_spend
gold.fact_monthly_budget
```

### MongoDB Production/Logistics

File:

```text
src/spark_jobs/gold_jobs/3_silver_to_gold_mongo.py
```

Nap cac bang:

```text
gold.dim_department
gold.fact_production_logs
gold.fact_logistics_costs
```

Luu y: cac bang MongoDB tren da duoc tao san trong `src/init/init_dw.sql`, nen viec thay ten bang trong DBeaver khong co nghia la bang da co du lieu. Can dung `COUNT(*)` de kiem tra.

## 4. Thu Tu Chay Spark Jobs

Chay tu thu muc goc project:

```text
D:\BI_MASAN\BI_MASAN_FINAL\BI_MASAN_revenue
```

### Bronze

```bat
python src/spark_jobs/bronze_jobs/1a_postgres_to_bronze_dim.py
python src/spark_jobs/bronze_jobs/1b_postgres_to_bronze_fact.py

python src/spark_jobs/bronze_jobs/1c_mysql_to_bronze_dim.py
python src/spark_jobs/bronze_jobs/1d_mysql_to_bronze_fact.py

python src/spark_jobs/bronze_jobs/1e_mongo_to_bronze_dim.py
python src/spark_jobs/bronze_jobs/1f_mongo_to_bronze_fact.py
```

### Silver

```bat
python src/spark_jobs/silver_jobs/2a_bronze_to_silver_dim.py
python src/spark_jobs/silver_jobs/2b_bronze_to_silver_fact.py

python src/spark_jobs/silver_jobs/2c_bronze_to_silver_mysql_dim.py
python src/spark_jobs/silver_jobs/2d_bronze_to_silver_mysql_fact.py

python src/spark_jobs/silver_jobs/2e_bronze_to_silver_mongo_dim.py
python src/spark_jobs/silver_jobs/2f_bronze_to_silver_mongo_fact.py
```

### Gold

```bat
python src/spark_jobs/gold_jobs/3_silver_to_gold_sales.py
python src/spark_jobs/gold_jobs/3_silver_to_gold_mysql.py
python src/spark_jobs/gold_jobs/3_silver_to_gold_mongo.py
```

Nen chay `3_silver_to_gold_sales.py` truoc `3_silver_to_gold_mongo.py`, vi Mongo Gold can lookup `gold.dim_product` va `gold.dim_branch`.

## 5. Ket Qua Da Kiem Tra

### MongoDB Source

Da kiem tra MongoDB source co du lieu:

```text
Departments    = 6
ProductionLogs = 2954
LogisticsCosts = 2964
```

### MongoDB Pipeline

Da chay thanh cong cac job MongoDB:

```text
Bronze departments     = 6 rows
Bronze production_logs = 2954 rows
Bronze logistics_costs = 2964 rows

Silver departments     = 6 rows
Silver production_logs = 2946 rows
Silver logistics_costs = 2955 rows

Gold dim_department       = 6 rows
Gold fact_production_logs = 2911 rows
Gold fact_logistics_costs = 2655 rows
```

So dong Gold nho hon Silver vi Gold co buoc:

- lookup surrogate key tu dimension table
- bo cac dong khong lookup duoc key
- group theo grain cua fact table

### Gold Data Warehouse Hien Tai

Ket qua kiem tra bang `COUNT(*)`:

```text
gold.dim_branch           = 9
gold.dim_campaign         = 6
gold.dim_date             = 4018
gold.dim_department       = 6
gold.dim_product          = 8
gold.fact_logistics_costs = 2655
gold.fact_marketing_spend = 2164
gold.fact_monthly_budget  = 144
gold.fact_production_logs = 2911
gold.fact_sales           = 2941
```

## 6. Cau Lenh Kiem Tra Trong DBeaver

Dung SQL nay de kiem tra cac bang Gold chinh:

```sql
SELECT 'dim_branch' AS table_name, COUNT(*) AS row_count FROM gold.dim_branch
UNION ALL
SELECT 'dim_campaign', COUNT(*) FROM gold.dim_campaign
UNION ALL
SELECT 'dim_date', COUNT(*) FROM gold.dim_date
UNION ALL
SELECT 'dim_department', COUNT(*) FROM gold.dim_department
UNION ALL
SELECT 'dim_product', COUNT(*) FROM gold.dim_product
UNION ALL
SELECT 'fact_logistics_costs', COUNT(*) FROM gold.fact_logistics_costs
UNION ALL
SELECT 'fact_marketing_spend', COUNT(*) FROM gold.fact_marketing_spend
UNION ALL
SELECT 'fact_monthly_budget', COUNT(*) FROM gold.fact_monthly_budget
UNION ALL
SELECT 'fact_production_logs', COUNT(*) FROM gold.fact_production_logs
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM gold.fact_sales
ORDER BY table_name;
```

Dung SQL nay de kiem tra nhanh ca staging tables:

```sql
SELECT schemaname, relname AS table_name, n_live_tup AS estimated_rows
FROM pg_stat_user_tables
WHERE schemaname = 'gold'
ORDER BY relname;
```

Luu y: cot size trong DBeaver nhu `40K`, `632K`, `1.3M` la dung luong luu tru, khong phai so dong.

## 7. Luu Y Khi Chay Tren Windows

Can co Java:

```bat
java -version
```

Neu chua co Java:

```bat
winget install EclipseAdoptium.Temurin.17.JDK
```

Can co `winutils.exe` tai:

```text
C:\hadoop\bin\winutils.exe
```

Moi terminal moi nen set:

```bat
set HADOOP_HOME=C:\hadoop
set hadoop.home.dir=C:\hadoop
set PATH=%HADOOP_HOME%\bin;%PATH%
```

Neu dung PowerShell:

```powershell
$env:HADOOP_HOME="C:\hadoop"
[System.Environment]::SetEnvironmentVariable("hadoop.home.dir", "C:\hadoop", "Process")
$env:PATH="C:\hadoop\bin;$env:PATH"
```

Canh bao `Exception while deleting Spark temp dir` sau khi job ket thuc thuong chi la loi don file tam cua Spark tren Windows. Neu log da co `Finished ...` va bang Gold co du lieu thi co the bo qua.

## 8. Trang Thai Hien Tai

- `README.md` da duoc khoi phuc ve ban ban dau.
- File nay dung de ghi chu rieng cac phan vua bo sung.
- MySQL Spark jobs da duoc tao.
- MongoDB Spark jobs da duoc tao va chay thu thanh cong.
- Gold jobs da duoc tach theo domain: Sales, MySQL, MongoDB.
- Data Warehouse PostgreSQL schema `gold` da co du lieu cho ca 3 domain.
