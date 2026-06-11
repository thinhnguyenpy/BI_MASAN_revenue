import os
import re
import sys
import io

import pandas as pd
import psycopg2
import oracledb
from dotenv import load_dotenv

# Fix console encoding on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Load environment variables
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

# =========================
# CONFIGURATIONS
# =========================
STG_DB_CONFIG = {
    "user": os.getenv("STG_DB_USER"),
    "password": os.getenv("STG_DB_PASSWORD"),
    "host": os.getenv("STG_DB_HOST"),
    "port": os.getenv("STG_DB_PORT"),
    "dbname": os.getenv("STG_DB_NAME"),
}
STG_SCHEMA = os.getenv("STG_SCHEMA", "public")
RAW_TABLE = os.getenv("RAW_TABLE", "vinamilk_case_xlsx")

DW_DB_CONFIG = {
    "user": os.getenv("DW_DB_USER"),
    "password": os.getenv("DW_DB_PASSWORD"),
    "host": os.getenv("DW_DB_HOST"),
    "port": os.getenv("DW_DB_PORT"),
    "service_name": os.getenv("DW_DB_SERVICE"),
}

DIM_DATE_TABLE = os.getenv("DIM_DATE_TABLE")
DIM_REGION_TABLE = os.getenv("DIM_REGION_TABLE")
DIM_BRANCH_TABLE = os.getenv("DIM_BRANCH_TABLE")
DIM_PRODUCT_TABLE = os.getenv("DIM_PRODUCT_TABLE")
DIM_CHANNEL_TABLE = os.getenv("DIM_CHANNEL_TABLE")
DIM_CUSTOMER_SEGMENT_TABLE = os.getenv("DIM_CUSTOMER_SEGMENT_TABLE")
DIM_PROMOTION_TABLE = os.getenv("DIM_PROMOTION_TABLE")
FACT_SALES_TABLE = os.getenv("FACT_SALES_TABLE")

# =========================
# HELPER FUNCTIONS
# =========================
def normalize_col(name):
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())

def pick_column(df, candidates):
    normalized = {normalize_col(c): c for c in df.columns}
    for candidate in candidates:
        found = normalized.get(normalize_col(candidate))
        if found:
            return found
    return None

def standardize_columns(df):
    mapping = {
        "order_id": ["orderid", "order_id"],
        "date": ["date", "ngay"],
        "year": ["year", "nam"],
        "month": ["month", "thang"],
        "region": ["region", "vung"],
        "branch": ["branch", "chinhanh"],
        "product": ["product", "sanpham"],
        "category": ["category", "danhmuc"],
        "channel": ["channel", "kenh"],
        "quantity": ["quantity", "soluong"],
        "revenue": ["revenue", "doanhthu"],
        "cost": ["cost", "chiphi"],
        "profit": ["profit", "loinhuan"],
        "market_size": ["marketsize", "market_size", "quymothitruong"],
        "budget": ["budget", "ngansach"],
        "logistics_cost": ["logisticscost", "logistics_cost", "chiphilogistics"],
        "marketing_cost": ["marketingcost", "marketing_cost", "chiphimarketing"],
        "customer_segment": ["customersegment", "customer_segment", "phan khuc khach hang"],
        "promotion_campaign": ["promotioncampaign", "promotion_campaign", "chien dich"],
    }

    rename = {}
    for target, candidates in mapping.items():
        source = pick_column(df, candidates)
        if source:
            rename[source] = target

    df = df.rename(columns=rename)

    required = ["order_id", "date", "region", "revenue"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in raw table: {missing}")

    optional_cols = [
        "year", "month", "branch", "product", "category", "channel", "quantity",
        "cost", "profit", "market_size", "budget", "logistics_cost",
        "marketing_cost", "customer_segment", "promotion_campaign"
    ]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = None

    return df[[
        "order_id", "date", "year", "month", "region", "branch", "product",
        "category", "channel", "quantity", "revenue", "cost", "profit",
        "market_size", "budget", "logistics_cost", "marketing_cost",
        "customer_segment", "promotion_campaign"
    ]]

def parse_dates(series):
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed", dayfirst=True).dt.date
    except TypeError:
        return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date

def clean_value(value):
    if pd.isna(value):
        return None
    return value

def to_records(df):
    return [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]

def clean_data(df):
    df = standardize_columns(df).copy()
    df["date"] = parse_dates(df["date"])

    text_cols = [
        "region", "branch", "product", "category", "channel",
        "customer_segment", "promotion_campaign"
    ]
    for col in text_cols:
        df[col] = df[col].astype("string").str.strip().replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})

    num_cols = [
        "order_id", "year", "month", "quantity", "revenue", "cost", "profit",
        "market_size", "budget", "logistics_cost", "marketing_cost"
    ]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["order_id", "date", "region", "revenue"])
    df["order_id"] = df["order_id"].astype("int64")

    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month

    df = df.drop_duplicates(subset=["order_id"], keep="last")
    return df.astype(object).where(pd.notnull(df), None)

# =========================
# ORACLE ETL FUNCTIONS
# =========================
def load_dimensions(conn, df):
    # oracledb batch execution works faster if auto-commit is off during executemany
    cur = conn.cursor()
    
    # 1. dim_date
    dim_date = (
        df[["date", "year", "month"]]
        .drop_duplicates()
        .assign(
            month_name=lambda x: pd.to_datetime(x["date"]).dt.month_name(),
            quarter=lambda x: pd.to_datetime(x["date"]).dt.quarter,
            year_month=lambda x: pd.to_datetime(x["date"]).dt.strftime("%Y-%m"),
        )
    )
    cur.executemany(
        f"""
        MERGE INTO {DIM_DATE_TABLE} tgt
        USING (SELECT :1 AS full_date, :2 AS year, :3 AS month, :4 AS month_name, :5 AS quarter, :6 AS year_month FROM dual) src
        ON (tgt.full_date = src.full_date)
        WHEN MATCHED THEN UPDATE SET 
            tgt.year = src.year, tgt.month = src.month, tgt.month_name = src.month_name, 
            tgt.quarter = src.quarter, tgt.year_month = src.year_month
        WHEN NOT MATCHED THEN INSERT 
            (full_date, year, month, month_name, quarter, year_month) 
            VALUES (src.full_date, src.year, src.month, src.month_name, src.quarter, src.year_month)
        """,
        to_records(dim_date)
    )

    # 2. dim_region
    regions = [(clean_value(x),) for x in df["region"].dropna().drop_duplicates()]
    cur.executemany(
        f"""
        MERGE INTO {DIM_REGION_TABLE} tgt
        USING (SELECT :1 AS region_name FROM dual) src
        ON (tgt.region_name = src.region_name)
        WHEN NOT MATCHED THEN INSERT (region_name) VALUES (src.region_name)
        """,
        regions
    )

    # 3. dim_branch
    branches = to_records(df[["branch", "region"]].dropna(subset=["branch"]).drop_duplicates())
    cur.executemany(
        f"""
        MERGE INTO {DIM_BRANCH_TABLE} tgt
        USING (SELECT :1 AS branch_name, :2 AS region_name FROM dual) src
        ON (tgt.branch_name = src.branch_name)
        WHEN MATCHED THEN UPDATE SET tgt.region_name = src.region_name
        WHEN NOT MATCHED THEN INSERT (branch_name, region_name) VALUES (src.branch_name, src.region_name)
        """,
        branches
    )

    # 4. dim_product
    products = to_records(df[["product", "category"]].dropna(subset=["product"]).drop_duplicates(subset=["product"]))
    cur.executemany(
        f"""
        MERGE INTO {DIM_PRODUCT_TABLE} tgt
        USING (SELECT :1 AS product_name, :2 AS category FROM dual) src
        ON (tgt.product_name = src.product_name)
        WHEN MATCHED THEN UPDATE SET tgt.category = NVL(tgt.category, src.category)
        WHEN NOT MATCHED THEN INSERT (product_name, category) VALUES (src.product_name, src.category)
        """,
        products
    )

    # 5. dim_channel
    channels = [(clean_value(x),) for x in df["channel"].dropna().drop_duplicates()]
    cur.executemany(
        f"""
        MERGE INTO {DIM_CHANNEL_TABLE} tgt
        USING (SELECT :1 AS channel_name FROM dual) src
        ON (tgt.channel_name = src.channel_name)
        WHEN NOT MATCHED THEN INSERT (channel_name) VALUES (src.channel_name)
        """,
        channels
    )

    # 6. dim_customer_segment
    segments = [(clean_value(x),) for x in df["customer_segment"].dropna().drop_duplicates()]
    cur.executemany(
        f"""
        MERGE INTO {DIM_CUSTOMER_SEGMENT_TABLE} tgt
        USING (SELECT :1 AS segment_name FROM dual) src
        ON (tgt.segment_name = src.segment_name)
        WHEN NOT MATCHED THEN INSERT (segment_name) VALUES (src.segment_name)
        """,
        segments
    )

    # 7. dim_promotion
    promotions = [(clean_value(x),) for x in df["promotion_campaign"].dropna().drop_duplicates()]
    cur.executemany(
        f"""
        MERGE INTO {DIM_PROMOTION_TABLE} tgt
        USING (SELECT :1 AS promotion_campaign FROM dual) src
        ON (tgt.promotion_campaign = src.promotion_campaign)
        WHEN NOT MATCHED THEN INSERT (promotion_campaign) VALUES (src.promotion_campaign)
        """,
        promotions
    )

    conn.commit()
    cur.close()

def load_fact(conn, df):
    fact_cols = [
        "order_id", "date", "region", "branch", "product", "channel",
        "customer_segment", "promotion_campaign", "quantity", "revenue", "cost",
        "profit", "market_size", "budget", "logistics_cost", "marketing_cost"
    ]
    fact_df = df[fact_cols]
    rows = to_records(fact_df)

    cur = conn.cursor()
    cur.executemany(
        f"""
        MERGE INTO {FACT_SALES_TABLE} tgt
        USING (
            SELECT :1 as order_id, :2 as full_date, :3 as region_name, :4 as branch_name, 
                   :5 as product_name, :6 as channel_name, :7 as segment_name, 
                   :8 as promotion_campaign, :9 as quantity, :10 as revenue, 
                   :11 as cost, :12 as profit, :13 as market_size, :14 as budget, 
                   :15 as logistics_cost, :16 as marketing_cost FROM dual
        ) src
        ON (tgt.order_id = src.order_id)
        WHEN MATCHED THEN UPDATE SET
            tgt.full_date = src.full_date,
            tgt.region_name = src.region_name,
            tgt.branch_name = src.branch_name,
            tgt.product_name = src.product_name,
            tgt.channel_name = src.channel_name,
            tgt.segment_name = src.segment_name,
            tgt.promotion_campaign = src.promotion_campaign,
            tgt.quantity = src.quantity,
            tgt.revenue = src.revenue,
            tgt.cost = src.cost,
            tgt.profit = src.profit,
            tgt.market_size = src.market_size,
            tgt.budget = src.budget,
            tgt.logistics_cost = src.logistics_cost,
            tgt.marketing_cost = src.marketing_cost
        WHEN NOT MATCHED THEN INSERT (
            order_id, full_date, region_name, branch_name, product_name, channel_name, 
            segment_name, promotion_campaign, quantity, revenue, cost, profit, 
            market_size, budget, logistics_cost, marketing_cost
        ) VALUES (
            src.order_id, src.full_date, src.region_name, src.branch_name, src.product_name, 
            src.channel_name, src.segment_name, src.promotion_campaign, src.quantity, 
            src.revenue, src.cost, src.profit, src.market_size, src.budget, 
            src.logistics_cost, src.marketing_cost
        )
        """,
        rows
    )
    conn.commit()
    cur.close()

def main():
    # 1. Kết nối PostgreSQL (Staging) để đọc dữ liệu
    print("Connecting to PostgreSQL Staging...")
    with psycopg2.connect(**STG_DB_CONFIG) as pg_conn:
        raw_query = f'SELECT * FROM "{STG_SCHEMA}"."{RAW_TABLE}"'
        raw_df = pd.read_sql(raw_query, pg_conn)
        print(f"Extracted {len(raw_df)} rows from Staging.")

    # 2. Xử lý và làm sạch dữ liệu
    print("Cleaning and transforming data...")
    clean_df = clean_data(raw_df)
    print(f"Cleaned data: {len(clean_df)} valid rows ready for DW.")

    # 3. Kết nối Oracle (Data Warehouse) để ghi dữ liệu
    print("Connecting to Oracle Data Warehouse...")
    
    # Cấu hình chuỗi kết nối (DSN) cho Oracle
    dsn = oracledb.makedsn(DW_DB_CONFIG["host"], DW_DB_CONFIG["port"], service_name=DW_DB_CONFIG["service_name"])
    
    with oracledb.connect(user=DW_DB_CONFIG["user"], password=DW_DB_CONFIG["password"], dsn=dsn) as oracle_conn:
        print("Loading dimensions into Oracle...")
        load_dimensions(oracle_conn, clean_df)
        
        print("Loading facts into Oracle...")
        load_fact(oracle_conn, clean_df)
        
    print("ETL Pipeline finished successfully!")

if __name__ == "__main__":
    main()