import os
import re
import sys
import io

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv


# Fix console encoding on Windows.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Load database connection settings and table names from .env.
dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path)

DB_CONFIG = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "dbname": os.getenv("DB_NAME"),
}

STG_SCHEMA = os.getenv("DB_SCHEMA_STAGING", "staging")
DW_SCHEMA = os.getenv("DB_SCHEMA_DWH", "dw")
RAW_TABLE = os.getenv("RAW_TABLE", "vinamilk_case_xlsx")

DIM_DATE_TABLE = os.getenv("DIM_DATE_TABLE", "dim_date")
DIM_REGION_TABLE = os.getenv("DIM_REGION_TABLE", "dim_region")
DIM_BRANCH_TABLE = os.getenv("DIM_BRANCH_TABLE", "dim_branch")
DIM_PRODUCT_TABLE = os.getenv("DIM_PRODUCT_TABLE", "dim_product")
DIM_CHANNEL_TABLE = os.getenv("DIM_CHANNEL_TABLE", "dim_channel")
DIM_CUSTOMER_SEGMENT_TABLE = os.getenv("DIM_CUSTOMER_SEGMENT_TABLE", "dim_customer_segment")
DIM_PROMOTION_TABLE = os.getenv("DIM_PROMOTION_TABLE", "dim_promotion")
FACT_SALES_TABLE = os.getenv("FACT_SALES_TABLE", "fact_sales")


def q(name):
    """Quote schema, table, and column names safely."""
    return '"' + name.replace('"', '""') + '"'


def table_name(schema, table):
    return f"{q(schema)}.{q(table)}"


def normalize_col(name):
    # Normalize column names so OrderID, order_id, and order id can match.
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def pick_column(df, candidates):
    # Find a raw column by checking a list of possible source names.
    normalized = {normalize_col(c): c for c in df.columns}
    for candidate in candidates:
        found = normalized.get(normalize_col(candidate))
        if found:
            return found
    return None


def standardize_columns(df):
    # Map raw column names to the standard column set used by this ETL.
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

    # These columns are required to build the revenue fact table.
    required = ["order_id", "date", "region", "revenue"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in raw table: {missing}")

    # Create missing optional columns as empty columns so the load step is stable.
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
    # Support common date formats such as yyyy-mm-dd and dd/mm/yyyy.
    try:
        return pd.to_datetime(series, errors="coerce", format="mixed", dayfirst=True).dt.date
    except TypeError:
        return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date


def clean_value(value):
    # Convert pandas missing values to Python None for psycopg2.
    if pd.isna(value):
        return None
    return value


def to_records(df):
    # Convert a dataframe into tuples that psycopg2 can insert safely.
    return [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]


def clean_data(df):
    # 1) Standardize source column names.
    df = standardize_columns(df).copy()

    # 2) Parse dates so dim_date and monthly trends are correct.
    df["date"] = parse_dates(df["date"])

    # 3) Clean text columns: trim spaces and convert empty strings to NULL.
    text_cols = [
        "region", "branch", "product", "category", "channel",
        "customer_segment", "promotion_campaign"
    ]
    for col in text_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )

    # 4) Convert revenue and cost metrics to numeric values.
    num_cols = [
        "order_id", "year", "month", "quantity", "revenue", "cost", "profit",
        "market_size", "budget", "logistics_cost", "marketing_cost"
    ]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) Drop rows missing required keys or core metrics.
    df = df.dropna(subset=["order_id", "date", "region", "revenue"])
    df["order_id"] = df["order_id"].astype("int64")

    # Derive year and month from the cleaned date to avoid raw input mismatch.
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month

    # Remove duplicate orders and keep the latest cleaned row.
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    # PostgreSQL/psycopg2 expects Python None instead of pandas NA/NaN.
    return df.astype(object).where(pd.notnull(df), None)


def load_dimensions(conn, df):
    with conn.cursor() as cur:
        # dim_date supports date, month, quarter, and year slicing.
        dim_date = (
            df[["date", "year", "month"]]
            .drop_duplicates()
            .assign(
                month_name=lambda x: pd.to_datetime(x["date"]).dt.month_name(),
                quarter=lambda x: pd.to_datetime(x["date"]).dt.quarter,
                year_month=lambda x: pd.to_datetime(x["date"]).dt.strftime("%Y-%m"),
            )
        )
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_DATE_TABLE)}
                (full_date, year, month, month_name, quarter, year_month)
            VALUES %s
            ON CONFLICT (full_date) DO UPDATE SET
                year = EXCLUDED.year,
                month = EXCLUDED.month,
                month_name = EXCLUDED.month_name,
                quarter = EXCLUDED.quarter,
                year_month = EXCLUDED.year_month
            """,
            to_records(dim_date),
        )

        # dim_region tracks revenue and profit by region.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_REGION_TABLE)} (region_name)
            VALUES %s
            ON CONFLICT (region_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["region"].dropna().drop_duplicates()],
        )

        # dim_branch supports revenue drill-down from region to branch.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_BRANCH_TABLE)} (branch_name, region_name)
            VALUES %s
            ON CONFLICT (branch_name) DO UPDATE SET
                region_name = EXCLUDED.region_name
            """,
            to_records(df[["branch", "region"]].dropna(subset=["branch"]).drop_duplicates()),
        )

        # dim_product tracks revenue by product and category.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_PRODUCT_TABLE)} (product_name, category)
            VALUES %s
            ON CONFLICT (product_name) DO UPDATE SET
                category = COALESCE({q(DIM_PRODUCT_TABLE)}.category, EXCLUDED.category)
            """,
            to_records(df[["product", "category"]].dropna(subset=["product"]).drop_duplicates(subset=["product"])),
        )

        # dim_channel tracks revenue by sales or marketing channel.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_CHANNEL_TABLE)} (channel_name)
            VALUES %s
            ON CONFLICT (channel_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["channel"].dropna().drop_duplicates()],
        )

        # dim_customer_segment tracks revenue by customer segment.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_CUSTOMER_SEGMENT_TABLE)} (segment_name)
            VALUES %s
            ON CONFLICT (segment_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["customer_segment"].dropna().drop_duplicates()],
        )

        # dim_promotion tracks revenue by promotion campaign.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_PROMOTION_TABLE)} (promotion_campaign)
            VALUES %s
            ON CONFLICT (promotion_campaign) DO NOTHING
            """,
            [(clean_value(x),) for x in df["promotion_campaign"].dropna().drop_duplicates()],
        )

    conn.commit()


def load_fact(conn, df):
    # The fact table stores the main dashboard measures: revenue, profit, cost,
    # market_size, budget, logistics_cost, marketing_cost.
    fact_cols = [
        "order_id", "date", "region", "branch", "product", "channel",
        "customer_segment", "promotion_campaign", "quantity", "revenue", "cost",
        "profit", "market_size", "budget", "logistics_cost", "marketing_cost"
    ]
    fact_df = df[fact_cols]

    rows = to_records(fact_df)
    with conn.cursor() as cur:
        # Upsert by order_id so rerunning the ETL does not create duplicates.
        execute_values(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, FACT_SALES_TABLE)} (
                order_id, full_date, region_name, branch_name, product_name,
                channel_name, segment_name, promotion_campaign, quantity,
                revenue, cost, profit, market_size, budget, logistics_cost,
                marketing_cost
            )
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                full_date = EXCLUDED.full_date,
                region_name = EXCLUDED.region_name,
                branch_name = EXCLUDED.branch_name,
                product_name = EXCLUDED.product_name,
                channel_name = EXCLUDED.channel_name,
                segment_name = EXCLUDED.segment_name,
                promotion_campaign = EXCLUDED.promotion_campaign,
                quantity = EXCLUDED.quantity,
                revenue = EXCLUDED.revenue,
                cost = EXCLUDED.cost,
                profit = EXCLUDED.profit,
                market_size = EXCLUDED.market_size,
                budget = EXCLUDED.budget,
                logistics_cost = EXCLUDED.logistics_cost,
                marketing_cost = EXCLUDED.marketing_cost
            """,
            rows,
        )
    conn.commit()


def main():
    # Read raw data that has already been loaded from Excel into staging.
    raw_query = f"SELECT * FROM {table_name(STG_SCHEMA, RAW_TABLE)}"

    with psycopg2.connect(**DB_CONFIG) as conn:
        print("Connected to PostgreSQL")

        raw_df = pd.read_sql(raw_query, conn)
        print(f"Raw rows: {len(raw_df)}")

        # Transform and clean before loading into the star schema.
        clean_df = clean_data(raw_df)
        print(f"Clean rows: {len(clean_df)}")

        # Load dimensions first, then load the fact table.
        load_dimensions(conn, clean_df)
        load_fact(conn, clean_df)

        print("ETL finished")
        print(f"Loaded dimensions and fact into schema: {DW_SCHEMA}")
        print(f"Fact table: {FACT_SALES_TABLE}")


if __name__ == "__main__":
    main()
