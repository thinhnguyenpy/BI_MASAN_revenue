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

DIM_DATE_TABLE = os.getenv("MARKETING_DIM_DATE_TABLE", "dim_date")
DIM_STAGE_TABLE = os.getenv("MARKETING_DIM_STAGE_TABLE", "dim_stage")
DIM_CHANNEL_TABLE = os.getenv("MARKETING_DIM_CHANNEL_TABLE", "dim_channel")
DIM_CUSTOMER_SEGMENT_TABLE = os.getenv("MARKETING_DIM_CUSTOMER_SEGMENT_TABLE", "dim_customer_segment")
DIM_CAMPAIGN_TABLE = os.getenv("MARKETING_DIM_CAMPAIGN_TABLE", "dim_campaign")
DIM_DEPARTMENT_TABLE = os.getenv("MARKETING_DIM_DEPARTMENT_TABLE", "dim_department")
DIM_MACHINE_TABLE = os.getenv("MARKETING_DIM_MACHINE_TABLE", "dim_machine")
DIM_INFORMATION_TABLE = os.getenv("MARKETING_DIM_INFORMATION_TABLE", "dim_information")
FACT_MARKETING_TABLE = os.getenv("FACT_MARKETING_TABLE", "fact_marketing_sales")

STAGE_ORDER = {
    "Impression": (1, "Awareness"),
    "Visit website": (2, "Consideration"),
    "Click": (3, "Consideration"),
    "Add to cart": (4, "Intent"),
    "Purchase": (5, "Conversion"),
}

CHANNEL_TYPE = {
    "Facebook": "Social",
    "Ecommerce": "Digital commerce",
    "TV": "Broadcast",
    "Retail promotion": "Trade promotion",
    "Traditional trade": "Traditional trade",
    "Other": "Other",
}


def q(name):
    """Quote schema, table, and column names safely."""
    return '"' + name.replace('"', '""') + '"'


def table_name(schema, table):
    return f"{q(schema)}.{q(table)}"


def normalize_col(name):
    # Normalize column names so ROI_Marketing, roi marketing, and roimarketing match.
    return re.sub(r"[^a-z0-9]", "", str(name).strip().lower())


def pick_column(df, candidates):
    normalized = {normalize_col(c): c for c in df.columns}
    for candidate in candidates:
        found = normalized.get(normalize_col(candidate))
        if found:
            return found
    return None


def standardize_columns(df):
    # Map raw source columns to the standard column set used by this marketing ETL.
    mapping = {
        "order_id": ["orderid", "order_id"],
        "date": ["date", "ngay"],
        "year": ["year", "nam"],
        "month": ["month", "thang"],
        "region": ["region", "vung"],
        "branch": ["branch", "chinhanh"],
        "department": ["department", "phongban", "bo phan"],
        "product": ["product", "sanpham"],
        "category": ["category", "danhmuc"],
        "stage": ["stage", "funnelstage", "giai doan"],
        "channel": ["channel", "kenh"],
        "information": ["information", "thongtin"],
        "machine": ["machine", "may"],
        "quantity": ["quantity", "soluong"],
        "revenue": ["revenue", "doanhthu"],
        "cost": ["cost", "chiphi"],
        "profit": ["profit", "loinhuan"],
        "profit_margin": ["profitmargin", "profit_margin", "bienloinhuan"],
        "inventory_level": ["inventorylevel", "inventory_level", "tonkho"],
        "calls": ["calls", "cuocgoi"],
        "waiting_time": ["waitingtime", "waiting_time", "thoigiancho"],
        "budget": ["budget", "ngansach"],
        "target": ["target", "muctieu"],
        "raw_material_cost": ["rawmaterialcost", "raw_material_cost"],
        "labor_cost": ["laborcost", "labor_cost"],
        "logistics_cost": ["logisticscost", "logistics_cost", "chiphilogistics"],
        "marketing_cost": ["marketingcost", "marketing_cost", "chiphimarketing"],
        "unit_cost": ["unitcost", "unit_cost"],
        "revenue_per_unit": ["revenueperunit", "revenue_per_unit"],
        "roi_marketing": ["roimarketing", "roi_marketing", "roi marketing"],
        "conversion_proxy": ["conversionproxy", "conversion_proxy"],
        "cost_per_machine_unit": ["costpermachineunit", "cost_per_machine_unit"],
        "region_profit_share": ["regionprofitshare", "region_profit_share"],
        "market_size": ["marketsize", "market_size", "quymothitruong"],
        "market_share": ["marketshare", "market_share"],
        "customer_segment": ["customersegment", "customer_segment", "phan khuc khach hang"],
        "campaign": ["promotioncampaign", "promotion_campaign", "campaignname", "campaign_name", "chien dich"],
    }

    rename = {}
    for target, candidates in mapping.items():
        source = pick_column(df, candidates)
        if source:
            rename[source] = target

    df = df.rename(columns=rename)

    required = ["order_id", "date", "channel", "stage", "revenue"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in raw table: {missing}")

    optional_cols = [
        "year", "month", "region", "branch", "department", "product", "category",
        "information", "machine", "quantity", "cost", "profit", "profit_margin",
        "inventory_level", "calls", "waiting_time", "budget", "target",
        "raw_material_cost", "labor_cost", "logistics_cost", "marketing_cost",
        "unit_cost", "revenue_per_unit", "roi_marketing", "conversion_proxy",
        "cost_per_machine_unit", "region_profit_share", "market_size",
        "market_share", "customer_segment", "campaign"
    ]
    for col in optional_cols:
        if col not in df.columns:
            df[col] = None

    return df[[
        "order_id", "date", "year", "month", "region", "branch",
        "department", "product", "category", "stage", "channel",
        "information", "machine", "quantity", "revenue", "cost", "profit",
        "profit_margin", "inventory_level", "calls", "waiting_time",
        "budget", "target", "raw_material_cost", "labor_cost",
        "logistics_cost", "marketing_cost", "unit_cost", "revenue_per_unit",
        "roi_marketing", "conversion_proxy", "cost_per_machine_unit",
        "region_profit_share", "market_size", "market_share",
        "customer_segment", "campaign"
    ]]


def parse_dates(series):
    # Keep ISO yyyy-mm-dd dates unambiguous, while still supporting dd/mm/yyyy.
    text = series.astype("string").str.strip()
    result = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns]")

    iso_mask = text.str.match(r"^\d{4}-\d{1,2}-\d{1,2}$", na=False)
    result.loc[iso_mask] = pd.to_datetime(text.loc[iso_mask], errors="coerce", format="%Y-%m-%d")
    non_iso = ~iso_mask
    result.loc[non_iso] = pd.to_datetime(text.loc[non_iso], errors="coerce", dayfirst=True)

    # Some rows use mm/dd/yyyy. Parse those only after day-first fails.
    fallback_mask = non_iso & result.isna()
    result.loc[fallback_mask] = pd.to_datetime(text.loc[fallback_mask], errors="coerce", dayfirst=False)

    return result.dt.date


def clean_value(value):
    # Convert pandas missing values to Python None for psycopg2.
    if pd.isna(value):
        return None
    return value


def to_records(df):
    return [
        tuple(clean_value(value) for value in row)
        for row in df.itertuples(index=False, name=None)
    ]


def execute_values_if_any(cur, sql, rows):
    if rows:
        execute_values(cur, sql, rows)


def classify_campaign(campaign):
    if campaign is None or pd.isna(campaign):
        return None
    campaign = str(campaign)
    if "Tet" in campaign or "Spring" in campaign:
        return "Seasonal"
    if "Summer" in campaign or "Mid-Autumn" in campaign:
        return "Seasonal"
    if "Year-End" in campaign:
        return "Year end"
    if "Back-to-School" in campaign:
        return "Back to school"
    return "Other"


def clean_data(df):
    # 1) Standardize source column names.
    df = standardize_columns(df).copy()

    # 2) Parse dates so dim_date and time slicers are reliable.
    df["date"] = parse_dates(df["date"])

    # 3) Clean text dimensions.
    text_cols = [
        "region", "branch", "department", "product", "category", "stage",
        "channel", "information", "machine", "customer_segment", "campaign"
    ]
    for col in text_cols:
        df[col] = (
            df[col]
            .astype("string")
            .str.strip()
            .replace({"": pd.NA, "nan": pd.NA, "None": pd.NA})
        )

    # 4) Convert all numeric dashboard metrics.
    num_cols = [
        "order_id", "year", "month", "quantity", "revenue", "cost", "profit",
        "profit_margin", "inventory_level", "calls", "waiting_time", "budget",
        "target", "raw_material_cost", "labor_cost", "logistics_cost",
        "marketing_cost", "unit_cost", "revenue_per_unit", "roi_marketing",
        "conversion_proxy", "cost_per_machine_unit", "region_profit_share",
        "market_size", "market_share"
    ]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 5) Drop rows that cannot participate in channel marketing analysis.
    df = df.dropna(subset=["order_id", "date", "channel", "stage", "revenue"])
    df["order_id"] = df["order_id"].astype("int64")

    # Derive year/month from cleaned date to avoid source mismatch.
    df["year"] = pd.to_datetime(df["date"]).dt.year
    df["month"] = pd.to_datetime(df["date"]).dt.month

    # Keep one fact row per source OrderID.
    df = df.drop_duplicates(subset=["order_id"], keep="last")

    return df.astype(object).where(pd.notnull(df), None)


def load_dimensions(conn, df):
    with conn.cursor() as cur:
        dim_date = (
            df[["date", "year", "month"]]
            .drop_duplicates()
            .assign(
                month_name=lambda x: pd.to_datetime(x["date"]).dt.month_name(),
                quarter=lambda x: pd.to_datetime(x["date"]).dt.quarter,
                year_month=lambda x: pd.to_datetime(x["date"]).dt.strftime("%Y-%m"),
            )
        )
        execute_values_if_any(
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

        stage_rows = [
            (stage_name, stage_order, stage_group)
            for stage_name, (stage_order, stage_group) in STAGE_ORDER.items()
        ]
        source_stage_rows = []
        known_stages = set(STAGE_ORDER)
        for stage in df["stage"].dropna().drop_duplicates():
            if stage not in known_stages:
                source_stage_rows.append((stage, 99, "Other"))

        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_STAGE_TABLE)}
                (stage_name, stage_order, stage_group)
            VALUES %s
            ON CONFLICT (stage_name) DO UPDATE SET
                stage_order = EXCLUDED.stage_order,
                stage_group = EXCLUDED.stage_group
            """,
            stage_rows + source_stage_rows,
        )

        channel_rows = [
            (channel, CHANNEL_TYPE.get(channel, "Other"))
            for channel in df["channel"].dropna().drop_duplicates()
        ]
        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_CHANNEL_TABLE)}
                (channel_name, channel_type)
            VALUES %s
            ON CONFLICT (channel_name) DO UPDATE SET
                channel_type = EXCLUDED.channel_type
            """,
            channel_rows,
        )

        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_CUSTOMER_SEGMENT_TABLE)} (segment_name)
            VALUES %s
            ON CONFLICT (segment_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["customer_segment"].dropna().drop_duplicates()],
        )

        campaign_rows = [
            (campaign, classify_campaign(campaign))
            for campaign in df["campaign"].dropna().drop_duplicates()
        ]
        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_CAMPAIGN_TABLE)}
                (campaign_name, campaign_type)
            VALUES %s
            ON CONFLICT (campaign_name) DO UPDATE SET
                campaign_type = EXCLUDED.campaign_type
            """,
            campaign_rows,
        )

        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_DEPARTMENT_TABLE)} (department_name)
            VALUES %s
            ON CONFLICT (department_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["department"].dropna().drop_duplicates()],
        )

        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_MACHINE_TABLE)} (machine_name)
            VALUES %s
            ON CONFLICT (machine_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["machine"].dropna().drop_duplicates()],
        )

        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, DIM_INFORMATION_TABLE)} (information_name)
            VALUES %s
            ON CONFLICT (information_name) DO NOTHING
            """,
            [(clean_value(x),) for x in df["information"].dropna().drop_duplicates()],
        )

    conn.commit()


def load_fact(conn, df):
    fact_cols = [
        "order_id", "date", "stage", "channel", "information", "machine",
        "department", "customer_segment", "campaign",
        "quantity", "revenue", "cost", "profit", "profit_margin",
        "inventory_level", "calls", "waiting_time", "conversion_proxy",
        "budget", "target", "marketing_cost", "roi_marketing",
        "raw_material_cost", "labor_cost", "logistics_cost", "unit_cost",
        "revenue_per_unit", "cost_per_machine_unit", "market_size",
        "market_share", "region_profit_share"
    ]
    fact_df = df[fact_cols]

    with conn.cursor() as cur:
        execute_values_if_any(
            cur,
            f"""
            INSERT INTO {table_name(DW_SCHEMA, FACT_MARKETING_TABLE)} (
                order_id, full_date, stage_name, channel_name, information_name,
                machine_name, department_name, segment_name, campaign_name, quantity,
                revenue, cost, profit, profit_margin, inventory_level, calls,
                waiting_time, conversion_proxy, budget, target, marketing_cost,
                roi_marketing, raw_material_cost, labor_cost, logistics_cost,
                unit_cost, revenue_per_unit, cost_per_machine_unit,
                market_size, market_share, region_profit_share
            )
            VALUES %s
            ON CONFLICT (order_id) DO UPDATE SET
                full_date = EXCLUDED.full_date,
                stage_name = EXCLUDED.stage_name,
                channel_name = EXCLUDED.channel_name,
                information_name = EXCLUDED.information_name,
                machine_name = EXCLUDED.machine_name,
                department_name = EXCLUDED.department_name,
                segment_name = EXCLUDED.segment_name,
                campaign_name = EXCLUDED.campaign_name,
                quantity = EXCLUDED.quantity,
                revenue = EXCLUDED.revenue,
                cost = EXCLUDED.cost,
                profit = EXCLUDED.profit,
                profit_margin = EXCLUDED.profit_margin,
                inventory_level = EXCLUDED.inventory_level,
                calls = EXCLUDED.calls,
                waiting_time = EXCLUDED.waiting_time,
                conversion_proxy = EXCLUDED.conversion_proxy,
                budget = EXCLUDED.budget,
                target = EXCLUDED.target,
                marketing_cost = EXCLUDED.marketing_cost,
                roi_marketing = EXCLUDED.roi_marketing,
                raw_material_cost = EXCLUDED.raw_material_cost,
                labor_cost = EXCLUDED.labor_cost,
                logistics_cost = EXCLUDED.logistics_cost,
                unit_cost = EXCLUDED.unit_cost,
                revenue_per_unit = EXCLUDED.revenue_per_unit,
                cost_per_machine_unit = EXCLUDED.cost_per_machine_unit,
                market_size = EXCLUDED.market_size,
                market_share = EXCLUDED.market_share,
                region_profit_share = EXCLUDED.region_profit_share
            """,
            to_records(fact_df),
        )
    conn.commit()


def main():
    raw_query = f"SELECT * FROM {table_name(STG_SCHEMA, RAW_TABLE)}"

    with psycopg2.connect(**DB_CONFIG) as conn:
        print("Connected to PostgreSQL")

        raw_df = pd.read_sql(raw_query, conn)
        print(f"Raw rows: {len(raw_df)}")

        clean_df = clean_data(raw_df)
        print(f"Clean rows: {len(clean_df)}")

        load_dimensions(conn, clean_df)
        load_fact(conn, clean_df)

        print("Marketing ETL finished")
        print(f"Loaded dimensions and fact into schema: {DW_SCHEMA}")
        print(f"Fact table: {FACT_MARKETING_TABLE}")


if __name__ == "__main__":
    main()
