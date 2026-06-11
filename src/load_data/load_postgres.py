import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

def load_sales_data(excel_path):
    print("Reading Excel file...")
    df = pd.read_excel(excel_path, sheet_name='Sheet1')
    
    # Chuẩn hóa tên cột
    df.columns = [c.strip() for c in df.columns]
    
    # Kết nối PostgreSQL
    conn = psycopg2.connect(
        host="localhost",
        port=5432,
        user="admin",
        password="admin_password",
        database="sales_db"
    )
    cursor = conn.cursor()
    
    try:
        # 1. Load Categories
        print("Loading Categories...")
        categories = df['Category'].dropna().unique()
        categories_data = [(cat,) for cat in categories]
        # Bổ sung target (category_name) cho ON CONFLICT
        execute_values(cursor, 
                       "INSERT INTO categories (category_name) VALUES %s ON CONFLICT (category_name) DO NOTHING;", 
                       categories_data)
        
        cursor.execute("SELECT category_id, category_name FROM categories;")
        cat_map = {name: id for id, name in cursor.fetchall()}
        
        # 2. Load Products
        print("Loading Products...")
        products_df = df[['Product', 'Category']].drop_duplicates().dropna(subset=['Product'])
        products_data = [(row['Product'], row['Product'], cat_map.get(row['Category'])) for _, row in products_df.iterrows()]
        execute_values(cursor, 
                       "INSERT INTO products (product_id, product_name, category_id) VALUES %s ON CONFLICT (product_id) DO NOTHING;", 
                       products_data)
        
        # 3. Load Branches
        print("Loading Branches...")
        branches_df = df[['Branch', 'Region']].drop_duplicates().dropna(subset=['Branch'])
        branches_data = [
            (f"BR_{str(row['Branch']).upper().replace(' ', '_')}", row['Branch'], row['Region']) 
            for _, row in branches_df.iterrows()
        ]
        execute_values(cursor, 
                       "INSERT INTO branches (branch_id, branch_name, region) VALUES %s ON CONFLICT (branch_id) DO NOTHING;", 
                       branches_data)
        
        branch_map = {name: id for id, name, _ in branches_data}
        
        # 4. Load Orders
        print("Loading Orders...")
        orders_df = df[['OrderID', 'Date', 'Branch', 'Channel', 'CustomerSegment']].drop_duplicates(subset=['OrderID']).dropna(subset=['OrderID'])
        
        # CHUYỂN ĐỔI: Ép toàn bộ cột Date thành chuỗi thô (Raw String), thay đổi NaN thành None (NULL)
        orders_df['Date'] = orders_df['Date'].astype(str).replace('nan', None).replace('NaT', None)
        
        orders_data = [
            (int(row['OrderID']), row['Date'], branch_map.get(row['Branch']), row['Channel'], row['CustomerSegment'])
            for _, row in orders_df.iterrows()
        ]
        execute_values(cursor, 
                       "INSERT INTO orders (order_id, order_date, branch_id, sales_channel, customer_segment) VALUES %s ON CONFLICT (order_id) DO NOTHING;", 
                       orders_data)
        
        # 5. Load Order Details
        print("Loading Order Details...")
        # Ép các cột tính toán thành số. Nếu là rác (chữ cái), biến thành NaN
        df['Quantity'] = pd.to_numeric(df['Quantity'], errors='coerce')
        df['RevenuePerUnit'] = pd.to_numeric(df['RevenuePerUnit'], errors='coerce')
        df['UnitCost'] = pd.to_numeric(df['UnitCost'], errors='coerce')
        
        # Replace toàn bộ NaN của Pandas thành None của Python để psycopg2 hiểu là NULL
        df = df.where(pd.notnull(df), None)
        
        details_df = df[['OrderID', 'Product', 'Quantity', 'RevenuePerUnit', 'UnitCost']].dropna(subset=['OrderID', 'Product'])
        details_data = [
            (int(row['OrderID']), row['Product'], row['Quantity'], row['RevenuePerUnit'], row['UnitCost'])
            for _, row in details_df.iterrows()
        ]
        execute_values(cursor, 
                       "INSERT INTO order_details (order_id, product_id, quantity, unit_price, unit_cost) VALUES %s ON CONFLICT (order_id, product_id) DO NOTHING;", 
                       details_data)
        
        conn.commit()
        print("PostgreSQL Sales Data Loading completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Đảm bảo tên file đúng với file của bạn
    load_sales_data("masan_case.xlsx")