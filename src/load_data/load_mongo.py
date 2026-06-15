import pandas as pd
from pymongo import MongoClient

def load_production_data(excel_path):
    print("Reading Excel file...")
    df = pd.read_excel(excel_path, sheet_name='Sheet1')
    df.columns = [c.strip() for c in df.columns]
    
    # Connect to MongoDB
    client = MongoClient("mongodb://admin:admin_password@localhost:27017/")
    db = client["production_db"]
    
    try:
        # ========================================================
        # 0. TẠO MÃ SẢN PHẨM (PRODUCT_ID) TỰ ĐỘNG
        # ========================================================
        print("Generating Product IDs...")
        unique_products = df['Product'].dropna().unique().tolist()
        unique_products.sort() # Sắp xếp A-Z để mã ID luôn cố định mỗi lần chạy
        
        # Tạo từ điển ánh xạ: VD: {'Sữa tươi': 'P001', 'Xúc xích': 'P002'}
        product_map = {prod: f"P{str(idx+1).zfill(3)}" for idx, prod in enumerate(unique_products)}

        # ========================================================
        # 1. Master Data: Categories & Products
        # ========================================================
        print("Loading Master Data to MongoDB...")
        categories = df['Category'].dropna().unique().tolist()
        db["Categories"].delete_many({}) 
        if categories:
            db["Categories"].insert_many([{"CategoryName": cat} for cat in categories])
        
        products_df = df[['Product', 'Category']].drop_duplicates().dropna(subset=['Product'])
        products_df = products_df.astype(object).where(pd.notnull(products_df), None)
        
        db["Products"].delete_many({})
        if not products_df.empty:
            db["Products"].insert_many([
                {
                    "ProductID": product_map[row['Product']], # Dùng từ điển để lấy mã
                    "ProductName": row['Product'], 
                    "CategoryName": row['Category']
                }
                for _, row in products_df.iterrows()
            ])
        
        # ========================================================
        # 2. Departments
        # ========================================================
        print("Loading Departments...")
        departments = df['Department'].dropna().unique().tolist()
        db["Departments"].delete_many({})
        if departments:
            db["Departments"].insert_many([{"DepartmentID": idx+1, "DepartmentName": dept} for idx, dept in enumerate(departments)])
        dept_map = {dept: idx+1 for idx, dept in enumerate(departments)}
        
        # ========================================================
        # 3. Production Logs
        # ========================================================
        print("Loading Production Logs...")
        prod_df = df[['Date', 'Product', 'Department', 'Stage', 'Machine', 'InventoryLevel', 'RawMaterialCost', 'LaborCost']].copy()
        prod_df = prod_df.drop_duplicates().dropna(subset=['Product'])
        
        prod_df['Date'] = prod_df['Date'].astype(str).replace(['nan', 'NaT', 'None'], None)
        
        for col in ['InventoryLevel', 'RawMaterialCost', 'LaborCost']:
            prod_df[col] = pd.to_numeric(prod_df[col], errors='coerce')
        
        prod_df = prod_df.astype(object).where(pd.notnull(prod_df), None)
        
        prod_logs = []
        for _, row in prod_df.iterrows():
            prod_logs.append({
                "LogDate": row['Date'],
                "ProductID": product_map[row['Product']],  # Dùng từ điển để lấy mã thay vì Tên
                "DepartmentID": dept_map.get(row['Department']),
                "Stage": row['Stage'],
                "Machine": row['Machine'],
                "InventoryLevel": row['InventoryLevel'],
                "RawMaterialCost": row['RawMaterialCost'],
                "LaborCost": row['LaborCost']
            })
        
        db["ProductionLogs"].delete_many({})
        if prod_logs:
            db["ProductionLogs"].insert_many(prod_logs)
            
        # ========================================================
        # 4. Logistics Costs
        # ========================================================
        print("Loading Logistics Costs...")
        log_df = df[['Date', 'Branch', 'LogisticsCost']].copy()
        log_df = log_df.drop_duplicates().dropna(subset=['Branch'])
        
        log_df['Date'] = log_df['Date'].astype(str).replace(['nan', 'NaT', 'None'], None)
        log_df['LogisticsCost'] = pd.to_numeric(log_df['LogisticsCost'], errors='coerce')
        log_df = log_df.astype(object).where(pd.notnull(log_df), None)
        
        log_costs = []
        for _, row in log_df.iterrows():
            log_costs.append({
                "LogDate": row['Date'],
                "BranchName": row['Branch'],
                "LogisticsCost": row['LogisticsCost']
            })
            
        db["LogisticsCosts"].delete_many({})
        if log_costs:
            db["LogisticsCosts"].insert_many(log_costs)
            
        print("MongoDB Production Data Loading completed successfully!")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        client.close()

if __name__ == "__main__":
    load_production_data("masan_case.xlsx")