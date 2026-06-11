import pandas as pd
import mysql.connector

def load_finance_data(excel_path):
    print("Reading Excel file...")
    df = pd.read_excel(excel_path, sheet_name='Sheet1')
    
    # Chuẩn hóa tên cột
    df.columns = [c.strip() for c in df.columns]
    
    # Kết nối MySQL Finance DB
    conn = mysql.connector.connect(
        host="localhost",
        port=3306,
        user="admin",
        password="admin_password",
        database="finance_db"
    )
    cursor = conn.cursor()
    
    try:
        # 1. Load Marketing Campaigns
        print("Loading Marketing Campaigns...")
        campaigns_df = df[['PromotionCampaign', 'Channel']].drop_duplicates().dropna(subset=['PromotionCampaign'])
        
        # Replace NaN bằng None để insert thành NULL
        campaigns_df = campaigns_df.where(pd.notnull(campaigns_df), None)
        
        for _, row in campaigns_df.iterrows():
            cursor.execute("""
                INSERT INTO marketing_campaigns (campaign_name, platform) 
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE platform=VALUES(platform);
            """, (row['PromotionCampaign'], row['Channel']))
            
        cursor.execute("SELECT campaign_id, campaign_name FROM marketing_campaigns;")
        camp_map = {name: id for id, name in cursor.fetchall()}
        
        # 2. Load Daily Marketing Spend
        print("Loading Daily Marketing Spend...")
        spend_df = df[['Date', 'PromotionCampaign', 'Region', 'MarketingCost']].copy()
        spend_df = spend_df.drop_duplicates().dropna(subset=['PromotionCampaign'])
        
        # CHUYỂN ĐỔI: Date thành chuỗi thô. Các chi phí thành số, lỗi rác thì biến thành None (NULL)
        spend_df['Date'] = spend_df['Date'].astype(str).replace(['nan', 'NaT', 'None'], None)
        spend_df['MarketingCost'] = pd.to_numeric(spend_df['MarketingCost'], errors='coerce')
        
        spend_df = spend_df.astype(object).where(pd.notnull(spend_df), None)
        
        for _, row in spend_df.iterrows():
            camp_id = camp_map.get(row['PromotionCampaign'])
            if camp_id:
                cursor.execute("""
                    INSERT INTO daily_marketing_spend (campaign_id, spend_date, region, amount_spent)
                    VALUES (%s, %s, %s, %s);
                """, (camp_id, row['Date'], row['Region'], row['MarketingCost']))
                
        # 3. Load Monthly Budgets
        print("Loading Monthly Budgets...")
        df_budget = df.copy()
        
        # Cố gắng Parse Date để lấy tháng (YYYY-MM). Nếu fail ra NaT thì cũng nhét vào DB dạng NULL
        df_budget['Date_Parsed'] = pd.to_datetime(df_budget['Date'], dayfirst=True, errors='coerce')
        df_budget['Month_Year'] = df_budget['Date_Parsed'].dt.strftime('%Y-%m')
        
        budget_df = df_budget[['Month_Year', 'Region', 'Budget', 'Target', 'MarketSize']].drop_duplicates(subset=['Month_Year', 'Region'])
        
        # Xử lý các cột số học
        budget_df['Budget'] = pd.to_numeric(budget_df['Budget'], errors='coerce')
        budget_df['Target'] = pd.to_numeric(budget_df['Target'], errors='coerce')
        budget_df['MarketSize'] = pd.to_numeric(budget_df['MarketSize'], errors='coerce')
        
        budget_df = budget_df.astype(object).where(pd.notnull(budget_df), None)
        
        for _, row in budget_df.iterrows():
            # Bỏ qua nếu dòng đó không có cả tháng lẫn vùng
            if row['Month_Year'] is None and row['Region'] is None:
                continue
                
            cursor.execute("""
                INSERT INTO monthly_budgets (month_year, region, budget_amount, target_revenue, market_size)
                VALUES (%s, %s, %s, %s, %s);
            """, (
                row['Month_Year'], 
                row['Region'], 
                row['Budget'], 
                row['Target'], 
                row['MarketSize']
            ))
            
        conn.commit()
        print("MySQL Finance Data Loading completed successfully!")
        
    except Exception as e:
        conn.rollback()
        print(f"Error occurred: {e}")
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    # Nhớ sửa lại tên file Excel của bạn ở đây
    load_finance_data("masan_case.xlsx")