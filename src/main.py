import pandas as pd

file_path = 'masan_case.xlsx'

xl = pd.ExcelFile(file_path)
print("Danh sách các Sheets:", xl.sheet_names)

for sheet_name in xl.sheet_names:
    print(f"\n=== Schema của Sheet: '{sheet_name}' ===")
    
    df = xl.parse(sheet_name, nrows=100) 
    
    print(df.dtypes)
    