import pandas as pd
import sqlite3
import os

def load_csv(db_path='retail_warehouse.db'):
    """
    Loads source csv data to sqlite file in the staging area.
    """
    conn = sqlite3.connect(db_path)
    
    # This prints exactly where your script is looking for CSVs
    print(f"Current Working Directory: {os.getcwd()}")

    sources = {
        'japan_branch.csv': 'stg_jp_branch',
        'japan_Customers.csv': 'stg_jp_customers',
        'japan_items.csv': 'stg_jp_items',
        'japan_payment.csv': 'stg_jp_payment',
        'myanmar_branch.csv': 'stg_mm_branch',
        'myanmar_customers.csv': 'stg_mm_customers',
        'myanmar_items.csv': 'stg_mm_items',
        'myanmar_payment.csv': 'stg_mm_payment',
        'sales_data.csv': 'stg_sales_raw'
    }

    print("--- Starting Extraction ---")
    for file_path, table_name in sources.items():
        if os.path.exists(file_path):
            df = pd.read_csv(file_path)
            # Clean column names to remove 'single quotes'
            df.columns = [col.strip("'").strip() for col in df.columns]
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            print(f"SUCCESS: {file_path} loaded into {table_name}")
        else:
            print(f"ERROR: File '{file_path}' NOT FOUND. Place it in {os.getcwd()}")
    
    conn.close()
    print("--- Extraction Finished ---")

if __name__ == "__main__":
    load_csv()