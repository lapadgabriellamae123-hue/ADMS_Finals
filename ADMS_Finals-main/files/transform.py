import sqlite3
import pandas as pd
import numpy as np

def clean_sqlite_table(db_path='retail_warehouse.db'):
    conn = sqlite3.connect(db_path)

    try:
        # --- 1. LOAD RAW DATA ---
        df_sales = pd.read_sql("SELECT * FROM stg_sales_raw", conn)
        df_jp_items = pd.read_sql("SELECT * FROM stg_jp_items", conn)
        df_mm_items = pd.read_sql("SELECT * FROM stg_mm_items", conn)

        # --- 2. SALES TRANSFORMATION (New Section) ---
        # Clean column names and strip whitespace
        df_sales.columns = df_sales.columns.str.strip()
        # Ensure date column is in datetime format for better loading
        df_sales['date'] = pd.to_datetime(df_sales['date'])

        # --- 3. JAPAN ITEMS TRANSFORMATION ---
        df_jp_items.rename(columns={'category': 'product_category'}, inplace=True)
        df_jp_items['unit_price_usd'] = df_jp_items['price'] * 0.0067

        # --- 4. MYANMAR ITEMS TRANSFORMATION ---
        df_mm_items.rename(columns={'name': 'product_name', 'type': 'product_category'}, inplace=True)
        df_mm_items['unit_price_usd'] = df_mm_items['price']

        # --- 5. GENERAL CLEANING ---
        for df in [df_sales, df_jp_items, df_mm_items]:
            df.replace('', np.nan, inplace=True)
            df.drop_duplicates(inplace=True)

        # --- 6. SAVE TO TRANSFORMATION LAYER ---
        # This creates the 'trf_sales' table your load.py is looking for
        df_sales.to_sql('trf_sales', conn, if_exists='replace', index=False)
        df_jp_items.to_sql('trf_jp_items', conn, if_exists='replace', index=False)
        df_mm_items.to_sql('trf_mm_items', conn, if_exists='replace', index=False)
        
        print("SUCCESS: Sales and Items standardized and saved.")

    except Exception as e:
        print(f"TRANSFORM ERROR: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clean_sqlite_table()