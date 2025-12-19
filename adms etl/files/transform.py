import sqlite3
import pandas as pd
import numpy as np

def clean_sqlite_table(db_path='retail_warehouse.db'):
    conn = sqlite3.connect(db_path)

    try:
        # Load Raw Data
        df_jp_items = pd.read_sql("SELECT * FROM stg_jp_items", conn)
        df_mm_items = pd.read_sql("SELECT * FROM stg_mm_items", conn)

        # --- JAPAN TRANSFORMATION ---
        # Japan CSV uses: id, product_name, category, price
        df_jp_items.rename(columns={'category': 'product_category'}, inplace=True)
        df_jp_items['unit_price_usd'] = df_jp_items['price'] * 0.0067

        # --- MYANMAR TRANSFORMATION ---
        # Myanmar CSV uses: id, name, type, price
        # We rename them to match the 'load.py' query expectations
        df_mm_items.rename(columns={
            'name': 'product_name', 
            'type': 'product_category'
        }, inplace=True)
        
        # Prices are already in USD for Myanmar
        df_mm_items['unit_price_usd'] = df_mm_items['price']

        # --- CLEANING (Strip spaces and handle nulls) ---
        for df in [df_jp_items, df_mm_items]:
            df.columns = df.columns.str.strip()
            df.replace('', np.nan, inplace=True)
            df.drop_duplicates(inplace=True)

        # Save to Transformation Layer
        df_jp_items.to_sql('trf_jp_items', conn, if_exists='replace', index=False)
        df_mm_items.to_sql('trf_mm_items', conn, if_exists='replace', index=False)
        
        print("SUCCESS: Myanmar and Japan items standardized and saved.")

    except Exception as e:
        print(f"TRANSFORM ERROR: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    clean_sqlite_table()