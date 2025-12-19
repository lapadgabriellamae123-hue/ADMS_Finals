import sqlite3
import pandas as pd
import os

TRANSFORM_DB = "data/Transformation/transformation.db"
PRESENTATION_DB = "data/Presentation/BIG_TABLE.db"

def build_big_table():
    """Joins sales and items into one consolidated Big Table."""
    os.makedirs("data/Presentation", exist_ok=True)
    t_conn = sqlite3.connect(TRANSFORM_DB)
    p_conn = sqlite3.connect(PRESENTATION_DB)

    # 1. Load Transformed Data
    jp_s = pd.read_sql("SELECT * FROM trf_jp_sales", t_conn)
    jp_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_jp_items", t_conn)
    
    mm_s = pd.read_sql("SELECT * FROM trf_mm_sales", t_conn)
    mm_i = pd.read_sql("SELECT id, product_name, product_category, unit_price_usd FROM trf_mm_items", t_conn)

    # 2. Join Sales with Items
    jp_final = jp_s.merge(jp_i, left_on="product_id", right_on="id").drop(columns=["id"])
    jp_final["store"] = "Japan"

    mm_final = mm_s.merge(mm_i, left_on="product_id", right_on="id").drop(columns=["id"])
    mm_final["store"] = "Myanmar"

    # 3. Create Big Table
    big_table = pd.concat([jp_final, mm_final], ignore_index=True)
    big_table["total_revenue_usd"] = big_table["quantity"] * big_table["unit_price_usd"]

    # 4. Save to Presentation
    big_table.to_sql("fact_global_sales", p_conn, if_exists="replace", index=False)
    
    print("[LOAD] Consolidated BIG TABLE created.")
    t_conn.close()
    p_conn.close()

if __name__ == "__main__":
    build_big_table()