import pandas as pd
import sqlite3

def create_big_table(db_path='retail_warehouse.db'):
    conn = sqlite3.connect(db_path)
    
    # 1. Pull cleaned data
    df_sales = pd.read_sql("SELECT * FROM trf_sales", conn)
    df_jp_items = pd.read_sql("SELECT * FROM trf_jp_items", conn)
    df_mm_items = pd.read_sql("SELECT * FROM trf_mm_items", conn)
    
    # 2. Process Japan Store
    # We join sales with Japan's specific items list
    df_jp = df_sales.merge(df_jp_items, left_on='product_id', right_on='id')
    df_jp['country'] = 'Japan'

    # 3. Process Myanmar Store
    # We join the same sales with Myanmar's specific items list
    df_mm = df_sales.merge(df_mm_items, left_on='product_id', right_on='id')
    df_mm['country'] = 'Myanmar'

    # 4. Combine into the final Big Table
    # This stacks Myanmar data underneath Japan data
    df_big_table = pd.concat([df_jp, df_mm], ignore_index=True)
    
    # Calculate revenue
    df_big_table['total_revenue_usd'] = df_big_table['quantity'] * df_big_table['unit_price_usd']
    
    # 5. Save
    df_big_table.to_sql('big_table', conn, if_exists='replace', index=False)
    
    # VERIFICATION: Check counts
    check = pd.read_sql("SELECT country, COUNT(*) as rows FROM big_table GROUP BY country", conn)
    print("--- DATA LOAD VERIFICATION ---")
    print(check)
    
    conn.close()

if __name__ == "__main__":
    create_big_table()