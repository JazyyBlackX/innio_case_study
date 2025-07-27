import sqlite3
import pandas as pd
from pathlib import Path

def extract_customers_orders(db_path: str):
    conn = sqlite3.connect(db_path)

  
    customers_df = pd.read_sql_query("SELECT * FROM Customers", conn)
    orders_df = pd.read_sql_query("SELECT * FROM Orders", conn)

   
    conn.close()

    return customers_df, orders_df


if __name__ == "__main__":
    db_file = Path(__file__).resolve().parents[1] / "data" / "northwind.db"
    customers, orders = extract_customers_orders(str(db_file))
    print("Customers:\n", customers.head())
    print("Orders:\n", orders.head())
