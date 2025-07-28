# analysis.py

import sys
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

def main():
    if len(sys.argv) != 2:
        print("Usage: python analysis.py <path_to_warehouse_db>")
        sys.exit(1)
    db_path = sys.argv[1]

    # 1) Connect and load the flattened view
    conn = sqlite3.connect(db_path)
    df = pd.read_sql("SELECT * FROM view_order_analysis", conn)
    conn.close()

    # Ensure order_date is datetime
    df['order_date'] = pd.to_datetime(df['order_date'])

    # 2) Orders by Region
    orders_by_region = (
        df.groupby('region_name')['order_id']
          .count()
          .reset_index(name='order_count')
          .sort_values('order_count', ascending=False)
    )
    print("1. Orders by Region:\n", orders_by_region, "\n")

    # 3) Total Freight by Region
    freight_by_region = (
        df.groupby('region_name')['freight']
          .sum()
          .reset_index(name='total_freight')
          .sort_values('total_freight', ascending=False)
    )
    print("2. Total Freight by Region:\n", freight_by_region, "\n")

    # 4) Average Weather by Region
    weather_by_region = (
        df.groupby('region_name')[['temperature_C', 'humidity_pct']]
          .mean()
          .reset_index()
    )
    print("3. Average Weather by Region:\n", weather_by_region, "\n")

    # 5) Correlation Freight vs Temperature
    corr = df['freight'].corr(df['temperature_C'])
    print(f"4. Correlation (freight vs temperature): {corr:.2f}\n")

    # 6) Orders by Weather Condition
    orders_by_weather = (
        df.groupby('weather_main')['order_id']
          .count()
          .reset_index(name='order_count')
          .sort_values('order_count', ascending=False)
    )
    print("5. Orders by Weather Condition:\n", orders_by_weather, "\n")

    # 7) Average Freight by Weather Condition
    avg_freight_by_weather = (
        df.groupby('weather_main')['freight']
          .mean()
          .reset_index(name='avg_freight')
          .sort_values('avg_freight', ascending=False)
    )
    print("6. Average Freight by Weather Condition:\n", avg_freight_by_weather, "\n")

    # 8) Weekly Orders by Region (CSV)
    ts = (
        df.set_index('order_date')
          .groupby('region_name')['order_id']
          .resample('W')
          .count()
          .unstack('region_name')
          .fillna(0)
    )
    ts.to_csv('weekly_orders_by_region.csv')
    print("7. Weekly orders by region saved to 'weekly_orders_by_region.csv'\n")

    top_customers = (
    df.groupby(['customer_id','company_name'])['order_id']
      .count()
      .reset_index(name='order_count')
      .sort_values('order_count', ascending=False)
      .head(10)
    )
    print("8. Top 10 Customers by Order Count:\n", top_customers, "\n")


    # 10) Top 10 Regions by Average Freight
    avg_freight_region = (
        freight_by_region
          .assign(avg_freight=df.groupby('region_name')['freight'].mean().values)
          .sort_values('avg_freight', ascending=False)
          .head(10)
    )
    print("9. Top 10 Regions by Average Freight:\n", avg_freight_region, "\n")

    # 11) Freight Distribution Histogram
    plt.figure()
    plt.hist(df['freight'], bins=20)
    plt.xlabel('Freight')
    plt.ylabel('Order Count')
    plt.title('Distribution of Freight Charges')
    plt.show()
    print("10. Displayed freight distribution histogram.")

    # 12. Orders for Countries with Region Changes (Pre‑ vs Post‑2016)
    # ----------------------------------------------------------------
    # 1) Identify countries whose mapping changed
    conn = sqlite3.connect(db_path)
    regions = pd.read_sql("SELECT * FROM raw_region_mapping", conn)
    conn.close()
    # Normalize column names
    regions = regions.rename(columns={
        'Region until 2016': 'region_before',
        'Region after 2016':  'region_after'
    })
    changed = regions.loc[
        regions['region_before'] != regions['region_after'],
        'Country'
    ].tolist()

    # 2) Filter orders for those countries
    df_changed = df[df['country'].isin(changed)].copy()

    # 3) Tag each order as pre‑ or post‑2016
    df_changed['period'] = df_changed['order_date'].dt.year.map(
        lambda y: 'post_2016' if y >= 2016 else 'pre_2016'
    )

    # 4) Pivot to compare counts
    pivot = (
        df_changed
        .groupby(['country','period'])['order_id']
        .count()
        .unstack(fill_value=0)
        .reset_index()
    )
    print("11. Orders by Country (where region changed) Pre vs Post 2016:\n", pivot, "\n")


if __name__ == "__main__":
    main()
