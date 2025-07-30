import sys
import sqlite3
import pandas as pd


def connect(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection to the warehouse DB with named row access."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def freight_summary(conn: sqlite3.Connection) -> pd.Series:
    """Compute descriptive stats and key percentiles for freight cost."""
    df = pd.read_sql("SELECT freight FROM fact_order;", conn)
    desc = df['freight'].describe()
    percentiles = df['freight'].quantile([0.05, 0.25, 0.5, 0.75, 0.95])
    percentiles.index = ['5th%', '25th%', '50th%', '75th%', '95th%']
    return pd.concat([desc, percentiles])


def orders_by_regiona(conn: sqlite3.Connection) -> pd.DataFrame:
    """Count orders grouped by region."""
    df = pd.read_sql(
        "SELECT r.region_name AS Region, COUNT(*) AS Orders "
        "FROM fact_order f "
        "JOIN dim_region r ON f.region_key = r.region_key "
        "WHERE f.order_date >= '2016-01-01' "
        "GROUP BY r.region_name;", conn
    )
    return df.sort_values('Orders', ascending=False)

def orders_by_regionb(conn: sqlite3.Connection) -> pd.DataFrame:
    """Count orders grouped by region."""
    df = pd.read_sql(
        "SELECT r.region_name AS Region, COUNT(*) AS Orders "
        "FROM fact_order f "
        "JOIN dim_region r ON f.region_key = r.region_key "
        "WHERE f.order_date < '2016-01-01' "
        "GROUP BY r.region_name;", conn
    )
    return df.sort_values('Orders', ascending=False)


def average_freight_by_region(conn: sqlite3.Connection) -> pd.DataFrame:
    """Calculate average freight cost per region."""
    df = pd.read_sql(
        "SELECT r.region_name AS Region, ROUND(AVG(freight), 2) AS Avg_Freight "
        "FROM fact_order f "
        "JOIN dim_region r ON f.region_key = r.region_key "
        "WHERE f.order_date < '2016-01-01' "
        "GROUP BY r.region_name;", conn
    )
    return df.sort_values('Avg_Freight', ascending=False)


def temp_freight_correlation(conn: sqlite3.Connection) -> float:
    """Compute Pearson correlation between temperature and freight cost."""
    df = pd.read_sql(
        "SELECT w.temperature_C AS Temp, f.Freight AS Freight "
        "FROM fact_order f "
        "LEFT JOIN dim_weather w ON f.weather_key = w.weather_key;", conn
    )
    df_clean = df.dropna()
    return df_clean['Temp'].corr(df_clean['Freight'])


def average_temp_by_region(conn: sqlite3.Connection) -> pd.DataFrame:
    """Compute average temperature per Country."""
    df = pd.read_sql(
        "SELECT r.country AS Country, ROUND(AVG(w.temperature_C), 1) AS Avg_Temp_C "
        "FROM fact_order f "
        "JOIN dim_region r ON f.region_key = r.region_key "
        "JOIN dim_weather w ON f.weather_key = w.weather_key "
        "GROUP BY r.country;", conn
    )
    return df.sort_values('Avg_Temp_C', ascending=False)


def weather_distribution_by_region(conn: sqlite3.Connection) -> pd.DataFrame:
    """Calculate the percentage distribution of weather types per region."""
    df = pd.read_sql(
        "SELECT r.region_name AS Region, w.weather_main AS Weather, COUNT(*) AS Count "
        "FROM fact_order f "
        "JOIN dim_region r ON f.region_key = r.region_key "
        "LEFT JOIN dim_weather w ON f.weather_key = w.weather_key "
        "WHERE f.order_date >= '2016-01-01'"
        "GROUP BY r.region_name, w.weather_main;", conn
    )
    # compute percentages
    total = df.groupby('Region')['Count'].transform('sum')
    df['Percentage'] = (df['Count'] / total * 100).round(1)
    return df.sort_values(['Region', 'Percentage'], ascending=[True, False])


def main(db_path: str):
    conn = connect(db_path)

    print("\n=== FREIGHT COST SUMMARY ===")
    print(freight_summary(conn).to_string())

    print("\n=== ORDERS BY REGION BEFORE 2016===")
    print(orders_by_regionb(conn).to_string(index=False))

    print("\n=== ORDERS BY REGION AFTER 2016===")
    print(orders_by_regiona(conn).to_string(index=False))

    print("\n=== AVERAGE FREIGHT BY REGION BEFORE 2016===")
    print(average_freight_by_region(conn).to_string(index=False))

    print("\n=== TEMPERATURE VS FREIGHT CORRELATION ===")
    print(f"Pearson Temp-Freight: {temp_freight_correlation(conn):.3f}")

    print("\n=== AVERAGE TEMPERATURE BY COUNTRY ===")
    print(average_temp_by_region(conn).to_string(index=False))

    print("\n=== WEATHER TYPE DISTRIBUTION BY REGION FROM 2016 ===")
    print(weather_distribution_by_region(conn).to_string(index=False))

    conn.close()


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: python analysis.py <warehouse_db>")
        sys.exit(1)
    main(sys.argv[1])