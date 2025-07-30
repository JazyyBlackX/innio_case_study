# load.py

import sys
import sqlite3
import pandas as pd
import logging

# Basic logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def connect(db_path: str) -> sqlite3.Connection:
    """Connect to the SQLite database."""
    logger.info(f"Connecting to database at {db_path}")
    return sqlite3.connect(db_path)

def build_dim_customer(conn: sqlite3.Connection) -> None:
    """Build dim_customer from raw_customers."""
    logger.info("Loading dimension table 'dim_customer'")
    df = pd.read_sql(
        "SELECT DISTINCT CustomerID, CompanyName, ContactName, City, Country FROM raw_customers;",
        conn
    )
    df.insert(0, 'customer_key', range(1, len(df) + 1))
    df.dropna(subset=['Country'], inplace=True)
    df.to_sql('dim_customer', conn, if_exists='replace', index=False)
    logger.info(f"dim_customer loaded with {len(df)} rows")

def build_dim_region(conn: sqlite3.Connection) -> None:
    """Build dim_region (SCD Type2) from raw_region_mapping."""
    logger.info("Loading dimension table 'dim_region'")
    regions = pd.read_sql("SELECT * FROM raw_region_mapping", conn)
    regions = regions.rename(columns={
        'Region until 2016': 'region_before',
        'Region after 2016':  'region_after'
    })

    records = []
    for _, row in regions.iterrows():
        country = row['Country']
        before = row['region_before']
        after  = row['region_after']

        # always include the “before” period
        records.append({
            'country': country,
            'region_name': before,
            'effective_start': '1900-01-01',
            'effective_end':   '2015-12-31'
        })
        # only include “after” if it changed
        if after != before:
            records.append({
                'country': country,
                'region_name': after,
                'effective_start': '2016-01-01',
                'effective_end':   '9999-12-31'
            })

    df_dim = pd.DataFrame(records)
    df_dim.insert(0, 'region_key', range(1, len(df_dim) + 1))
    df_dim.to_sql('dim_region', conn, if_exists='replace', index=False)
    logger.info(f"dim_region loaded with {len(df_dim)} rows")

def build_dim_weather(conn: sqlite3.Connection) -> None:
    """Build dim_weather from raw_weather, keeping only the latest record per city."""
    logger.info("Loading dimension table 'dim_weather' (deduped by city)")

    # 1. Pull everything from raw_weather
    df = pd.read_sql("SELECT * FROM raw_weather;", conn)

    # 2. Ensure the timestamp is a datetime
    df['weather_timestamp'] = pd.to_datetime(df['weather_timestamp'])

    # 3. Sort so latest timestamp per city is last, then drop duplicates
    df = (
        df
        .sort_values(['city', 'weather_timestamp'])
        .drop_duplicates(subset='city', keep='last')
        .reset_index(drop=True)
    )

    # 4. Rename any awkward columns
    df = df.rename(columns={
        "humidity_%": "humidity_pct",
        "cloud_coverage_%": "cloud_coverage_pct",
        # if your raw column was named 'clouds_all':
        "clouds_all": "cloud_coverage_pct"
    })

    # 5. Add a surrogate key
    df.insert(0, 'weather_key', range(1, len(df) + 1))

    # 6. Persist back to dim_weather
    df.to_sql("dim_weather", conn, if_exists="replace", index=False)
    logger.info(f"dim_weather loaded with {len(df)} rows (one per city)")

def build_fact_order(conn: sqlite3.Connection) -> None:
    """Build fact_order by joining staging with dims (including weather)."""
    logger.info("Loading fact table 'fact_order'")
    query = """
    SELECT
      ROW_NUMBER() OVER (ORDER BY s.OrderID)       AS order_key,
      s.OrderID                                   AS order_id,
      date(s.OrderDate)                           AS order_date,
      s.Freight                                   AS freight,
      c.customer_key                              AS customer_key,
      r.region_key                                AS region_key,
      w.weather_key                               AS weather_key
    FROM stg_order_customer_region AS s

    -- weather is optional, so LEFT JOIN
    LEFT JOIN dim_weather AS w
      ON s.City = w.city

    -- customer must match by natural key
    JOIN dim_customer AS c
      ON s.CustomerID = c.CustomerID

    -- region match by country + date range
    JOIN dim_region AS r
      ON s.Country = r.country
     AND date(s.OrderDate) BETWEEN date(r.effective_start) 
                                AND date(r.effective_end)
    """
    df = pd.read_sql(query, conn)
    df.to_sql('fact_order', conn, if_exists='replace', index=False)
    logger.info(f"fact_order loaded with {len(df)} rows")

def build_view_order_analysis(conn: sqlite3.Connection) -> None:
    """Create a flattened view for reporting and ad‑hoc analysis."""
    logger.info("Building view 'view_order_analysis'")
    conn.execute("DROP VIEW IF EXISTS view_order_analysis;")
    sql = """
    CREATE VIEW view_order_analysis AS
    SELECT
    f.order_id,
    f.order_date,
    f.freight,
    c.CustomerID   AS customer_id,
    c.CompanyName  AS company_name,
    c.City         AS city,
    c.Country      AS country,
    r.region_name  AS region_name,
    w.temperature_C     AS temperature_C,
    w.feels_like_C      AS feels_like_C,
    w.humidity_pct      AS humidity_pct,
    w.pressure_hPa      AS pressure_hPa,
    w.wind_speed_mps    AS wind_speed_mps,
    w.cloud_coverage_pct AS cloud_coverage_pct,
    w.weather_main      AS weather_main,
    w.weather_description AS weather_description
    FROM fact_order       AS f
    JOIN dim_customer    AS c ON f.customer_key = c.customer_key
    JOIN dim_region      AS r ON f.region_key   = r.region_key
    LEFT JOIN dim_weather AS w ON f.weather_key  = w.weather_key;
    """
    conn.execute(sql)
    logger.info("view_order_analysis created")

def main():
    if len(sys.argv) != 2:
        logger.error("Usage: python load.py <warehouse_db>")
        sys.exit(1)
    db_path = sys.argv[1]
    conn = connect(db_path)
    try:
        build_dim_customer(conn)
        build_dim_region(conn)
        build_dim_weather(conn)
        build_fact_order(conn)
        build_view_order_analysis(conn)
        logger.info("Load process completed successfully.")
    except Exception:
        logger.exception("Load process failed")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == '__main__':
    main()
