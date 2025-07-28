import sys
import sqlite3
import pandas as pd
import logging

# Configure basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

# Simple transform script for staging tables

def connect(db_path):
    """Connect to the SQLite database."""
    logger.info(f"Connecting to database at {db_path}")
    return sqlite3.connect(db_path)


def transform_order_customer(conn):
    """Create staging table of orders joined with customers."""
    logger.info("Transform: building stg_order_customer")
    query = (
        "SELECT o.OrderID, o.CustomerID, o.OrderDate, o.Freight, "
        "c.CompanyName, c.City, c.Country "
        "FROM raw_orders o "
        "LEFT JOIN raw_customers c ON o.CustomerID = c.CustomerID"
    )
    df = pd.read_sql_query(query, conn)
    missing = df['CompanyName'].isnull().sum()
    if missing:
        logger.warning(f"{missing} orders without matching customer records")
    df.to_sql('stg_order_customer', conn, if_exists='replace', index=False)
    logger.info(f"Loaded {len(df)} rows into stg_order_customer")


def transform_order_region(conn, region_file):
    """Enrich staging orders with region mappings (correct column names)."""
    logger.info("Transform: building stg_order_customer_region")
    # Read staging orders
    df = pd.read_sql('SELECT * FROM stg_order_customer', conn)
    # Read region mapping and normalize column names to match actual Excel headers
    regions = pd.read_excel(region_file)
    rename_map = {
        'Region until 2016': 'Region_before2016',
        'Region after 2016': 'Region_after2016'
    }
    regions = regions.rename(columns=rename_map)
    # Merge on country
    needed_cols = ['Country', 'Region_before2016', 'Region_after2016']
    df = df.merge(
        regions[needed_cols],
        on='Country', how='left'
    )
    # Convert OrderDate to string to extract year
    df['OrderDate_str'] = df['OrderDate'].str.slice(0, 10)

    # Convert to datetime
    df['OrderDate'] = pd.to_datetime(df['OrderDate_str'],format='%Y-%m-%d',errors='coerce')

    # and extract the year for your SCD logic
    df['OrderYear'] = df['OrderDate'].dt.year
    # Apply SCD region selection
    df['Region'] = df.apply(
        lambda x: x['Region_after2016'] if x['OrderYear'] >= 2016 else x['Region_before2016'],
        axis=1
    )
    missing = df['Region'].isnull().sum()
    if missing:
        logger.warning(f"{missing} orders without region mapping")
        logger.warning(f"{missing} orders without region mapping")
    df.to_sql('stg_order_customer_region', conn, if_exists='replace', index=False)
    logger.info(f"Loaded {len(df)} rows into stg_order_customer_region")


if __name__ == '__main__':
    if len(sys.argv) != 3:
        logger.error("Usage: python transform.py <warehouse_db> <region_file>")
        sys.exit(1)
    db_path, region_file = sys.argv[1], sys.argv[2]
    conn = connect(db_path)
    try:
        transform_order_customer(conn)
        transform_order_region(conn, region_file)
        logger.info("Transform process completed successfully.")
    except Exception:
        logger.exception("Transform process failed")
        sys.exit(1)
    finally:
        conn.close()
