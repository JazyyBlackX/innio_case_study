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


def connect(db_path: str) -> sqlite3.Connection:    
    """Connect to a SQLite database."""
    logger.info(f"Connecting to database at {db_path}")
    return sqlite3.connect(db_path)


def extract_table(src_conn, table_name: str, dest_conn, dest_table: str) -> None:
    """Extract a full table from source and load into destination as raw."""
    logger.info(f"Extracting '{table_name}' into '{dest_table}'")
    df = pd.read_sql_query(f"SELECT * FROM {table_name};", src_conn)
    logger.info(f"  Retrieved {len(df)} rows from {table_name}")
    df.to_sql(dest_table, dest_conn, if_exists='replace', index=False)
    logger.info(f"  Loaded {len(df)} rows into {dest_table}")


def extract_region_mapping(dest_conn, region_file: str) -> None:
    """Read region mapping Excel file and load as raw_region_mapping."""
    logger.info(f"Reading region mapping from '{region_file}'")
    df = pd.read_excel(region_file)
    logger.info(f"  Read {len(df)} rows from region mapping")
    df.to_sql('raw_region_mapping', dest_conn, if_exists='replace', index=False)
    logger.info(f"  Loaded {len(df)} rows into raw_region_mapping")



if __name__ == '__main__':
    if len(sys.argv) != 4:
        logger.error("Usage: python extract.py <source_db> <dest_db> <region_file>")
        sys.exit(1)

    src_db, dest_db, region_file = sys.argv[1], sys.argv[2], sys.argv[3]

    try:
        src_conn = connect(src_db)
        dest_conn = connect(dest_db)

        extract_table(src_conn, 'Customers', dest_conn, 'raw_customers')
        extract_table(src_conn, 'Orders', dest_conn, 'raw_orders')
        extract_region_mapping(dest_conn, region_file)

        logger.info("Extract process completed successfully.")
    except Exception:
        logger.exception("Extract process failed")
        sys.exit(1)
    finally:
        try:
            src_conn.close()
            dest_conn.close()
        except Exception:
            pass
