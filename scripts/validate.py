# validate_etl.py

import sys
import logging
import sqlite3

import pandas as pd


def setup_logger():
    logger = logging.getLogger(__name__)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
        handler.setFormatter(fmt)
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False  # Prevent duplicate logs
    return logger

logger = setup_logger()


def connect(db_path: str) -> sqlite3.Connection:
    """Connect to SQLite and enable foreign keys."""
    conn = sqlite3.connect(db_path)
    conn.execute('PRAGMA foreign_keys = ON;')
    return conn


def validate_view_schema(conn: sqlite3.Connection, view: str, expected_columns: list) -> int:
    """Validate that a view has expected columns."""
    cursor = conn.execute(f"PRAGMA table_info({view});")
    cols = [row[1] for row in cursor.fetchall()]
    errors = 0
    for col in expected_columns:
        if col not in cols:
            logger.error(f"Schema check failed: view '{view}' missing column '{col}'")
            errors += 1
    if errors == 0:
        logger.info(f"Schema check passed for view '{view}'")
    return errors

def validate_nulls(conn: sqlite3.Connection) -> int:
    """Ensure no nulls in critical fields of view_order_analysis."""
    critical = ['customer_id', 'City', 'Country', 'region_name']
    errors = 0
    for col in critical:
        df = pd.read_sql(
            f"SELECT COUNT(*) AS cnt FROM view_order_analysis WHERE {col} IS NULL;",
            conn
        )
        cnt = int(df['cnt'].iloc[0])
        if cnt > 0:
            logger.error(f"Null check failed: {cnt} missing values in '{col}'")
            errors += 1
    if errors == 0:
        logger.info("Null check passed: no missing values in critical fields.")
    return errors


def validate_duplicates(conn: sqlite3.Connection) -> int:
    """Ensure no duplicate order_id in view_order_analysis."""
    df = pd.read_sql(
        "SELECT order_id, COUNT(*) AS cnt FROM view_order_analysis GROUP BY order_id HAVING cnt > 1;",
        conn
    )
    dup_count = len(df)
    if dup_count > 0:
        logger.error(f"Duplicate check failed: {dup_count} duplicate order_id(s) found.")
    else:
        logger.info("Duplicate check passed: no duplicate order_id.")
    return dup_count


def validate_region_coverage(conn: sqlite3.Connection) -> int:
    """Ensure all customer countries are present in dim_region."""
    df_c = pd.read_sql("SELECT DISTINCT Country FROM dim_customer;", conn)
    df_r = pd.read_sql("SELECT DISTINCT country FROM dim_region;", conn)
    missing = set(df_c['Country']) - set(df_r['country'])
    if missing:
        for country in missing:
            logger.error(f"Region coverage check failed: no mapping for country '{country}'")
    else:
        logger.info("Region coverage check passed: all customer countries mapped.")
    return len(missing)


def validate_weather_city_match(conn: sqlite3.Connection) -> int:
    """Ensure all weather cities exist in dim_customer; log info if customer city missing weather."""
    df_c = pd.read_sql("SELECT DISTINCT City FROM dim_customer;", conn)
    df_w = pd.read_sql("SELECT DISTINCT city FROM dim_weather;", conn)
    customer_cities = set(df_c['City'])
    weather_cities = set(df_w['city'])

    # Error if weather city not in customer cities
    extra_weather_cities = weather_cities - customer_cities
    for city in extra_weather_cities:
        logger.error(f"Weather match check failed: weather data for city '{city}' not present in customers.")

    # Info if customer city missing weather
    missing_weather_cities = customer_cities - weather_cities
    for city in missing_weather_cities:
        logger.info(f"Weather match info: no weather data for city '{city}' (missing in API response)")

    if not extra_weather_cities:
        logger.info("Weather match check passed: all weather cities mapped to customers.")

    return len(extra_weather_cities)


def main(db_path: str):
    conn = connect(db_path)
    failures = 0
    # view schema
    expected_view_cols = ['order_id', 'order_date', 'freight', 'customer_id', 'company_name', 'city', 'country', 'region_name',
                          'temperature_C', 'feels_like_C', 'humidity_pct', 'pressure_hPa', 'wind_speed_mps', 'cloud_coverage_pct',
                          'weather_main', 'weather_description']
    failures += validate_view_schema(conn, 'view_order_analysis', expected_view_cols)
    # data validations
    failures += validate_nulls(conn)
    failures += validate_duplicates(conn)
    failures += validate_region_coverage(conn)
    failures += validate_weather_city_match(conn)
    conn.close()
    if failures > 0:
        logger.error(f"Validation failed with {failures} issue(s).")
        sys.exit(1)
    else:
        logger.info("All validations passed.")

if __name__ == '__main__':
    if len(sys.argv) != 2:
        logger.error("Usage: python validate.py <warehouse_db>")
        sys.exit(1)
    main(sys.argv[1])
