# validate_etl.py

import sys
import logging
import sqlite3

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger("validate_etl")




def business_validate(conn: sqlite3.Connection):
    df = pd.read_sql("SELECT * FROM view_order_analysis", conn, parse_dates=["order_date"])
    logger.info("Loaded view_order_analysis (%d rows)", len(df))

    key_fields = ["city", "region_name","country", "order_id"]
    for col in key_fields:
        null_count = df[col].isna().sum()
        if null_count:
            logger.error("Found %d nulls in %s", null_count, col)
            sys.exit(1)
    logger.info("No nulls in key fields %s", key_fields)

    # 2b) No duplicate orders
    dup_count = df.duplicated(subset=["order_id"]).sum()
    if dup_count:
        logger.error("Found %d duplicate order_id", dup_count)
        sys.exit(1)
    logger.info("No duplicate order_id in view")

    # 2c) Every city has weather
    weather_cities = set(pd.read_sql("SELECT city FROM dim_weather", conn)["city"])
    missing_weather = set(df["city"]) - weather_cities
    if missing_weather:
        logger.info("Cities missing in dim_weather: %s \n Not Available in API Call", missing_weather)
    else: 
        logger.info("All cities in view have matching dim_weather entries")

    # 2d) Every country–region combination exists in dim_region
    # build set of valid (country, region_name)
    rr = pd.read_sql("SELECT country, region_name FROM dim_region", conn)
    valid_regions = set(rr.itertuples(index=False, name=None))
    bad = set(df[["country", "region_name"]].itertuples(index=False, name=None)) - valid_regions
    if bad:
        logger.info("Country/region mismatches: %s", bad)
        sys.exit(1)
    logger.info("All country/region combos valid in dim_region")

    logger.info("Business validations passed for view_order_analysis")


# -------------------
# 3) Main entrypoint
# -------------------

def main():
    if len(sys.argv) != 2:
        print("Usage: python validate_etl.py <path_to_warehouse_db>")
        sys.exit(1)

    db_path = sys.argv[1]
    conn = sqlite3.connect(db_path)

    # Only validating the final view here; if you'd like to validate
    # raw & staging tables they can get their own schemas and calls.
    df_view = pd.read_sql("SELECT * FROM view_order_analysis", conn, parse_dates=["order_date"])
    business_validate(conn)

    conn.close()
    logger.info("All validations passed ✅")


if __name__ == "__main__":
    main()
