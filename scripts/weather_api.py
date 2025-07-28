import sys
import sqlite3
import pandas as pd
import requests
import logging
from datetime import datetime

# Basic logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

API_URL = "https://api.openweathermap.org/data/2.5/weather"

def connect(db_path):
    logger.info(f"Connecting to database at {db_path}")
    return sqlite3.connect(db_path)

def get_cities(conn):
    df = pd.read_sql("SELECT DISTINCT City FROM dim_customer", conn)
    cities = df['City'].tolist()
    logger.info(f"Found {len(cities)} unique cities")
    return cities

def fetch_weather_for_city(city, api_key):
    params = {"q": city, "appid": api_key, "units": "metric"}
    r = requests.get(API_URL, params=params, timeout=5)
    r.raise_for_status()
    d = r.json()
    # Safe extraction of nested fields
    return {
        'city': city,
        'api_city_name': d.get('name'),
        'country': d.get('sys', {}).get('country'),
        'lat': d.get('coord', {}).get('lat'),
        'lon': d.get('coord', {}).get('lon'),
        'temperature_C': d.get('main', {}).get('temp'),
        'feels_like_C': d.get('main', {}).get('feels_like'),
        'humidity_%': d.get('main', {}).get('humidity'),
        'pressure_hPa': d.get('main', {}).get('pressure'),
        'wind_speed_mps': d.get('wind', {}).get('speed'),
        'cloud_coverage_%': d.get('clouds', {}).get('all'),
        'weather_main': (d.get('weather') or [{}])[0].get('main'),
        'weather_description': (d.get('weather') or [{}])[0].get('description'),
        'weather_timestamp': datetime.utcfromtimestamp(d.get('dt', 0))
    }

def main():
    if len(sys.argv) != 3:
        logger.error("Usage: python weather.py <warehouse_db> <openweather_api_key>")
        sys.exit(1)

    db_path, api_key = sys.argv[1], sys.argv[2]
    conn = connect(db_path)
    records = []

    for city in get_cities(conn):
        try:
            rec = fetch_weather_for_city(city, api_key)
            records.append(rec)
            logger.info(f"Fetched weather for {city}: {rec['temperature_C']}°C, {rec['weather_description']}")
        except Exception as e:
            logger.warning(f"Failed for {city!r}: {e}")

    weather_df = pd.DataFrame(records)
    weather_df.to_sql("raw_weather", conn, if_exists="replace", index=False)
    logger.info(f"Loaded raw_weather with {len(weather_df)} rows")
    conn.close()

if __name__ == "__main__":
    main()
