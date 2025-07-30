# INNIO Data Engineering Case Study

## Overview

This project delivers a complete, container-ready ETL workflow that:
1. **Extracts** orders and customers from the Northwind sample database.  
2. **Enriches** the data with regional groupings (via `region_mapping.xlsx`) and real-time weather data from OpenWeather.  
3. **Transforms & Loads** the result into an analytics-ready SQLite warehouse.  
4. **Validates** tables and the final view with Pandera for data quality.  
5. **Orchestrates** end-to-end execution using Prefect.  
6. **Packages** everything in Docker for one-command reproducibility.

---

## Quick Start 🚀

**Prerequisite:** Docker Desktop

### Build and Run the ETL Pipeline

```bash
# 1. Build the Docker image
docker build -t innio-etl .

# 2. Run the pipeline with bundled sample data
docker run --rm innio-etl
```

- Logs and validation results will stream to your terminal.
- The pipeline exits with code `0` on success or `1` on any validation failure.

### Custom Data Paths or API Key

```bash
docker run --rm innio-etl   "../data/northwind.db"   "../data/warehouse.db"   "../data/region_mapping.xlsx"   "<YOUR_OPENWEATHER_API_KEY>"
```

- If arguments are omitted, the defaults (`../data/...`) are used.

---

## Project Structure

```
innio_case_study/
│
├── data/
│   ├── northwind.db
│   └── region_mapping.xlsx
│
├── scripts/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── weather_api.py
│   ├── analysis.py
│   ├── validate.py
│   └── orchestrate_etl.py
│
├── requirements.txt
├── Dockerfile
└── README.md
```

---

## Orchestration & Monitoring

- **Prefect 2.x** is used for workflow orchestration (`scripts/orchestrate_etl.py`).
- Task dependencies: **extract → transform → weather_api → load → validate**.
- Each task logs its progress and has **one retry** on failure.

---

## Tool Choices & Rationale

| Component            | Tool             | Rationale                                   |
|----------------------|------------------|---------------------------------------------|
| Extraction           | pandas, sqlite3  | Quick, zero-config SQL-to-DataFrame loading |
| API Integration      | requests         | Simple, reliable HTTP calls to OpenWeather  |
| Transformation       | pandas           | Flexible joins and date logic               |
| Data Quality         | Pandera          | In-code DataFrame schema & validation       |
| Orchestration        | Prefect          | Python-native, easy retries & logging       |
| Packaging            | Docker           | Portable, consistent environment            |

---

## Data Quality Checks

- **Schema validation**: Raw, staging, and final tables are validated with Pandera schemas.
- **Null & duplicate checks**: Ensure no missing or repeated key fields.
- **Referential integrity**: Verify city-to-weather and region mappings.
- **Exit on failure**: Pipeline halts (`exit 1`) on any quality violation.

---

## Challenges & Solutions

1. **Windows path escaping**  
   - *Issue:* Hardcoded backslashes in Python string literals caused `unicodeescape` errors.  
   - *Solution:* All file paths are passed as CLI arguments. Where literals are needed, raw strings (e.g. `r"C:\path\to\db"`) or doubled backslashes are used.

2. **Date parsing issues**  
   - *Issue:* Full timestamps like `"2016-07-04 09:23:46"` parsed without format resulted in `NaT` for many rows.  
   - *Solution:* Slice the first 10 characters (`"YYYY-MM-DD"`) and parse with `format="%Y-%m-%d"`, ensuring valid dates for all records.

3. **Region mapping headers mismatch**  
   - *Issue:* Excel columns were named slightly differently than expected.  
   - *Solution:* Added `df.rename(columns={…})` in both `transform.py` and `load.py` to map to standard names (`region_before`, `region_after`) before merging.

4. **Single source of truth for region data**  
   - *Issue:* Reading the Excel file in multiple scripts risked drift if the file changed.  
   - *Solution:* Consolidated region extraction into `extract.py`, writing to `raw_region_mapping` table. Downstream scripts query that table.

5. **Locked database errors in notebooks**  
   - *Issue:* Dropping views/tables in Jupyter led to “database is locked” errors.  
   - *Solution:* Used context managers (`with sqlite3.connect(db) as conn:`) to ensure proper connection closure and restarted kernels when needed.

6. **Duplicated weather records causing row explosion**  
   - *Issue:* Raw weather had multiple records per city; joining on city without dedupe caused duplicate orders.  
   - *Solution:* In `build_dim_weather`, sorted by `weather_timestamp` and kept only the latest record per city before joining.

7. **Incorrect join on customer key**  
   - *Issue:* Joined `stg_order_customer_region.CustomerID` to `dim_customer.customer_key` mistakenly, yielding zero rows.  
   - *Solution:* Corrected join to natural `CustomerID` and generated `customer_key` via `ROW_NUMBER()` in the dimension.

8. **Recreating views in SQLite**  
   - *Issue:* SQLite does not support `CREATE OR REPLACE VIEW`.  
   - *Solution:* Each run drops the view if it exists (`DROP VIEW IF EXISTS view_order_analysis;`) then recreates it.

9. **Weather API experimentation**  
   - *Issue:* Started with OpenWeather Geocoding API (v3.0), hit rate limits, and struggled with accented city names.  
   - *Solution:* Switched to the simpler v2.5 `weather?q=City` endpoint, added safe JSON extraction logic, and handle special characters.

---

## Example Output

```
[PASS] Schema validation for view_order_analysis
INFO  No nulls in key fields ['city','region_name','temperature_C']
INFO  No duplicate order_id entries
INFO  Referential integrity checks passed ✅
All validations passed successfully!
```

Final enriched data is persisted in `data/warehouse.db` with all dimension and fact tables, plus `view_order_analysis`.

---

## Running Locally (without Docker)

```bash
python -m venv venv
# Windows:
venv\Scripts\activate
# macOS/Linux:
source venv/bin/activate

pip install -r requirements.txt
python scripts/orchestrate_etl.py
```

---

## Author & Contact



Avnish Sharma

\[avnish.sharma@example.com](mailto:avnish.sharma@example.com)



