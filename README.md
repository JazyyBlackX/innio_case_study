# INNIO Data Engineering Case Study

## Overview

This project demonstrates a complete ETL workflow for the INNIO case study, encompassing:

1. **Extraction** of orders and customers from the Northwind SQLite database and loading of raw region mappings.
2. **API Integration** to fetch real‑time weather data for customer cities via OpenWeatherMap.
3. **Transformation** to join and enrich data (orders, customers, regions, weather) into staging tables.
4. **Loading** of dimension tables (`dim_customer`, `dim_region`, `dim_weather`), a fact table (`fact_order`), and a reporting view (`view_order_analysis`).
5. **Validation** of schema, nulls, duplicates, and referential integrity.
6. **Analysis** of freight costs, order counts, temperature correlations, and weather distributions.
7. **Orchestration** with Prefect for task dependencies, retries, and logging.
8. **Dockerization** for one‑command reproducibility.

This project is built on Windows 11.

---

## Setup

Follow these steps to configure your environment before running the pipeline: (Windows)

1. **Open Terminal/PowerShell/CMD(Admin)**
   
2. Install Git CLI (for cloning the repository; install via package manager or from https://git-scm.com/downloads)

   ```bash
   winget install --id Git.Git -e --source winget
   ```

3. **Clone the repository**
   ```bash
   git clone https://github.com/JazyyBlackX/innio_case_study
   cd innio_case_study
   ```

4. **Install Docker Desktop** for containerized execution.
     https://www.docker.com/products/docker-desktop/
   
5. Optional **Create and activate a Python virtual environment** (if running locally without Docker)

   ```bash
   python -m venv venv
   source venv/bin/activate       # macOS/Linux
   venv\Scripts\activate.bat    # Windows
   ```
6. Optional **Install Python dependencies**

   ```bash
   pip install -r requirements.txt
   ```


---

## Prerequisites

* **Docker Desktop** (for containerized execution)
* **Python 3.8+** (if running locally)
* **SQLite** (bundled with Python)
* **OpenWeatherMap API Key** (free tier)
* **Git CLI (for cloning the repository; install via package manager or from https://git-scm.com/downloads)**

---

## Repository Structure

```
innio_case_study/
├── data/
│   ├── northwind.db               # Sample source DB
│   ├── warehouse.db               # Target DB
│   └── region_mapping.xlsx        # Country → Region mapping
│
├── scripts/
│   ├── extract.py                 # Extract raw tables and region file
│   ├── weather_api.py             # Fetch and store raw weather data
│   ├── transform.py               # Build staging tables
│   ├── load.py                    # Build dimensions, fact, and view
│   ├── validate.py                # Data quality checks
│   ├── analysis.py                # Reporting and summaries
│   └── orchestration.py           # Prefect flow definition
│
├── Dockerfile                     # Container setup
├── requirements.txt               # Python dependencies
└── README.md                      # This document
```

---

## Quick Start 🚀

### Dockerized Run

```bash
# 1. Change Directory to innio_case_study
cd innio_case_study

# 2. Build the Docker image
docker build -t innio-etl .

# 2. Run the full pipeline with default arguments
docker run --rm innio-etl

# 3. Run the full pipeline with customer arguments
docker run --rm \
  -e OPENWEATHER_API_KEY=<YOUR_API_KEY> \
  innio-etl \
  /app/data/northwind.db /app/data/warehouse.db /app/data/region_mapping.xlsx
```

* **Defaults** are embedded: `/app/data/northwind.db`, `/app/data/warehouse.db`, `/app/data/region_mapping.xlsx`, and the `OPENWEATHER_API_KEY` environment variable.
* Pipeline logs and validation results print to `stdout`.
* Exit code `0` on success, `1` on any failure.

### Local Development (Without Docker)

```bash
# 1. Create and activate virtual environment
python -m venv venv
source venv/bin/activate       # macOS/Linux
venv\Scripts\activate.bat      # Windows

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the Prefect flow
python scripts/orchestration.py \
  data/northwind.db data/warehouse.db data/region_mapping.xlsx \
  <YOUR_OPENWEATHER_API_KEY>
```

You can also run each step individually:

```bash
python scripts/extract.py data/northwind.db data/warehouse.db data/region_mapping.xlsx
python scripts/weather_api.py data/warehouse.db <YOUR_API_KEY>
python scripts/transform.py data/warehouse.db data/region_mapping.xlsx
python scripts/load.py data/warehouse.db
python scripts/validate.py data/warehouse.db
python scripts/analysis.py data/warehouse.db
```

---

## Script Details & Usage

* **extract.py**
  Extracts `Customers` and `Orders` into `raw_customers`/`raw_orders` and loads `raw_region_mapping` from Excel.

  ```bash
  python scripts/extract.py <source_db> <dest_db> <region_file>
  ```

* **weather\_api.py**
  Fetches current weather per city in `dim_customer` and writes `raw_weather`.

  ```bash
  python scripts/weather_api.py <warehouse_db> <openweather_api_key>
  ```

* **transform.py**
  Builds `stg_order_customer` and enriches with region logic into `stg_order_customer_region`.

  ```bash
  python scripts/transform.py <warehouse_db> <region_file>
  ```

* **load.py**
  Constructs dimension tables (`dim_customer`, `dim_region`, `dim_weather`), the fact table (`fact_order`), and the reporting view (`view_order_analysis`).

  ```bash
  python scripts/load.py <warehouse_db>
  ```

* **validate.py**
  Runs schema, nulls, duplicates, region coverage, and weather‑city match checks.

  ```bash
  python scripts/validate.py <warehouse_db>
  ```

* **analysis.py**
  Prints freight cost statistics, order counts by region, temperature correlations, and weather distributions.

  ```bash
  python scripts/analysis.py <warehouse_db>
  ```

* **orchestration.py**
  Defines a Prefect flow (`ETL_Workflow`) that runs all tasks in sequence with retries and logging.

  ```bash
  python scripts/orchestration.py <source_db> <warehouse_db> <region_file> <api_key>
  ```

---

## Tooling & Rationale

| Component         | Library/Tool        | Purpose and Rationale                                                    |
| ----------------- | ------------------- | ------------------------------------------------------------------------ |
| SQL Access        | `sqlite3`, `pandas` | Zero‑config, fast in‑memory reads into DataFrames.                       |
| API Calls         | `requests`          | Lightweight HTTP client with timeout and error handling.                 |
| Data Manipulation | `pandas`            | Flexible joins, filters, and date operations.                            |
| Orchestration     | `Prefect`           | Python‑native, built‑in retries, logging, and UI.                        |
| Validation        | Custom scripts      | In-code checks for schema, nulls, duplicates, and referential integrity. |
| Containerization  | `Docker`            | Ensures environment consistency and portability.                         |

---

## Challenges & Solutions

1. **Path Escaping on Windows**

   * *Issue:* Hardcoded backslashes caused Unicode escape errors.
   * *Fix:* All file paths passed as CLI args; raw strings used if necessary.

2. **Date Parsing Consistency**

   * *Issue:* Full timestamp strings (`YYYY‑MM‑DD HH:MM:SS`) sometimes failed to parse.
   * *Fix:* Slice to `YYYY‑MM‑DD` and parse with explicit format before SCD logic.

3. **Region Mapping Header Mismatch**

   * *Issue:* Excel headers didn’t match expected column names.
   * *Fix:* Unified column renaming in both transform and load steps.

4. **Database Locking in Notebooks**

   * *Issue:* Dropping views/tables in Jupyter led to locks.
   * *Fix:* Used `with sqlite3.connect(...)` context managers and restarted kernels.

5. **Duplicate Weather Records**

   * *Issue:* Multiple API calls per city caused row explosion.
   * *Fix:* Deduplicated by city, keeping only the latest timestamp in `dim_weather`.

6. **Incorrect Dimensional Joins**

   * *Issue:* Fact load joined on the wrong key, yielding zero rows.
   * *Fix:* Corrected to natural `CustomerID` join and generated surrogate keys via `ROW_NUMBER()`.

7. **SQLite View Recreation**

   * *Issue:* No `CREATE OR REPLACE VIEW` support.
   * *Fix:* `DROP VIEW IF EXISTS` before `CREATE VIEW` in `load.py`.

8. **API Rate Limits & Encoding**

   * *Issue:* Geocoding endpoint limits and accented city names.
   * *Fix:* Switched to the simpler `weather?q=City` endpoint with safe JSON parsing.

---

## Example Output

```
[PASS] Schema check for view_order_analysis
INFO  No nulls in critical fields
INFO  No duplicate order_id entries
INFO  Referential integrity passed
[PASS] All validations passed successfully
```

Final enriched data and analytics view live in `data/warehouse.db`.

---

## Future Enhancements

This project can be extended with a number of production‑grade improvements as time and resources allow:

* **Prefect Cloud or Self‑Hosted Orion Scheduling**: Formalize deployments using Prefect’s Deployment API, work pools, and agents to run ETL flows on a managed schedule with built‑in monitoring and retries.
* **Great Expectations Integration**: Replace or augment the custom `validate.py` checks with GE expectation suites, checkpoints, and data docs to automatically generate HTML reports and integrate into CI pipelines.
* **Unit & Integration Testing**: Add pytest test suites for each script and end‑to‑end integration tests using a temporary SQLite instance to ensure reproducibility with every change.
* **CI/CD Pipeline**: Automate builds, tests, and deployments through GitHub Actions or GitLab CI, including Docker image builds and linting checks on pull requests.
* **Container Orchestration**: Use Docker Compose or Kubernetes to separate services (e.g., Prefect server, agent, cron container) and enable parallel task execution and scaling.
* **Data Catalog & Lineage**: Integrate with tools like OpenLineage or Apache Atlas to capture dataset metadata, lineage graphs, and enable data discovery.
* **Cloud Data Warehouse**: Extend the target from SQLite to a cloud warehouse (e.g., Snowflake, BigQuery, Redshift) for higher volume scenarios and leverage managed scaling.

---


## Contact

**Author:** Avnish Sharma
**Email:** [avnish1999@hotmail.com](mailto:avnish1999@hotmail.com)
