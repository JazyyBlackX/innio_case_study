\# INNIO Data Engineering Case Study



\## Overview



This project delivers a complete, container-ready ETL workflow that:



1\. Extracts orders \& customers from the Northwind sample database.

2\. Enriches the data with regional groupings (via region\\\_mapping.xlsx) and real-time weather data from OpenWeather.

3\. Transforms \& loads the result into an analytics-ready SQLite warehouse.

4\. Validates tables and the final view with Pandera for data quality.

5\. Runs end-to-end using Prefect orchestration and Docker for one-command reproducibility.



\## Quick Start



Prerequisite: Docker Desktop



Build and run the ETL pipeline:



```

docker build -t innio-etl .

docker run --rm innio-etl

```



Logs and validations print directly to your terminal.



Optional: custom data paths or API key:



```

docker run --rm innio-etl \\

&nbsp; "../data/northwind.db" \\

&nbsp; "../data/warehouse.db" \\

&nbsp; "../data/region\_mapping.xlsx" \\

&nbsp; "<YOUR\_OPENWEATHER\_API\_KEY>"

```



\## Project Structure



```

innio\_case\_study/

│

├── data/

│   ├── northwind.db

│   └── region\_mapping.xlsx

│

├── scripts/

│   ├── extract.py

│   ├── transform.py

│   ├── load.py

│   ├── weather\_api.py

│   ├── validate.py

│   ├── analysis.py

│   └── orchestrate\_etl.py

│

├── requirements.txt

├── Dockerfile

└── README.md

```



\## Orchestration \& Monitoring



\* Orchestrator: Prefect 2.x (orchestrate\\\_etl.py)

\* Task order: extract → transform → weather → load → validate

\* Logging and retries handled by Prefect

\* Optional UI:

&nbsp; prefect orion start

&nbsp; open \[http://127.0.0.1:4200](http://127.0.0.1:4200)



\## Tool Choices \& Rationale



\* pandas + sqlite3: simple, zero-config SQL→DataFrame

\* requests: robust HTTP client for OpenWeather

\* Pandera: in-script DataFrame validation

\* Prefect: lightweight, Pythonic workflow management

\* Docker: full portability, reviewer only needs Docker



\## Data Quality Checks



\* Schema validation via Pandera

\* Null \& duplicate checks on key columns

\* Referential integrity: city→weather, country+region→dim\\\_region

\* Immediate exit on any validation failure



\## Challenges \& Solutions



\* API version mismatch: switched to OpenWeather 2.5 and URL-encoded cities

\* dtype drift: aligned Pandera schemas to pandas float64 for REAL columns

\* Docker build issues: removed sqlite3 from requirements.txt

\* Path portability: scripts accept CLI args; Docker defaults to /app/data



\## Example Output



```

\[PASS] Schema validation for view\_order\_analysis

No nulls in key fields

No duplicate order\_id entries

Referential integrity passed ✅

All validations passed successfully!

```



\## Running Locally (without Docker)



```

python -m venv venv

source venv/bin/activate   # or .\\venv\\Scripts\\activate on Windows

pip install -r requirements.txt

python scripts/orchestrate\_etl.py

```



\## Author \& Contact



Avnish Sharma

\[avnish.sharma@example.com](mailto:avnish.sharma@example.com)

\# INNIO Data Engineering Case Study



\## Overview



This project delivers a complete, container-ready ETL workflow that:



1\. Extracts orders \& customers from the Northwind sample database.

2\. Enriches the data with regional groupings (via region\\\_mapping.xlsx) and real-time weather data from OpenWeather.

3\. Transforms \& loads the result into an analytics-ready SQLite warehouse.

4\. Validates tables and the final view with Pandera for data quality.

5\. Runs end-to-end using Prefect orchestration and Docker for one-command reproducibility.



\## Quick Start



Prerequisite: Docker Desktop



Build and run the ETL pipeline:



```

docker build -t innio-etl .

docker run --rm innio-etl

```



Logs and validations print directly to your terminal.



Optional: custom data paths or API key:



```

docker run --rm innio-etl \\

&nbsp; "../data/northwind.db" \\

&nbsp; "../data/warehouse.db" \\

&nbsp; "../data/region\_mapping.xlsx" \\

&nbsp; "<YOUR\_OPENWEATHER\_API\_KEY>"

```



\## Project Structure



```

innio\_case\_study/

│

├── data/

│   ├── northwind.db

│   └── region\_mapping.xlsx

│

├── scripts/

│   ├── extract.py

│   ├── transform.py

│   ├── load.py

│   ├── weather\_api.py

│   ├── validate.py

│   ├── analysis.py

│   └── orchestrate\_etl.py

│

├── requirements.txt

├── Dockerfile

└── README.md

```



\## Orchestration \& Monitoring



\* Orchestrator: Prefect 2.x (orchestrate\\\_etl.py)

\* Task order: extract → transform → weather → load → validate

\* Logging and retries handled by Prefect

\* Optional UI:

&nbsp; prefect orion start

&nbsp; open \[http://127.0.0.1:4200](http://127.0.0.1:4200)



\## Tool Choices \& Rationale



\* pandas + sqlite3: simple, zero-config SQL→DataFrame

\* requests: robust HTTP client for OpenWeather

\* Pandera: in-script DataFrame validation

\* Prefect: lightweight, Pythonic workflow management

\* Docker: full portability, reviewer only needs Docker



\## Data Quality Checks



\* Schema validation via Pandera

\* Null \& duplicate checks on key columns

\* Referential integrity: city→weather, country+region→dim\\\_region

\* Immediate exit on any validation failure



\## Challenges \& Solutions



\* API version mismatch: switched to OpenWeather 2.5 and URL-encoded cities

\* dtype drift: aligned Pandera schemas to pandas float64 for REAL columns

\* Docker build issues: removed sqlite3 from requirements.txt

\* Path portability: scripts accept CLI args; Docker defaults to /app/data



\## Example Output



```

\[PASS] Schema validation for view\_order\_analysis

No nulls in key fields

No duplicate order\_id entries

Referential integrity passed ✅

All validations passed successfully!

```



\## Running Locally (without Docker)



```

python -m venv venv

source venv/bin/activate   # or .\\venv\\Scripts\\activate on Windows

pip install -r requirements.txt

python scripts/orchestrate\_etl.py

```



\## Author \& Contact



Avnish Sharma

\[avnish.sharma@example.com](mailto:avnish.sharma@example.com)



