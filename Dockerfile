FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts/ ./scripts/
COPY data/ ./data/

# Use /app/data as default data location in all scripts
WORKDIR /app/scripts

# Default: run orchestrate_etl.py using the data folder
CMD ["python", "orchestrate_etl.py", "../data/northwind.db", "../data/warehouse.db", "../data/region_mapping.xlsx", "8de9abed422e492a58cf716b0e24caf0"]
