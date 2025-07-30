
FROM python:3.11-slim


WORKDIR /app

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts and data
COPY scripts/ ./scripts/
COPY data/    ./data/


# Default command: run the orchestration
# Arguments: source DB, dest DB, region mapping, and API key
CMD ["python", "scripts/orchestration.py", \
     "/app/data/northwind.db", \
     "/app/data/warehouse.db", \
     "/app/data/region_mapping.xlsx", \
     "8de9abed422e492a58cf716b0e24caf0"]
