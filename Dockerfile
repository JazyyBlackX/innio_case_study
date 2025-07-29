FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy scripts and data to the image
COPY scripts/ ./scripts/
COPY data/ ./data/

# Set the working directory to scripts
WORKDIR /app/scripts

CMD ["python", "orchestration.py", "../data/northwind.db", "../data/warehouse.db", "../data/region_mapping.xlsx", "8de9abed422e492a58cf716b0e24caf0"]
