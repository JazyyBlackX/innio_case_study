
import os
import subprocess
from pathlib import Path
from prefect import flow, task, get_run_logger

@task(retries=2, retry_delay_seconds=60)
def extract_task(src_db: str, dest_db: str, region_file: str):
    """Run extract.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/extract.py', src_db, dest_db, region_file]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

@task(retries=2, retry_delay_seconds=60)
def api_task(dest_db: str, api_key: str):
    """Run weather_api.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/weather_api.py', dest_db, api_key]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

@task(retries=2, retry_delay_seconds=60)
def transform_task(db_path: str, region_file: str):
    """Run transform.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/transform.py', db_path, region_file]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

@task(retries=2)
def load_task(db_path: str):
    """Run load.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/load.py', db_path]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

@task(retries=1)
def validate_task(db_path: str):
    """Run validate.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/validate.py', db_path]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)

@task(retries=1)
def analysis_task(db_path: str):
    """Run analysis.py"""
    logger = get_run_logger()
    cmd = ['python', 'scripts/analysis.py', db_path]
    logger.info(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)



@flow(name="ETL_Workflow")
def etl_flow(
    source_db: str = '/app/data/northwind.db',
    warehouse_db: str = '/app/data/warehouse.db',
    region_file: str = '/app/data/region_mapping.xlsx',
    api_key: str = '8de9abed422e492a58cf716b0e24caf0'
):
    """Orchestrate tasks."""
    extract_task(source_db, warehouse_db, region_file)
    api_task(warehouse_db, api_key)
    transform_task(warehouse_db, region_file)
    load_task(warehouse_db)
    validate_task(warehouse_db)
    analysis_task(warehouse_db)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Orchestrate the ETL pipeline")
    parser.add_argument('source_db')
    parser.add_argument('warehouse_db')
    parser.add_argument('region_file')
    parser.add_argument('api_key')
    args = parser.parse_args()
    etl_flow(
        source_db=args.source_db,
        warehouse_db=args.warehouse_db,
        region_file=args.region_file,
        api_key=args.api_key
    )

