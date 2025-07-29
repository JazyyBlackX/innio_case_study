from prefect import flow, task
import subprocess
import sys

@task(log_prints=True, retries=1)
def extract(src_db, warehouse_db, region_file):
    subprocess.run(
        f'python extract.py "{src_db}" "{warehouse_db}" "{region_file}"',
        shell=True, check=True
    )

@task(log_prints=True, retries=1)
def transform(warehouse_db, region_file):
    subprocess.run(
        f'python transform.py "{warehouse_db}" "{region_file}"',
        shell=True, check=True
    )

@task(log_prints=True, retries=1)
def weather_api(warehouse_db, api_key):
    subprocess.run(
        f'python weather_api.py "{warehouse_db}" {api_key}',
        shell=True, check=True
    )

@task(log_prints=True, retries=1)
def load(warehouse_db):
    subprocess.run(
        f'python load.py "{warehouse_db}"',
        shell=True, check=True
    )

@task(log_prints=True, retries=1)
def analysis(warehouse_db):
    subprocess.run(
        f'python analysis.py "{warehouse_db}"',
        shell=True, check=True
    )

@task(log_prints=True, retries=1)
def validate(warehouse_db):
    subprocess.run(
        f'python validate.py "{warehouse_db}"',
        shell=True, check=True
    )

@flow
def etl_flow(
    src_db="C:\\Users\\user\\innio_case_study\\data\\northwind.db",
    warehouse_db="C:\\Users\\user\\innio_case_study\\data\\warehouse.db",
    region_file="C:\\Users\\user\\innio_case_study\\data\\region_mapping.xlsx",
    api_key="8de9abed422e492a58cf716b0e24caf0"
):
    extract(src_db, warehouse_db, region_file)
    transform(warehouse_db, region_file)
    weather_api(warehouse_db, api_key)
    load(warehouse_db)
    analysis(warehouse_db)
    validate(warehouse_db)

if __name__ == "__main__":
    args = sys.argv[1:]
    src_db = sys.argv[1] if len(args) > 0 else "../data/northwind.db"
    warehouse_db = sys.argv[2] if len(args) > 1 else "../data/warehouse.db"
    region_file = sys.argv[3] if len(args) > 2 else "../data/region_mapping.xlsx"
    api_key = args[3] if len(args) > 3 else "8de9abed422e492a58cf716b0e24caf0"

    etl_flow(src_db, warehouse_db, region_file, api_key)
