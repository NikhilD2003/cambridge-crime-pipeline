"""Run: python src/etl_pipeline.py   (one run; scheduling is done by GitHub Actions, see docs/)"""
from prefect import flow

from extract import extract_data
from load import load_into_postgres
from transform import transform_data
from validate import validate_data


@flow(name="cpd_incident_etl", log_prints=True)
def etl():
    n = load_into_postgres(transform_data(validate_data(extract_data())))
    print(f"ETL complete: {n} incidents upserted")


if __name__ == "__main__":
    etl()
