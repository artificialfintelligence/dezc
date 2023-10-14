import os
from pathlib import Path

import pandas as pd
from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

from prefect import flow, task

DEFAULT_DATA_DIR = Path(Path(__file__).parent.parent.parent, "data")
DATA_DIR = os.environ.get("DATA_DIR", default=DEFAULT_DATA_DIR)


@task(retries=2)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trip data from GCS"""
    sub_path = f"{color}/{color}_tripdata_{year}-{month:02}.parquet"
    gcs_path = f"data/{sub_path}"
    gcs_block = GcsBucket.load("dezc-gcs-bucket-block")

    # Want `data` folder in gcs to correspond to `data` folder locally
    destination_dir = Path(DATA_DIR).parent
    gcs_block.get_directory(from_path=gcs_path, local_path=str(destination_dir))
    return Path(DATA_DIR, sub_path)


@task(log_prints=True)
def transform_data(file_path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(file_path)
    print(f"Pre-cleaning: {df['passenger_count'].isna().sum()} records without passenger count")
    df["passenger_count"].fillna(0, inplace=True)
    print(f"Post-cleaning: {df['passenger_count'].isna().sum()} records without passenger count")
    return df


@task
def write_to_bq(df: pd.DataFrame) -> None:
    """Write data from the DataFrame to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("dezc-gcp-creds-block")
    df.to_gbq(
        destination_table="dezc_wk2_prefect.rides",
        project_id="brilliant-vent-400717",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data from GCS to BQ"""
    color = "yellow"
    year = 2021
    month = 1

    dl_file_path = extract_from_gcs(color, year, month)
    df = transform_data(dl_file_path)
    write_to_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
