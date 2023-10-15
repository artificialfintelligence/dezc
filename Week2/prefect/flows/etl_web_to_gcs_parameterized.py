import os
from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect_gcp.cloud_storage import GcsBucket

from prefect import flow, task
from prefect.tasks import task_input_hash

DEFAULT_DATA_DIR = Path(Path(__file__).parent.parent.parent, "data")
DATA_DIR = os.environ.get("DATA_DIR", default=DEFAULT_DATA_DIR)


@task(
    retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1)
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into Pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix DType issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(3))
    print(f"Columns: {df.dtypes}")
    print(f"Number of rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_filename: str) -> Path:
    """Write DataFrame out to a Parquet file and return the path to it"""
    path = Path(DATA_DIR, color, dataset_filename + ".parquet")
    # Create the sub-directory referenced by `color` if it doesn't exist
    try:
        os.mkdir(path.parent)
    except FileExistsError:
        print(f"Directory '{path.parent}' already exists and will be re-used.")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_to_gcs(in_file_path: Path) -> None:
    """Upload local Parquet file to GCS"""
    color = in_file_path.parent.name
    filename = in_file_path.name
    gcs_bucket_block = GcsBucket.load("dezc-gcs-bucket-block")
    gcs_bucket_block.upload_from_path(
        from_path=in_file_path, to_path=f"data/{color}/{filename}"
    )


@flow()
def etl_web_to_gcs(color: str, year: int, month: int) -> None:
    """The main ETL function"""
    dataset_filename = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_filename}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    parquet_file_path = write_local(df_clean, color, dataset_filename)
    write_to_gcs(parquet_file_path)


@flow(log_prints=True)
def etl_web_to_gcs_super(color: str, year: int, months: list[int]):
    for month in months:
        etl_web_to_gcs(color, year, month)


if __name__ == "__main__":
    color = "yellow"
    months = [1, 2, 3]
    year = 2021
    etl_web_to_gcs_super(color, year, months)
