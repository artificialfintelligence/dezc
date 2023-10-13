from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
import os
import pandas as pd

DEFAULT_DATA_DIR = Path(Path(__file__).parent.parent.parent, "data")
DATA_DIR = os.environ.get("DATA_DIR", default=DEFAULT_DATA_DIR)


@task(retries=2)
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
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "yellow"
    year = 2021
    month = 1
    dataset_filename = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_filename}.csv.gz"

    df = fetch(dataset_url)
    df_clean = clean(df)
    parquet_file_path = write_local(df_clean, color, dataset_filename)
    write_to_gcs(parquet_file_path)


if __name__ == "__main__":
    etl_web_to_gcs()
