#!/usr/bin/env python
# coding: utf-8

import argparse
import os
from datetime import timedelta
from pathlib import Path
from time import time

import pandas as pd
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine

from prefect import flow, task

DEFAULT_DATA_DIR = Path(Path(__file__).parent.parent.parent, "data")
DATA_DIR = os.environ.get("DATA_DIR", default=DEFAULT_DATA_DIR)


@task(log_prints=True, retries=2, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_and_read_data(url):
    # The backup files are gzipped and it's important to keep the correct extension
    # for Pandas to be able to open the file
    if url.endswith(".csv.gz"):
        csv_name = "output.csv.gz"
    else:
        csv_name = "output.csv"

    csv_path = Path(DATA_DIR, csv_name)
    os.system(f"wget {url} -O {csv_path}")

    print("Connection to Postgres database established.")

    df = pd.read_csv(csv_path)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    return df


@task(log_prints=True)
def remove_no_passenger_rides(df):
    print(
        f"Pre-cleaning count of records with zero passengers: {(df['passenger_count'] == 0).sum()}"
    )
    df_clean = df[df["passenger_count"] != 0]
    print(
        f"Post-cleaning count of records with zero passengers: {(df_clean['passenger_count'] == 0).sum()}"
    )
    return df_clean


@task(log_prints=True, retries=2)
def ingest_data(df, user, password, host, port, db, table_name):
    postgres_url = f"postgresql://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(postgres_url)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    print("Created schema. Commencing data loading into Postgres. This'll take a while...")

    t_start = time()
    df.to_sql(name=table_name, con=engine, if_exists="append")
    t_end = time()

    print("Finishied inserting all data. Took %.3f seconds." % (t_end - t_start))


@flow(name="Ingest Flow")
def main_flow():
    user = "root"
    password = "root"
    host = "localhost"
    port = "5432"
    db = "ny_taxi"
    table_name = "yellow_taxi_trips"
    csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

    raw_data = download_and_read_data(csv_url)
    data = remove_no_passenger_rides(raw_data)
    ingest_data(data, user, password, host, port, db, table_name)


if __name__ == "__main__":
    main_flow()
