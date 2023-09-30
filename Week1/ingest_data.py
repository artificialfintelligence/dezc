#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from time import time


def main(params: list[str]) -> None:
    url = params.url
    host = params.host
    port = params.port
    username = params.username
    password = params.password
    db = params.db
    table_name = params.table_name

    pq_filename = "downloaded_data.parquet"

    os.system(f"wget {url} -O ./data/{pq_filename}")

    engine = create_engine(
        f"postgresql://{username}:{password}@{host}:{port}/{db}"
    )

    parquet_file = pq.ParquetFile(f"./data/{pq_filename}")

    batch_size = 100_000
    batch = parquet_file.iter_batches(batch_size=batch_size)
    df = next(batch).to_pandas()
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")

    t_start = time()
    df.to_sql(name=table_name, con=engine, if_exists="append")
    t_end = time()
    print(
        f"Inserted a batch of {len(df)} rows, took {(t_end - t_start):0.3f} seconds..."
    )
    while True:
        try:
            df = next(batch).to_pandas()
            t_start = time()
            df.to_sql(name=table_name, con=engine, if_exists="append")
            t_end = time()

            print(
                f"Inserted another batch of {len(df)} rows, took {(t_end - t_start):.3f} seconds..."
            )
        except StopIteration:
            print("Finished ingesting data into the Postgres database.")
            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Ingest Parquet data to Postgres database"
    )

    parser.add_argument("--url", help="URL of the Parquet file")
    parser.add_argument("--host", help="Host name of Postgres server")
    parser.add_argument("--port", help="Port to connect on to Postgres server")
    parser.add_argument("--username", help="Username for Postgres connection")
    parser.add_argument("--password", help="Password for Postgres connection")
    parser.add_argument("--db", help="Name of database to use")
    parser.add_argument("--table_name", help="Name of table to write to")

    args = parser.parse_args()

    main(args)
