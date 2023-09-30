# Notes for Week 1

## Setting up Postgresql in a Docker container

* Run the Docker container. If we don't have the image already, it'll download it from Docker Hub the first time we run this.
``` bash
docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
-p 5432:5432 \
postgres:13
```

* Install `pgcli` with `pip` and then:
``` bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

* Download the data:
``` bash
wget -P ./data/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```
* If this was a CSV file we could have taken a peek using CLI commands like:
``` bash
head -n 100 ./data/yellow_tripdata_2021-01.csv
```
and gotten a count of the number of lines with:
``` bash
wc -l ./data/yellow_tripdata_2021-01.csv
```

* Check out [Notes1_2.ipynb]() where we read the data using Pandas, get the DDL from the DataFrame and write the data straight to our database from there (and we do it in chunks because it's a huge amount of data).
