# Notes for Week 1

## Setting up PostgreSQL in a Docker container

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

* Check out [Notes1_2.ipynb](Notes1_2.ipynb) where we read the data using Pandas, get the DDL from the DataFrame and write the data straight to our database from there (and we do it in chunks because it's a huge amount of data).

## Using pgAdmin to interface with PostgreSQL

* pgAdmin is a web-based GUI that we will install in a container. It's much more convenient than using `pgcli`.
``` bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@domain.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
dpage/pgadmin4
```

* Now we can access pgAdmin from a browser by navigating to [localhost:8080](http://localhost:8080). But pgAdmin will not be able to "see" the other container running PostgreSQL unless we defaine a *network*:
``` bash
docker network create pg-network
```

* Of course we need to modify our `docker run` commands to specify the network name as well:
``` bash
docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="ny_taxi" \
-v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
-p 5432:5432 \
--network=pg-network \
--name pg-database \
postgres:13
```
and

``` bash
docker run -it \
-e PGADMIN_DEFAULT_EMAIL="admin@domain.com" \
-e PGADMIN_DEFAULT_PASSWORD="root" \
-p 8080:80 \
--network=pg-network \
--name pg-admin \
dpage/pgadmin4
```
> _Note:_ In the above, the `name` option is particularly important for the PostgreSQL container because that's how we will point pgAdmin to the database. pgAdmin's container name is not as important (because nothing will be "connecting" to it).

* Now we can go back to pgAdmin and register a server with information pertaining to our PostgreSQL server. As per the note above, when specifying the "Host name/address" under the "Connection" tab, we must use the name defined for our PostgreSQL container (here: `pg-database`).
