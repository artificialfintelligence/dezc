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

* Download the data.
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

* Now we can access pgAdmin from a browser by navigating to [localhost:8080](http://localhost:8080). But pgAdmin will not be able to "see" the other container running PostgreSQL unless we defaine a *network*.
``` bash
docker network create pg-network
```

* Of course we need to modify our `docker run` commands to specify the network name as well.
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

## Poor Man's Ingestion Script

* Convert our Jupyter Notebook code to a python script (and then manually clean it up).
``` bash
jupyter nbconvert --to=script --output=ingest_data Notes1_2.ipynb
```

> _Note:_ I actually opted to change the ingestion method substantially just to practice ingesting Parquet data in batches, which I had only mentioned in passing as a comment in the notebook.

* We can now `DROP TABLE` the old data in pgAdmin and try out our new ingestion script.
``` bash
URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet" \
&& \
python ingest_data.py \
  --url=${URL} \
  --host=localhost \
  --port=5432 \
  --username=root \
  --password=root \
  --db=ny_taxi \
  --table_name=yellow_taxi_data
```

> _Note:_ In practice, we would never pass the password in such an unsecure fashion. (It will appear in the shelll command history which can be accessed with the `history` command!!) We would use environment variables or even more secure password store methods.

> _Tip:_ In the Linux shell, `echo $?` shows the exit code of the last run command. `0` means it finished running successfully.

* And now we Dockerize our ingestion script. Check out the [Dockerfile](Dockerfile). Let's build the image.
``` bash
docker build -t ny_taxi_ingest:v001 .
```
* Now we instantiate our Docker container and pass it the same parameters we passed to `python ingest_data.py` earlier (except for `host` which is not `localhost` anymore).
``` bash
docker run -it \
  --network=pg-network \
  ny_taxi_ingest:v001 \
    --url=${URL} \
    --host=pg-database \
    --port=5432 \
    --username=root \
    --password=root \
    --db=ny_taxi \
    --table_name=yellow_taxi_data
```

> _Note:_ This is important! Note how we have passed `docker` parameters to it *before the image name* and then the rest of the parameters (which are parameters for the entry point `python ingest_data.py`) *after the image name*.

### A Tangent

* Let's say we didn't want to keep downloading the file from the web every time, or the data we want to ingest is already downloaded to a file on our local machine. How do we use our script in this scenario? Well, first we would need to start an HTTP Server on our local machine!
``` bash
python -m http.server
```

* This web server is now accessible on [localhost:8000](http://localhost:8000) from a browser and it simply shows the contents of our current directory. But there is **a problem**: To our running container, `localhost` means something different. (It means itself, not the host as we intend). So instead of `localhost` we need to access the host using its IP address, which we can get using the `ifconfig` command in the Linux shell. This command will output a *lot* of information about *all* of our network interfaces. We'd have to sift through it and look for lines containing `inet` (but ignore the loopback address `127.0.0.1`). Alternatively, use the following command to filter out the results:
``` bash
ifconfig | grep "inet " | grep -v 127.0.0.1
```
or, use `ipconfig`, a more modern alternative to `ifconfig` available on newer versions of MacOS. Below, `en0` is the default network interface for Wi-Fi connections on MacOS.
``` bash
ipconfig getifaddr en0
```

* Now all  we have to do is change the URL variable and run our Docker container as before.
``` bash
URL="http://192.168.1.75:8000/data/downloaded_data.parquet"
```

> _Note:_ Of course this is very much a toy example. In practice, we would not be doing this whole `docker` with `network` thing. This is all just a proof of concept for local testing. In 'real world' scenarios, `url` would often refer to a database running in the cloud (e.g. BigQuery) and something like a Kubernetes job would replace typing in `docker` commands. We won't be using Kubernetes in this course, but we *are* going to see how to use Prefect and/or Apache Airflow for this later.
