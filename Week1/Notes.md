# Notes for Week 1

## Setting up PostgreSQL in a Docker Container

Run the Docker container. If we don't have the image already, it'll download it from Docker Hub the first time we run this.
``` bash
docker run -it \
  -e POSTGRES_USER="root" \
  -e POSTGRES_PASSWORD="root" \
  -e POSTGRES_DB="ny_taxi" \
  -v $(pwd)/data/ny_taxi_postgres_data:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:13
```

Install `pgcli` with `pip` and then:
``` bash
pgcli -h localhost -p 5432 -u root -d ny_taxi
```

Download the data.
``` bash
wget -P ./data/ https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2021-01.parquet
```

If this was a CSV file we could have taken a peek using CLI commands like:
``` bash
head -n 100 ./data/yellow_tripdata_2021-01.csv
```
and gotten a count of the number of lines with:
``` bash
wc -l ./data/yellow_tripdata_2021-01.csv
```

> Next, check out [Notes1_2.ipynb](Notes1_2.ipynb) where we read the data using Pandas, get the DDL from the DataFrame and write the data straight to our database from there.

## Using pgAdmin to Interface with PostgreSQL

pgAdmin is a web-based GUI that we will install in a container. It's much more convenient than using `pgcli`.
``` bash
docker run -it \
  -e PGADMIN_DEFAULT_EMAIL="admin@domain.com" \
  -e PGADMIN_DEFAULT_PASSWORD="root" \
  -p 8080:80 \
  dpage/pgadmin4
```

Now we can access pgAdmin from a browser by navigating to [localhost:8080](http://localhost:8080). But pgAdmin will not be able to "see" the other container running PostgreSQL unless we defaine a *network*.
``` bash
docker network create pg-network
```

Of course we need to modify our `docker run` commands to specify the network name as well.
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

Now we can go back to pgAdmin and register a server with information pertaining to our PostgreSQL server. As per the note above, when specifying the "Host name/address" under the "Connection" tab, we must use the name defined for our PostgreSQL container (here: `pg-database`).

## Poor Man's Ingestion Script

Convert our Jupyter Notebook code to a python script (and then manually clean it up).
``` bash
jupyter nbconvert --to=script --output=ingest_data Notes1_2.ipynb
```

> _Note:_ I actually opted to change the ingestion method substantially just to practice ingesting Parquet data in batches, which I had only mentioned in passing as a comment in the notebook.

We can now `DROP TABLE` the old data in pgAdmin and try out our new ingestion script.
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

And now we Docker-ize our ingestion script. Check out the [Dockerfile](Dockerfile). Let's build the image.
``` bash
docker build -t ny_taxi_ingest:v001 .
```

Now we instantiate our Docker container and pass it the same parameters we passed to `python ingest_data.py` earlier (except for `host` which is not `localhost` anymore).
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

> Note how we have passed `docker`'s parameters to it *before* the image name and then the rest of the parameters (which are for the entry point `python ingest_data.py`) *after* the image name.

### A Tangent

Let's say we didn't want to keep downloading the file from the web every time, or the data we want to ingest is already downloaded to a file on our local machine. How do we use our script in this scenario? Well, first we would need to start an HTTP Server on our local machine.
``` bash
python -m http.server
```

This web server is now accessible on [localhost:8000](http://localhost:8000) from a browser and it simply shows the contents of our current directory. But there is **a problem**: To our running container, `localhost` means something different. (It means itself, not the host as we intend). So instead of `localhost` we need to access the host using its IP address, which we can get using the `ifconfig` command in the Linux shell. This command will output a *lot* of information about *all* of our network interfaces. We'd have to sift through it and look for lines containing `inet` (but ignore the loopback address `127.0.0.1`). Alternatively, use the following command to filter out the results:
``` bash
ifconfig | grep "inet " | grep -v 127.0.0.1
```
or, use `ipconfig`, a more modern alternative to `ifconfig` available on newer versions of MacOS. Below, `en0` is the default network interface for Wi-Fi connections on MacOS.
``` bash
ipconfig getifaddr en0
```

Now all  we have to do is change the URL variable and run our Docker container as before.
``` bash
URL="http://192.168.1.75:8000/data/downloaded_data.parquet"
```

> _Note:_ Of course this is very much a toy example. In practice, we would not be doing this whole `docker` with `network` thing. This is all just a proof of concept for local testing. In 'real world' scenarios, `url` would often refer to a database running in the cloud (e.g. BigQuery) and something like a Kubernetes job would replace typing in `docker` commands. We won't be using Kubernetes in this course, but we *are* going to see how to use Prefect and/or Apache Airflow for this later.

## Orchestrating Containers with Docker-Compose

Check out [docker-compose.yaml](docker-compose.yaml) which will make everything much easier going forward. We won't need to specify and run our Docker containers manually and individually or define a network separately anymore. It is basically a way to run multiple related "services" with a single config file.

Run `docker-compose` in "detached mode" with `-d` so that you can continue to interact with the shell without having to open another shell window. If there are multiple configuration files and you'd like to specify which YAML file to use, use the `-f` flag.
``` bash
docker-compose up -d
```

Shut both services down at once gracefully with:
``` bash
docker-compose down
```

## Introduction to Terraform
[Terraform](https://developer.hashicorp.com/terraform) is an open-source tool by HashiCorp. It is an IaC (Infsrustructure-as-Code) solution which means it lets you provision infrastructure resources (VMs, containers, storage, networking, etc.) with declarative configuration files. This enables you to use version control software to reuse and share configuration and manage your infrastructure in a safe, consistent and repeatable manner, bringing DevOps best practices for change management to your infrastructure. It's great for stack-based deployments and works really well with cloud providers. It also uses a state-based approach to track resource changes throughout deployments.

You specify which version of Terraform to use for `terraform` commands with the `.terraform-version` file. An alternative would be to use the [tfenv](https://github.com/tfutils/tfenv) plugin.

The main required files are `main.tf` and `variables.tf`. The files `resources.tf` and `output.tf` are optional.

### Declarations
In `main.tf`:
- `terraform`: Basic Terraform settings
  - `required_version`: Minimum compatible Terraform version
  - `backend`: Where to persist Terraform's "state" snapshots which map real-world resources to your configuration; see the [docs](https://developer.hashicorp.com/terraform/language/settings/backends/configuration).
  - `required_providers`: The [providers](https://registry.terraform.io/browse/providers) required by this module. (Think of this as analogous to an `import` statement in Python.) The [Terraform Registry](https://registry.terraform.io/) is the main  directory of publicly available providers from most major infrastructure platforms.
- `provider`: [Configures](https://developer.hashicorp.com/terraform/language/providers/configuration) a provider previously declared as required.
- `resource`: Each [Resource block](https://developer.hashicorp.com/terraform/language/resources/syntax) defines a component of your infrastructure. The specifics depend on the provider and they have fully documented the syntax for each resource (e.g. `google_storage_bucket`, `google_bigquery_dataset`, `google_bigquery_table`, etc.) on Terraform Registry, and of course you can go to each provider's website (e.g. [Google Cloud Storage documentation](https://cloud.google.com/storage/docs)) and look through their SDK and API docs to understand the links between that and Terraform's syntax more deeply.

In `variables.tf`:
- `locals`: Analogous to "constants" in programming
- `variable`: "Runtime" variables

### Execution

Initialize the state file (a `.tfstate` file), initialize and configure the backend, install plugins/providers, checkout existing config from version control. These all go in the `.terraform` directory, which this command also creates.
``` bash
terraform init
```

Check changes to new infrastructure plan, match and preview local changes against remote state, and propose an Execution Plan. (Essentially a "dry run")
``` bash
terraform plan -var="project=<your-gcp-project-id>"
```

Create new infrastructure. Asks for approval to the proposed plan, and applies changes in the cloud.
``` bash
terraform apply -var="project=<your-gcp-project-id>"
```

Delete infrastructure after your work is done to avoid costs on any running services. Removes your stack from the cloud.
``` bash
terraform destroy
```
> Check out this [tutorial on how to get started with Terraform on Google Cloud](https://developer.hashicorp.com/terraform/tutorials/gcp-get-started) for more details and to learn more in depth.

## Setting up the Environment on a Cloud Virtual Machine
After setting up a new GCP account to take advantage of the "US$300 credits for 3 months" free trial and creating a new project and setting up the necessary Service Accounts under `IAM & Admin`, we:
- Generated SSH public and private keys using:
  ``` bash
  ssh-keygen -t rsa -f ~/.ssh/KEY_FILENAME -C USERNAME -b 2048
  ```
  Replacing `KEY_FILENAME` and `USERNAME` with our desired values. We will be asked for an optional passphrase for logging in, which we may leave blank. The keys are locally stored under `~/.ssh/`. Our private key is for our eyes only and we should never store it anywhere publicly. We then add our public key (the contents of the key file with the `.pub` extension) to `Compute Engine` > Settings > Metadata > SSH Keys.
- Created a new VM instance under `Compute Engine` (after enabling the API, of course, which in turn necessitated setting up a billing account first).
- Copied the external IP address of our newly-created VM and logged in to it with SSH using:
  ``` bash
  ssh -i ~/.ssh/KEY_FILENAME USERNAME@VM_EXTERNAL_IP_ADDRESS
  ```
- We configured SSH by modifying the contents of `~/.ssh/config` so that going forward we can simply SSH into our vm with the command:
  ``` bash
  ssh de-zoomcamp
  ```
  My updated `config` file is in my `dotfiles` repo on GitHub. I will just have to update the IP Address when it changes, or the name of the identity (key) file if I rename it.
- The VM comes with the Google Cloud SDK pre-installed, which is nice. Next, we downloaded the appropriate version of [Anaconda](https://www.anaconda.com/download#downloads) with `wget` and installed it with the `bash` command. To reload `.bashrc` and have the installation take effect, simply `logout` (or Ctrl+D) and log back in, or alternatively run:
  ``` bash
  source ~/.bashrc
  ```
- We installed Docker. First we get the latest directory of packages by updating `apt-get`:
  ``` bash
  sudo apt-get update
  ```
  Then:
  ``` bash
  sudo apt-get install docker.io
  ```
- We followed [these steps](https://github.com/sindresorhus/guides/blob/main/docker-without-sudo.md) to become able to run Docker without `sudo`:
  ``` bash
  sudo groupadd docker \
  && \
  sudo gpasswd -a $USER docker
  ```
  Log out and back in so that group memberships are re-evaluated. Then:
  ``` bash
  sudo service docker restart
  ```
- We installed the "Remote - SSH" plugin (by Microsoft) for VS Code so that we can use VS Code with our VM just as though it was our local machine.
- We cloned our repo in the VM.
- We downloaded the appropriate version of [docker-compose](https://github.com/docker/compose/releases) from its repo with `wget` into the `~/bin/` folder (which we created) on the VM, named it `docker-compose` and then gave it executable privilege with:
  ``` bash
  chmod +x docker-compose
  ```
  We also added the `~/bin/` folder to the `PATH` environment variable by adding the following line to the very end of `~/.bashrc`:
  ``` bash
  export PATH="${HOME}/bin:${PATH}"
  ```
- We installed `pgcli` using `pip`, or if you face any issues (I didn't), `pip uninstall` it and then use `conda` instead:
  ``` bash
  conda install -c conda-forge pgcli \
  && \
  pip install -U mycli
  ```
- We set up port forwarding in VS Code. We performed the following mappings: 5432:5432, 8080:8080 and 8888:8888, which respectively allow us to interact with the running instances of PostgreSQL, pgAdmin and Jupyter Notebooks on the VM from our local machine.
- We downloaded [Terraform](https://developer.hashicorp.com/terraform/downloads) binaries with `wget` into our `~/bin/` directory. But it was a zip archive file so we first had to:
  ``` bash
  sudo apt-get install unzip
  ```
  And then `unzip` the downloaded file which contained the already-executable file `terraform`.
- We uploaded our GCP service account credentials file to the VM using SFTP. First, we `cd` into the folder containing the file and then from within that folder:
  ``` bash
  sftp de-zoomcamp
  ```
  Then, assuming the credentials file is named `dezc-gcp-creds.json`, inside SFTP we do:
  ``` bash
  mkdir .gcp
  ```
  ``` bash
  cd .gcp
  ```
  ``` bash
  put dezc-gcp-creds.json
  ```
  And finally, since we cannot use the same method we used on our Mac (OAuth) to authenticate with GCP on a remote host, instead we go:
  ``` bash
  export GOOGLE_APPLICATION_CREDENTIALS=~/.gcp/dezc-gcp-creds.json \
  && \
  gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS
  ```
Now we are ready to run the four Terraform commands discussed in the previous section to set up our `Cloud Storage` bucket and `BigQuery` dataset (elsewhere known as "schema") in the cloud. These two pieces constitute our data lake and data warehouse, respectively. We can also spin up our Docker containers in our VM and interact with the instance remotely through our browser, shell and VS Code, as we saw earlier.

Always stop the VM instance when not in use to avoid incurring charges. Allegedly, even after we've stopped an instance, we can incur charges for resources such as storage. But deleting it means we'll have to set everything up from scratch next time we need it. There is also a "suspend" option that I haven't had a chance to look into, but it might be a best-of-both-worlds option.
