# ⚡️Spark in the ☁️Cloud
Enabled the Cloud Dataproc API and create a Dataproc cluster on Compute Engine (or GKE). You can choose to have optional components such as Jupyter Notebook, Docker, etc. to work with these tools on the cluster's VM if you wish.
Previously we had to use things like `pyspark.conf.SparkConf` to configure and connect to GCS. But we do not need to configure the cluster anymore, because the Dataproc version is already configured.
Now if you go to Dataproc on GC and select our newly created cluster, you can submit a job to it. But first we need to upload the script to GCS.
``` bash
gsutil cp 10_spark_sql_script.py gs://dezc-data-lake_brilliant-vent-400717/code/10_spark_sql_script.py
```

> **Note:** In practice, we would probably have a separate bucket for code (from the data). But we will reuse the same bucket for simplicity here.

Make sure to select the correct job type (PySpark). We do not have any additional python files (dependecies) and we do not need to specify a Jar file. We do have to specify arguments, however:


* `--input_green=gs://dezc-data-lake_brilliant-vent-400717/pq/green/2020/*/`
* `--input_yellow=gs://dezc-data-lake_brilliant-vent-400717/pq/yellow/2020/*/`
* `--output=gs://dezc-data-lake_brilliant-vent-400717/report-2020`

There are actually many different ways to submit a job to a Dataproc cluster in Google Cloud:
- Through the web UI (AKA the console; what we just did)
- Through the CLI (what we will do next)
- Through REST API
- Through the SDK

To learn more, see
[this official guide](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud).

First, create a Service Account with the right permissions in IAM & Admin (or, for our purposes, just grant the "Dataproc Admin" role to the SA we've been using for everything - which is, again, *not* standard/good practice). Then, issue the CLI command:
``` bash
gcloud dataproc jobs submit pyspark \
    --cluster=dezc-cluster \
    --region=us-west1 \
    gs://dezc-data-lake_brilliant-vent-400717/code/10_spark_sql_script.py \
    -- \
        --input_green=gs://dezc-data-lake_brilliant-vent-400717/pq/green/2021/*/ \
        --input_yellow=gs://dezc-data-lake_brilliant-vent-400717/pq/yellow/2021/*/ \
        --output=gs://dezc-data-lake_brilliant-vent-400717/report-2021
```

So now you can imagine that we could use a bash Operator in Airflow to do exactly what we've done here, changing the paramaters as needed. Technically we could also use `spark-submit' and specify the master, but this way is just a tad simpler.
