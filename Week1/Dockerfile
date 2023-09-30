FROM python:3.10.6

RUN apt-get install wget
RUN pip install pandas pyarrow sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py
RUN mkdir data

ENTRYPOINT [ "python", "ingest_data.py" ]
