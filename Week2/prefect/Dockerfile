FROM prefecthq/prefect:2.11.0-python3.10

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY flows /opt/prefect/flows

WORKDIR /opt/prefect
RUN mkdir data
ENV DATA_DIR=/opt/prefect/data
