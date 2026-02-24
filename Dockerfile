FROM apache/airflow:2.8.1-python3.10

USER root
# Install any necessary OS-level packages
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow
# Install the required Python packages for the mini data platform
RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    minio==7.2.3 \
    pandas==2.1.4 \
    psycopg2-binary==2.9.9
