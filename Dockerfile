FROM apache/airflow:2.8.0-python3.10

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
RUN pip install --no-cache-dir \
    psycopg2-binary==2.9.9 \
    yfinance==0.2.32 \
    pandas==2.1.3 \
    numpy==1.24.3 \
    google-cloud-bigquery==3.13.0 \
    google-cloud-storage==2.10.0 \
    apache-airflow-providers-google==10.10.0
