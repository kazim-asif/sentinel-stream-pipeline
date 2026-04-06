FROM apache/airflow:3.1.8

USER root
# Install system-level dependencies
RUN apt-get update && apt-get install -y gcc python3-dev ca-certificates

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip certifi
RUN pip install --no-cache-dir -r requirements.txt