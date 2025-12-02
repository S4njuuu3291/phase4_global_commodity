ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3
FROM ${AIRFLOW_IMAGE_NAME}

COPY requirements.txt .

RUN pip install --no-cache-dir --user -r requirements.txt
