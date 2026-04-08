# Gunakan versi stabil terbaru
ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.8
FROM ${AIRFLOW_IMAGE_NAME}

USER airflow

# Copy requirements.txt
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt