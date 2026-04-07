ARG AIRFLOW_IMAGE_NAME=apache/airflow:3.1.3
FROM ${AIRFLOW_IMAGE_NAME}

# Jangan pake USER root kalau cuma install pip packages
USER airflow

# 1. Copy requirements saja dulu (untuk caching layer)
COPY --chown=airflow:0 requirements.txt .

# 2. Install requirements
# Tambahkan --upgrade agar tidak bentrok dengan lib bawaan
RUN pip install --no-cache-dir -U -r requirements.txt

# 3. COPY folder secara spesifik (JANGAN COPY SEMUA '.')
# Ini biar folder /opt/airflow/ bawaan image tidak rusak
COPY --chown=airflow:0 dags/ /opt/airflow/dags/
COPY --chown=airflow:0 plugins/ /opt/airflow/plugins/
COPY --chown=airflow:0 config/ /opt/airflow/config/
# Jika kamu punya folder keys
COPY --chown=airflow:0 keys/ /opt/airflow/keys/

# Set Workdir tetap di sini
WORKDIR /opt/airflow/