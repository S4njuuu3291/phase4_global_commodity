import json
import logging
import yaml
import pendulum
from datetime import datetime
import os

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

from utils import fetch_metal_prices, fetch_currency_rate, fetch_fred_data, fetch_news, transform_metal_prices, transform_currency_rates, transform_fred_data, transform_news

# Get config file path relative to this file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
try:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f) or {}
except Exception as e:
    logging.error(f"Failed to load config.yaml: {e}")
    config = {}

bucket_name = config.get("bucket_name")
if not bucket_name:
    logging.warning("BUCKET_NAME not configured - DAG will run but S3 uploads may fail")

DBT_PROJECT_DIR = "/opt/airflow/commodity_dbt"
DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_DIR} && "

default_args = {
    'owner': 'sanju',
    'retries': 2,
}

@dag(
    dag_id='commodity_pipeline',
    default_args=default_args,
    start_date=datetime(2026, 4, 8),
    schedule='@daily',
    catchup=False,
    tags=['commodity', 'duckdb']
)
def commodity_pipeline():
    @task(
        retries=5, 
        retry_delay=pendulum.duration(minutes=10) # Kasih napas buat servernya
    )
    def ingest_metal():
        return fetch_metal_prices("metal")

    @task(
        retries=5, 
        retry_delay=pendulum.duration(minutes=10) # Kasih napas buat servernya
    )
    def ingest_currency():
        return fetch_currency_rate("currency")

    @task(
        retries=5, 
        retry_delay=pendulum.duration(minutes=10) # Kasih napas buat servernya
    )
    def ingest_fred():
        return fetch_fred_data("fred")

    @task(
        retries=5, 
        retry_delay=pendulum.duration(minutes=10) # Kasih napas buat servernya
    )
    def ingest_news():
        return fetch_news("news_api")
    
    @task
    def transform_metal(metal_path):
        return transform_metal_prices(metal_path)

    @task
    def transform_currency(currency_path):
        return transform_currency_rates(currency_path)
    
    @task
    def transform_fred(fred_path):
        return transform_fred_data(fred_path)
    
    @task
    def transform_news_task(news_path):
        return transform_news(news_path)

    # --- DBT Models & Data Mart Layer ---
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'export DBT_PROFILES_DIR=/opt/airflow/commodity_dbt/.dbt && {DBT_COMMAND_PREFIX}dbt run --select staging.* marts.*'
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'export DBT_PROFILES_DIR=/opt/airflow/commodity_dbt/.dbt && {DBT_COMMAND_PREFIX}dbt test'
    )

    # @task
    # def transform_silver(metal_path, currency_path, fred_path, news_path):
    #     # Di sini DuckDB kamu beraksi menggunakan path S3 yang dikirim dari Bronze
    #     print(f"Processing Silver Layer for: {metal_path}, {currency_path}, ...")
    #     # run_duckdb_logic(metal_path, ...)
    #     return "s3://bucket/silver/..."

    # --- Flow / Dependency ---
    # 1. Jalankan semua ingestion secara paralel
    m_path = ingest_metal()
    c_path = ingest_currency()
    f_path = ingest_fred()
    n_path = ingest_news()

    # 2. Jalankan semua transformasi setelah ingestion selesai (parallel)
    metal_result = transform_metal(m_path)
    currency_result = transform_currency(c_path)
    fred_result = transform_fred(f_path)
    news_result = transform_news_task(n_path)

    # 3. Setelah semua transform selesai, jalankan dbt
    [metal_result, currency_result, fred_result, news_result] >> dbt_run >> dbt_test

# Eksekusi DAG
commodity_pipeline_dag = commodity_pipeline()