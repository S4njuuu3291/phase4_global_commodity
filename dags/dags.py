import json
import logging
import yaml
import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from google.cloud import storage, bigquery
from utils import (
    fetch_currency_rate,
    fetch_fred_data,
    fetch_metal_prices,
    fetch_news_count,
    get_secret,
    sanitize_keys,
    to_ndjson,
)

import dotenv
import os

dotenv.load_dotenv()

# Get config file path relative to this file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
config = yaml.safe_load(open(config_path))

project_id = os.getenv("GCP_PROJECT_ID")
bucket_name = os.getenv("BUCKET_NAME")

if not bucket_name:
    raise ValueError("BUCKET_NAME environment variable is not set")
if not project_id:
    raise ValueError("GCP_PROJECT_ID environment variable is not set")

DBT_PROJECT_DIR = "/opt/airflow/global_commodity_dbt"
DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_DIR} && poetry run dbt"

def get_all_keys():
    logging.info("======== Fetching all secret keys ========")
    try:
        metal_key = get_secret("metal_price_api_key", project_id)
        currency_key = get_secret("currency_rates_api_key", project_id)
        fred_key = get_secret("fred_api_key", project_id)
        news_key = get_secret("news_api_key", project_id)
        return {
            "metal_key": metal_key,
            "currency_key": currency_key,
            "fred_key": fred_key,
            "news_key": news_key,
        }
    except Exception as e:
        logging.error(f"======= Error fetching secret keys: {e} ======")
        raise

@dag(
    dag_id="global_commodity",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 1, 28, tz="UTC"),
    catchup=False,
)
def global_commodity_dag():
    @task
    def task_get_all_keys():
        return get_all_keys()


    @task
    def extract_val_load_gcs(keys: dict):
        logging.info("======== Fetching all commodity data ========")
        keyword = config["news_keywords"]
        
        # Initialize empty data structure
        data = {
            "timestamp": pendulum.now("UTC").to_datetime_string(),
            "metals": None,
            "currency": None,
            "macro": None,
            "news": None,
        }
        
        try:
            # METAL PRICES - with error handling
            try:
                metal_prices = fetch_metal_prices(keys["metal_key"])
                if metal_prices.get("status") == "success":
                    data["metals"] = metal_prices
                    logging.info("✓ Metal prices fetched successfully")
                else:
                    logging.warning("⚠ Metal prices fetch failed, using None")
            except Exception as e:
                logging.warning(f"⚠ Metal prices error (continuing): {e}")

            # CURRENCY RATES - with error handling
            try:
                currency_rates = fetch_currency_rate(keys["currency_key"])
                if currency_rates.get("rates") and currency_rates["rates"].get("IDR"):
                    data["currency"] = currency_rates
                    logging.info("✓ Currency rates fetched successfully")
                else:
                    logging.warning("⚠ Currency rates fetch failed, using None")
            except Exception as e:
                logging.warning(f"⚠ Currency rates error (continuing): {e}")

            # FRED DATA - with error handling (TOLERANT)
            try:
                fred_data = fetch_fred_data(keys["fred_key"])
                
                # Check if we got ANY data
                valid_fred_data = {}
                for key in fred_data.keys():
                    count = fred_data[key].get("count", 0)
                    if count > 0:
                        valid_fred_data[key] = fred_data[key]
                        logging.info(f"✓ FRED data for {key}: {count} observations")
                    else:
                        logging.warning(f"⚠ FRED data for {key}: NO data (count={count}), skipping")
                
                # Use valid data if we have at least one series
                if valid_fred_data:
                    data["macro"] = valid_fred_data
                    logging.info(f"✓ FRED data fetched: {len(valid_fred_data)}/{len(fred_data)} series successful")
                else:
                    logging.warning("⚠ No valid FRED data available, using None")
                    data["macro"] = None
                    
            except Exception as e:
                logging.warning(f"⚠ FRED data error (continuing): {e}")
                data["macro"] = None

            # NEWS COUNT - with error handling
            try:
                news_count = fetch_news_count(keys["news_key"], keyword)
                if news_count:
                    # Filter out None values
                    valid_news = {k: v for k, v in news_count.items() if v is not None}
                    if valid_news:
                        data["news"] = valid_news
                        logging.info(f"✓ News count fetched: {len(valid_news)}/{len(news_count)} keywords successful")
                    else:
                        logging.warning("⚠ No valid news data, using None")
                else:
                    logging.warning("⚠ News count fetch failed, using None")
            except Exception as e:
                logging.warning(f"⚠ News count error (continuing): {e}")

            # GOOGLE TRENDS (commented out)
            # google_trends = fetch_google_trends(keyword)
            # if google_trends is None:
            #     logging.warning("⚠ Google Trends fetch failed")
            # logging.info("======== Google Trends done ========")

            # Check if we have AT LEAST ONE data source
            successful_sources = sum(1 for v in data.values() if v is not None and v != data["timestamp"])
            logging.info(f"======== Data collection summary: {successful_sources}/4 sources successful ========")
            
            if successful_sources == 0:
                raise ValueError("All data sources failed - cannot proceed with empty data")

            data = sanitize_keys(data)
            data = to_ndjson(data)
            
        except Exception as e:
            logging.error(f"======= Critical error in data extraction: {e} ======")
            raise

        # Load to GCS
        try:
            logging.info("======== Loading data to GCS ========")
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            destination_blob_name = f"global_commodity/date={pendulum.now('UTC').to_date_string()}/commodity_data.json"
            blob = bucket.blob(destination_blob_name)
            blob.upload_from_string(data, content_type="application/json")
            logging.info(f"✓ Data successfully loaded to GCS: gs://{bucket_name}/{destination_blob_name}")
        except Exception as e:
            logging.error(f"✗ Error loading data to GCS: {e}")
            raise
        
        return destination_blob_name

    get_all_keys_task = task_get_all_keys()
    evl_task = extract_val_load_gcs(get_all_keys_task)

    gcs_object_path = evl_task

    @task
    def task_gcs_to_bq(gcs_path: str):
        """Load data from GCS to BigQuery using BigQuery client directly"""
        logging.info("======== Loading data from GCS to BigQuery ========")
        try:
            bq_client = bigquery.Client(project="project-global-commodityy")
            
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=True,
                ignore_unknown_values=True,
            )
            
            table_id = "project-global-commodityy.dwh_commodity.commodity_data"
            uri = f"gs://{bucket_name}/{gcs_path}"
            
            load_job = bq_client.load_table_from_uri(
                uri, table_id, job_config=job_config
            )
            load_job.result()
            logging.info(f"✓ Loaded {load_job.output_rows} rows into {table_id}")
            return table_id
        except Exception as e:
            logging.error(f"✗ Error loading to BigQuery: {e}")
            raise

    gcs_to_bq_task = task_gcs_to_bq(gcs_object_path)

    # dbt transformation task
    dbt_run_task = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_COMMAND_PREFIX} run --full-refresh",
    )

    # dbt test
    dbt_test_task = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_COMMAND_PREFIX} test",
    )
    
    gcs_to_bq_task >> dbt_run_task >> dbt_test_task


commodity_dag = global_commodity_dag()