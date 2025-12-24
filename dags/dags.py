import json
import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

# import gcs to bigquery operator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from utils import (
    fetch_currency_rate,
    fetch_fred_data,
    fetch_google_trends,
    fetch_metal_prices,
    fetch_news_count,
    get_secret,
    sanitize_keys,
    to_ndjson,
)

project_id = "project-global-commodityy"
bucket_name = "data-lake-bronze-project-global-commodityy"
DBT_PROJECT_DIR = "/opt/airflow/global_commodity_dbt"
DBT_COMMAND_PREFIX = f"cd {DBT_PROJECT_DIR} && poetry run dbt"


@dag(
    dag_id="global_commodity",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 12, 6, tz="UTC"),
    catchup=True,
)
def global_commodity_dag():
    @task
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

    @task
    def extract_val_load_gcs(keys: dict):
        logging.info("======== Fetching all commodity data ========")
        keyword = ["gold", "silver", "platinum", "copper", "nickel"]
        try:
            metal_prices = fetch_metal_prices(keys["metal_key"])
            if metal_prices.get("status") != "success":
                raise ValueError("Failed to fetch metal prices")
            logging.info("======== Metal done ========")

            currency_rates = fetch_currency_rate(keys["currency_key"])
            if currency_rates.get("rates").get("IDR") is None:
                raise ValueError("Failed to fetch currency rates")
            logging.info("======== Currency done ========")

            fred_data = fetch_fred_data(keys["fred_key"])
            for key in fred_data.keys():
                if fred_data[key].get("count") < 1:
                    raise ValueError(f"Failed to fetch FRED data for {key}")
            logging.info("======== FRED done ========")

            news_count = fetch_news_count(keys["news_key"], keyword)
            for key in news_count.keys():
                if news_count[key] is None:
                    raise ValueError(f"Failed to fetch news count for {key}")
            logging.info("======== News done ========")

            # google_trends = fetch_google_trends(keyword)
            # if google_trends is None:
            #     raise ValueError("Failed to fetch Google Trends data")
            # logging.info("======== Google Trends done ========")

            data = {
                "timestamp": pendulum.now("UTC").to_datetime_string(),
                "metals": metal_prices,
                "currency": currency_rates,
                "macro": fred_data,
                "news": news_count,
            }

            data = sanitize_keys(data)
            data = to_ndjson(data)
        except Exception as e:
            logging.error(f"======= Error fetching commodity data: {e} ======")
            raise

        # Load to GCS
        try:
            logging.info("======== Loading data to GCS ========")
            gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
            destination_blob_name = f"global_commodity/date={pendulum.now('UTC').to_date_string()}/commodity_data.json"
            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=destination_blob_name,
                data=data,
                mime_type="application/json",
            )
            logging.info("======== Data successfully loaded to GCS ========")
        except Exception as e:
            logging.error(f"======= Error loading data to GCS: {e} ======")
            raise
        # RETURN path for xcom
        return destination_blob_name

    get_all_keys_task = get_all_keys()
    evl_task = extract_val_load_gcs(get_all_keys_task)

    gcs_object_path = evl_task
    # gcs_object_path = "global_commodity/date=2025-12-09/commodity_data.json"

    gcs_to_bq = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=bucket_name,
        source_objects=[gcs_object_path],
        destination_project_dataset_table="project-global-commodityy.dwh_commodity.commodity_data",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_APPEND",
        autodetect=True,
        gcp_conn_id="google_cloud_default",
        ignore_unknown_values=True,
        project_id="project-global-commodityy",
        schema_update_options=["ALLOW_FIELD_ADDITION"],
    )

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
    # gcs_to_bq
    gcs_to_bq >> dbt_run_task >> dbt_test_task


commodity_dag = global_commodity_dag()
