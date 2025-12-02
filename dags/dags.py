import logging

from airflow.decorators import dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from utils import (
    fetch_currency_rate,
    fetch_fred_data,
    fetch_google_trends,
    fetch_metal_prices,
    fetch_news_count,
    get_secret,
)

project_id = "project-global-commodityy"


@dag(
    dag_id="global_commodity",
    schedule_interval="@daily",
    start_date="2025-12-02",
    catchup=True,
)
def global_commodity_dag():
    @task
    def get_all_keys():
        logging.info("Fetching all secret keys")
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
            logging.error(f"Error fetching secret keys: {e}")


commodity_dag = global_commodity_dag()
