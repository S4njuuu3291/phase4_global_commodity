import datetime
import json
import logging
import re
import time
from cmath import e

import pandas as pd
import requests
from google.cloud import secretmanager
from pytrends.request import TrendReq


def get_secret(secret_name, project_id):
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": secret_path})
    secret_value = response.payload.data.decode("UTF-8")
    return secret_value


def fetch_metal_prices(API_KEY):
    url = f"https://api.metals.dev/v1/latest?api_key={API_KEY}&currency=USD&unit=g"
    try:
        logging.info("Fetching metal prices from API.")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        relevant_metals = [
            "gold",
            "silver",
            "platinum",
            "palladium",
            "copper",
            "aluminum",
            "nickel",
        ]
        filtered_metals = {
            k: v for k, v in data["metals"].items() if k in relevant_metals
        }

        processed_data = {
            "timestamp": data["timestamps"]["metal"],
            "status": data["status"],
            "metals": filtered_metals,
            "currency_base": data["currency"],
        }

        return processed_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def fetch_currency_rate(API_KEY):
    url = f"https://api.currencyfreaks.com/v2.0/rates/latest?apikey={API_KEY}"
    try:
        logging.info("Fetching currency rates from API.")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data

    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        return None


def fetch_fred_data(API_KEY):
    macro_datas = {}
    series_ids = ["DGS10", "DTWEXBGS", "CPIAUCSL"]

    for series_id in series_ids:
        now = datetime.datetime.now()
        week_ago = now - datetime.timedelta(days=7)

        try:
            logging.info(f"Fetching FRED data for series ID: {series_id}")
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={API_KEY}&file_type=json&observation_start={week_ago}&observation_end={now}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            macro_datas[series_id] = data
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            return None
    if len(macro_datas) == 3 and all(key in macro_datas for key in series_ids):
        return macro_datas


def fetch_google_trends(keywords):
    try:
        logging.info(f"Fetching Google Trends data for timeframe: today 3-m.")

        TIME_RANGE = "today 3-m"

        pytrends_inst = TrendReq()
        pytrends_inst.build_payload(keywords, timeframe=TIME_RANGE)
        trends_data = pytrends_inst.interest_over_time()

        if not trends_data.empty:
            if "isPartial" in trends_data.columns:
                trends_data = trends_data.drop(columns=["isPartial"])

            trends_summary_series = trends_data.mean(numeric_only=True)

            trends_summary_df = trends_summary_series.reset_index()

            trends_summary_df.columns = ["indicator_name", "indicator_value"]

            yesterday_str = (
                datetime.datetime.now() - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            trends_summary_df["date"] = yesterday_str
            processed_data = trends_summary_df[
                ["date", "indicator_name", "indicator_value"]
            ]
            logging.info(
                f"Successfully aggregated {len(processed_data)} daily trends scores."
            )
            return processed_data.to_dict(orient="records")
        else:
            logging.warning("No Google Trends data found for the given keywords.")
            return None
    except Exception as e:
        logging.error(f"Error fetching Google Trends data: {e}")
        return None


def fetch_news_count(API_KEY, keywords):
    news_counts = {"timestamp": datetime.datetime.now().strftime("%Y-%m-%d")}
    for keyword in keywords:
        url = f"https://newsapi.org/v2/everything?q={keyword}&from=YYYY-MM-DD&to=YYYY-MM-DD&apiKey={API_KEY}"
        try:
            logging.info(f"Fetching news count for keyword: {keyword}")
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if data["status"] == "ok":
                news_counts[keyword] = data["totalResults"]
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            return None
    return news_counts


API_KEY_NEWS = get_secret("news_api_key", "project-global-commodityy")
print(
    fetch_news_count(
        API_KEY_NEWS,
        ["gold", "silver", "platinum", "palladium", "copper", "aluminum", "nickel"],
    )
)
