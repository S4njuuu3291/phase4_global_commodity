import datetime
import logging
import time

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
            "copper",
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

    now = datetime.datetime.now().date()

    for series_id in series_ids:
        if series_id == "CPIAUCSL":
            # must be in YYYY-MM-DD format
            week_ago = now - datetime.timedelta(days=180)
        else:
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


import datetime
import json
import logging
import random
import time

from pytrends.request import TrendReq


def fetch_google_trends(keywords):
    if not keywords:
        logging.warning("Keyword list cannot be empty.")
        return None

    all_trends_data = []

    try:
        # logging.info(
        #     f"Fetching Google Trends data individually for long-term (5-year) and short-term (7-day) averages."
        # )

        pytrends_inst = TrendReq()
        yesterday_str = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime(
            "%Y-%m-%d"
        )

        for keyword in keywords:
            # TIME_RANGE_5Y = "today 5-y"
            # pytrends_inst.build_payload([keyword], timeframe=TIME_RANGE_5Y)
            # trends_data_5y = pytrends_inst.interest_over_time()

            TIME_RANGE_7D = "now 7-d"
            pytrends_inst.build_payload([keyword], timeframe=TIME_RANGE_7D)
            trends_data_7d = pytrends_inst.interest_over_time()

            # avg_5y = None
            # if not trends_data_5y.empty and keyword in trends_data_5y.columns:
            #     if "isPartial" in trends_data_5y.columns:
            #         trends_data_5y = trends_data_5y.drop(columns=["isPartial"])
            #     avg_5y = float(trends_data_5y[keyword].mean(numeric_only=True))
            #     logging.info(f"5Y Avg score for '{keyword}': {avg_5y:.2f}")
            # else:
            #     logging.warning(f"No 5Y data found for keyword: '{keyword}'.")

            avg_7d = None
            if not trends_data_7d.empty and keyword in trends_data_7d.columns:
                if "isPartial" in trends_data_7d.columns:
                    trends_data_7d = trends_data_7d.drop(columns=["isPartial"])
                avg_7d = float(trends_data_7d[keyword].mean(numeric_only=True))
                logging.info(f"7D Avg score for '{keyword}': {avg_7d:.2f}")
            else:
                logging.warning(f"No 7D data found for keyword: '{keyword}'.")

            data_point = {
                "date": yesterday_str,
                "indicator_name": keyword,
                # "indicator_value_5y_avg": avg_5y,
                "indicator_value_7d_avg": avg_7d,
            }

            all_trends_data.append(data_point)

            sleep_time = random.randint(8, 15)
            logging.info(f"Pausing for {sleep_time} seconds...")
            time.sleep(sleep_time)

        if all_trends_data:
            logging.info(
                f"Successfully aggregated {len(all_trends_data)} trends scores."
            )
            return json.dumps(all_trends_data)
        else:
            logging.warning("No Google Trends data found for any of the keywords.")
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


def to_ndjson(record: dict) -> str:
    return json.dumps(record) + "\n"


import re


def sanitize_keys(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for k, v in obj.items():
            # Replace invalid characters with underscore
            new_key = re.sub(r"[^a-zA-Z0-9_]", "_", k)

            # If starts with digit, prefix with underscore
            if re.match(r"^[0-9]", new_key):
                new_key = f"_{new_key}"

            new_obj[new_key] = sanitize_keys(v)
        return new_obj
    elif isinstance(obj, list):
        return [sanitize_keys(i) for i in obj]
    else:
        return obj
