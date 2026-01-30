import datetime
import logging
import time
import json
from typing import Optional, Dict, List, Any

import httpx
from google.cloud import secretmanager
from pytrends.request import TrendReq
import yaml
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from models import ProcessedMetalModel,CurrencyRateModel,FredDataModel, NewsCountModel
from exceptions import APIFetchError, ValidationError, SecretManagerError

# Get config file path relative to this file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
config = yaml.safe_load(open(config_path))

def get_secret(secret_name: str, project_id: str) -> str:
    """Fetch secret from Google Cloud Secret Manager.
    
    Retrieves the latest version of a secret from GCP Secret Manager.
    Raises SecretManagerError if secret cannot be accessed.
    
    Args:
        secret_name: Name of the secret to retrieve
        project_id: GCP project ID
        
    Returns:
        Secret value as string
        
    Raises:
        SecretManagerError: If secret fetch fails
        
    Example:
        >>> api_key = get_secret("metal_api_key", "my-project")
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": secret_path})
        secret_value = response.payload.data.decode("UTF-8")
        logging.info(f"Successfully fetched secret: {secret_name}")
        return secret_value
    except Exception as e:
        raise SecretManagerError(secret_name, project_id, str(e))


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_metal_prices(api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch metal prices from metals.dev API.
    
    Retrieves current prices for gold, silver, platinum, copper, and nickel.
    Automatically retries up to 3 times with exponential backoff on network errors.
    
    Args:
        api_key: API key for metals.dev API
        
    Returns:
        Dictionary containing metal prices with keys:
        - timestamp: ISO format timestamp from API
        - status: API response status ('success' or 'failed')
        - metals: Dict with metal prices {metal_name: price}
        - currency_base: Base currency (usually 'USD')
        Returns None if all retry attempts fail
        
    Raises:
        httpx.HTTPStatusError: If API returns error status (after retries exhausted)
        ValueError: If response validation fails
        
    Example:
        >>> prices = fetch_metal_prices("my_api_key")
        >>> gold_price = prices['metals']['gold']
    """
    url = f"{config['api_url']['metal']}?api_key={api_key}&currency=USD&unit=g"
    try:
        if not api_key:
            raise ValueError("API key cannot be empty")
            
        logging.info("Fetching metal prices from API.")
        response = httpx.get(url, timeout=10)
        
        # Handle rate limiting with longer backoff
        if response.status_code == 429:
            logging.warning("Rate limited by metals API (429)")
            raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
        
        response.raise_for_status()
        
        # Check for empty response
        if not response.text:
            raise ValueError("Empty response body from metals API")
        
        data = response.json()
        
        # Validate response structure
        if not data:
            raise ValueError("Empty JSON response from metals API")
        
        if data.get("status") != "success":
            raise ValueError(f"API returned non-success status: {data.get('status')}")
        
        # Check for required fields
        metals = data.get("metals")
        if not metals:
            raise ValueError("Missing 'metals' field in response")
        
        # Check if any metals are null
        for metal_name in ["gold", "silver", "platinum", "copper", "nickel"]:
            if metals.get(metal_name) is None:
                logging.warning(f"Metal price for {metal_name} is null")
        
        timestamps = data.get("timestamps")
        if not timestamps or not timestamps.get("metal"):
            raise ValueError("Missing or invalid timestamp in response")
            
        relevant_metals = [
            "gold",
            "silver",
            "platinum",
            "copper",
            "nickel",
        ]
        filtered_metals = {
            k: v for k, v in metals.items() if k in relevant_metals
        }
        
        if not filtered_metals:
            raise ValueError("No relevant metals found in API response")

        processed_data = {
            "timestamp": timestamps["metal"],
            "status": data["status"],
            "metals": filtered_metals,
            "currency_base": data.get("currency", "USD"),
        }
        processed_data_model = ProcessedMetalModel(**processed_data)
        logging.info("Metal prices fetched successfully")
        return processed_data_model.model_dump()
        
    except httpx.TimeoutException:
        logging.error("Metal API request timed out after 10 seconds")
        return None
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
        logging.error(f"Error fetching metal prices: {e}")
        return None


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_currency_rate(api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch currency exchange rates from currencyfreaks API.
    
    Retrieves current exchange rates for multiple currencies against base currency.
    Automatically retries up to 3 times with exponential backoff.
    
    Args:
        api_key: API key for currencyfreaks API
        
    Returns:
        Dictionary with exchange rates containing:
        - date: Date of exchange rate
        - base: Base currency code
        - rates: Dict with currency codes and their rates
        Returns None if all retry attempts fail
        
    Raises:
        httpx.HTTPStatusError: If API returns error
        ValueError: If response validation fails
        
    Example:
        >>> rates = fetch_currency_rate("my_api_key")
        >>> idr_rate = rates['rates']['IDR']
    """
    url = f"{config['api_url']['currency']}?apikey={api_key}"
    try:
        if not api_key:
            raise ValueError("API key cannot be empty")
            
        logging.info("Fetching currency rates from API.")
        response = httpx.get(url, timeout=10)
        
        # Handle rate limiting
        if response.status_code == 429:
            logging.warning("Rate limited by currency API (429)")
            raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
        
        response.raise_for_status()
        
        # Check for empty response
        if not response.text:
            raise ValueError("Empty response body from currency API")
        
        data = response.json()
        if not data:
            raise ValueError("Empty JSON response from currency API")
        
        # Validate required fields
        rates = data.get("rates")
        if not rates:
            raise ValueError("Missing 'rates' field in response")
        
        if not isinstance(rates, dict) or len(rates) == 0:
            raise ValueError("Rates field is empty or invalid")
        
        # Check for critical currency (IDR for Indonesia)
        if rates.get("IDR") is None:
            logging.warning("IDR rate is missing from response")
        
        # Check for null values in rates
        null_rates = [k for k, v in rates.items() if v is None]
        if null_rates:
            logging.warning(f"Null rates found for: {null_rates}")
            
        processed_data_model = CurrencyRateModel(**data)
        logging.info("Currency rates fetched successfully")
        return processed_data_model.model_dump()

    except httpx.TimeoutException:
        logging.error("Currency API request timed out after 10 seconds")
        return None
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
        logging.error(f"Error fetching currency rates: {e}")
        return None
    

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_fred_data(api_key: str) -> Optional[Dict[str, Any]]:
    """Fetch macroeconomic data from FRED (Federal Reserve Economic Data).
    
    Retrieves economic indicators: 10-year treasury yield, trade-weighted dollar index, 
    and consumer price index. Each series has its own lookback period.
    
    Args:
        api_key: API key for FRED API
        
    Returns:
        Dictionary with series_id keys, each containing:
        - count: Number of observations
        - observations: List of data points with date and value
        Returns None if any series fetch fails
        
    Raises:
        httpx.HTTPStatusError: If API returns error
        ValueError: If response validation fails
        
    Example:
        >>> fred_data = fetch_fred_data("my_api_key")
        >>> dgs10 = fred_data['DGS10']['observations'][0]
    """
    macro_datas: Dict[str, Any] = {}
    series_ids = config["fred_series_ids"]

    now = datetime.datetime.now().date()

    for series_id in series_ids:
        if series_id == "CPIAUCSL":
            # 180 days lookback for inflation data
            week_ago = now - datetime.timedelta(days=180)
        else:
            # 7 days lookback for other series
            week_ago = now - datetime.timedelta(days=7)

        try:
            if not api_key:
                raise ValueError("API key cannot be empty")
                
            logging.info(f"Fetching FRED data for series ID: {series_id}")
            url = f"{config['api_url']['fred']}?series_id={series_id}&api_key={api_key}&file_type=json&observation_start={week_ago}&observation_end={now}"
            response = httpx.get(url, timeout=10)
            
            # Handle rate limiting
            if response.status_code == 429:
                logging.warning(f"Rate limited by FRED API for {series_id} (429)")
                raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
            
            response.raise_for_status()
            
            # Check for empty response
            if not response.text:
                raise ValueError(f"Empty response body for FRED series {series_id}")
            
            data = response.json()
            
            if not data:
                raise ValueError(f"Empty JSON response for FRED series {series_id}")
            
            # Validate required fields
            observations = data.get("observations")
            if observations is None:
                raise ValueError(f"Missing 'observations' field for {series_id}")
            
            if not isinstance(observations, list):
                raise ValueError(f"Observations field is not a list for {series_id}")
            
            # Check if observations is empty (count < 1)
            count = data.get("count", 0)
            if count < 1:
                raise ValueError(f"No observations available for {series_id} (count={count})")
            
            # Warn if observations have null values
            null_observations = [obs for obs in observations if obs.get("value") is None]
            if null_observations:
                logging.warning(f"Found {len(null_observations)} null values in {series_id}")
                
            processed_data_model = FredDataModel(**data)
            data = processed_data_model.model_dump()
            macro_datas[series_id] = data
            logging.info(f"FRED data fetched for {series_id} ({count} observations)")
            
        except httpx.TimeoutException:
            logging.error(f"FRED API request timed out for {series_id}")
            return None
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
            logging.error(f"Error fetching FRED data for {series_id}: {e}")
            return None
            
    return macro_datas

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_news_count(api_key: str, keywords: List[str]) -> Optional[Dict[str, Any]]:
    """Fetch news article counts from NewsAPI for given keywords.
    
    Counts number of news articles mentioning each keyword for today's date.
    Automatically retries up to 3 times with exponential backoff.
    
    Args:
        api_key: API key for NewsAPI
        keywords: List of keywords to search for (e.g., ['gold', 'silver'])
        
    Returns:
        Dictionary with keyword keys and article counts:
        {
            "timestamp": "2026-01-30",
            "gold": 150,
            "silver": 120,
            ...
        }
        Returns None if any request fails
        
    Raises:
        httpx.HTTPStatusError: If API returns error
        ValueError: If response validation fails
        
    Example:
        >>> news = fetch_news_count("my_api_key", ["gold", "silver"])
        >>> gold_count = news['gold']
    """
    news_counts: Dict[str, Any] = {"timestamp": datetime.datetime.now().strftime("%Y-%m-%d")}
    now = datetime.datetime.now().date()
    
    if not api_key:
        raise ValueError("API key cannot be empty")
    if not keywords or not isinstance(keywords, list):
        raise ValueError("Keywords must be a non-empty list")
        
    for keyword in keywords:
        if not keyword:
            logging.warning("Skipping empty keyword")
            continue
            
        url = f"{config['api_url']['news_api']}?q={keyword}&from={now}&to={now}&apiKey={api_key}"
        try:
            logging.info(f"Fetching news count for keyword: {keyword}")
            response = httpx.get(url, timeout=10)
            
            # Handle rate limiting
            if response.status_code == 429:
                logging.warning(f"Rate limited by NewsAPI for '{keyword}' (429)")
                raise httpx.HTTPStatusError("Rate limited", request=response.request, response=response)
            
            response.raise_for_status()
            
            # Check for empty response
            if not response.text:
                raise ValueError(f"Empty response body for keyword '{keyword}'")
            
            data = response.json()
            if not data:
                raise ValueError(f"Empty JSON response for keyword '{keyword}'")
            
            # Validate required fields
            status = data.get("status")
            if status is None:
                raise ValueError(f"Missing 'status' field for keyword '{keyword}'")
            
            if status != "ok":
                raise ValueError(f"API returned non-ok status '{status}' for keyword '{keyword}'")
            
            total_results = data.get("totalResults")
            if total_results is None:
                raise ValueError(f"Missing 'totalResults' field for keyword '{keyword}'")
            
            if not isinstance(total_results, int) or total_results < 0:
                raise ValueError(f"Invalid totalResults value {total_results} for keyword '{keyword}'")
            
            processed_data_model = NewsCountModel(**{
                "status": status,
                "totalResults": total_results,
            })
            count = processed_data_model.model_dump()["totalResults"]
            news_counts[keyword] = count
            logging.info(f"News count for '{keyword}': {count} articles")
            
        except httpx.TimeoutException:
            logging.error(f"NewsAPI request timed out for keyword '{keyword}'")
            return None
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
            logging.error(f"Error fetching news count for '{keyword}': {e}")
            return None
            
    return news_counts


def to_ndjson(record: Dict[str, Any]) -> str:
    """Convert dictionary to NDJSON (newline-delimited JSON) format.
    
    Serializes a dictionary to JSON and appends newline.
    Suitable for BigQuery NEWLINE_DELIMITED_JSON format.
    
    Args:
        record: Dictionary to convert
        
    Returns:
        JSON string with trailing newline
        
    Raises:
        TypeError: If record is not serializable to JSON
        
    Example:
        >>> ndjson = to_ndjson({"name": "gold", "price": 2000})
        >>> print(ndjson)
        {"name": "gold", "price": 2000}
    """
    if not isinstance(record, dict):
        raise TypeError(f"Expected dict, got {type(record).__name__}")
        
    try:
        return json.dumps(record) + "\n"
    except (TypeError, ValueError) as e:
        logging.error(f"Error serializing record to JSON: {e}")
        raise


def sanitize_keys(obj: Any) -> Any:
    """Sanitize dictionary keys for BigQuery compatibility.
    
    Recursively processes dictionaries to:
    - Replace special characters with underscores
    - Prefix numeric-starting keys with underscore
    - Handle nested dicts and lists
    
    Args:
        obj: Dictionary, list, or scalar value to sanitize
        
    Returns:
        Sanitized object with valid BigQuery column names
        
    Example:
        >>> data = {"bad key!": 123, "123field": "value"}
        >>> sanitize_keys(data)
        {"bad_key_": 123, "_123field": "value"}
    """
    import re
    
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


def fetch_google_trends(keywords: List[str]) -> Optional[List[Dict[str, Any]]]:
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