import datetime
import logging
import json
import sys
from typing import Optional, Dict, List, Any

import httpx
import yaml
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import boto3
from botocore.exceptions import ClientError

# Ensure dags folder is on sys.path for Airflow DAG parsing
_DAGS_DIR = os.path.dirname(os.path.abspath(__file__))
if _DAGS_DIR not in sys.path:
    sys.path.insert(0, _DAGS_DIR)

# Import with retry logic in case sys.path isn't set yet
try:
    # from models import ProcessedMetalModel,CurrencyRateModel,FredDataModel
    from exceptions import APIFetchError, ValidationError, SecretManagerError, S3UploadError
except ModuleNotFoundError as e:
    # Fallback: explicitly add /opt/airflow/dags to sys.path
    if '/opt/airflow/dags' not in sys.path:
        sys.path.insert(0, '/opt/airflow/dags')
    # Retry import
    from exceptions import APIFetchError, ValidationError, SecretManagerError, S3UploadError

# Setup konfigurasi log
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)    # Print ke terminal
    ]
)

# Get config file path relative to this file
config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
try:
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
except Exception as e:
    logging.error(f"Failed to load config.yaml: {e}")
    config = {}

# Lazy initialization untuk AWS clients
# Akan diinisialisasi hanya saat function dipanggil, bukan saat import
ssm = None
s3 = None
bucket_name = config.get("bucket_name", "")

def _init_aws_clients():
    """Initialize AWS clients lazily on first use"""
    global ssm, s3
    if ssm is None or s3 is None:
        try:
            ssm = boto3.client('ssm', region_name='ap-southeast-1')
            s3 = boto3.client('s3', region_name='ap-southeast-1')
            logging.info("AWS clients initialized successfully")
        except Exception as e:
            logging.error(f"Failed to initialize AWS clients: {e}")
            raise

def get_secret_ssm(api_name: str):
    """Fetch API config from AWS SSM Parameter Store"""
    _init_aws_clients()
    
    # Sesuaikan dengan path hasil Terraform kamu
    path = f"/global-commodity/datasource/{api_name}/"
    
    try:
        # Gunakan get_parameters_by_path untuk ambil semua isi folder
        response = ssm.get_parameters_by_path(
            Path=path,
            Recursive=True,
            WithDecryption=True
        )

        if not response['Parameters']:
            print(f"[-] Data tidak ditemukan untuk path: {path}")
            return None

        config = {
            param['Name'].split('/')[-1]: param['Value'].strip() 
            for param in response['Parameters']
        }

        print(f"[+] Berhasil mengambil config untuk: {api_name}")
        return config

    except ClientError as e:
        print(f"[-] Error AWS: {e}")
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def upload_response_to_s3(
    data: Dict[str, Any], 
    bucket_name: str, 
    s3_key: str
) -> bool:
    """
    Defensive function to upload dictionary data to S3 as NDJSON/JSON.
    """
    _init_aws_clients()
    
    try:
        # 1. Pre-upload Validation
        if not data:
            logging.error("Attempted to upload empty data to S3.")
            return False
            
        # 2. Convert to String (NDJSON ready)
        # Kita pakai ensure_ascii=False agar karakter unik tetap aman
        json_data = json.dumps(data, ensure_ascii=False) + "\n"
        
        # 3. AWS Client Setup
        # Di AWS, session management lebih baik dilakukan via IAM Role
        s3_client = boto3.client('s3')
        
        logging.info(f"Uploading data to s3://{bucket_name}/{s3_key}")
        
        # 4. Execution with Body Check
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/x-ndjson'
        )
        
        logging.info("Upload successful.")
        return True

    except ClientError as e:
        # Menangkap error spesifik AWS (misal: Bucket tidak ada atau Access Denied)
        error_code = e.response['Error']['Code']
        logging.error(f"AWS Error [{error_code}]: {e}")
        raise S3UploadError(bucket_name, s3_key, str(e))
        
    except Exception as e:
        # Menangkap error tak terduga (misal: JSON Serialization error)
        logging.error(f"Unexpected error during S3 upload: {e}")
        return False

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_metal_prices(ssm_keyword: str) -> Optional[str]:

    metals_url_key = get_secret_ssm(ssm_keyword)
    url = metals_url_key["url"]
    api_key = metals_url_key["api_key"]

    #YYYY-MM-DD HH
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")

    try:
        if not api_key:
            raise ValueError("API key cannot be empty")
            
        logging.info(f"Fetching metal prices from API.")  # Log tujuannya saja
        response = httpx.get(
            url,
            params={
                "api_key": api_key,
                "currency": "USD",
                "unit": "g"
            },
            timeout=10
        )
        
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
        
        logging.info("Metal prices fetched successfully")
        
        # Check for required fields
        # metals = data.get("metals")
        # if not metals:
        #     raise ValueError("Missing 'metals' field in response")
        
        # # Check if any metals are null
        # for metal_name in ["gold", "silver", "platinum", "copper", "nickel"]:
        #     if metals.get(metal_name) is None:
        #         logging.warning(f"Metal price for {metal_name} is null")
        
        # timestamps = data.get("timestamps")
        # if not timestamps or not timestamps.get("metal"):
        #     raise ValueError("Missing or invalid timestamp in response")
            
        # relevant_metals = [
        #     "gold",
        #     "silver",
        #     "platinum",
        #     "copper",
        #     "nickel",
        # ]
        # filtered_metals = {
        #     k: v for k, v in metals.items() if k in relevant_metals
        # }
        
        # if not filtered_metals:
        #     raise ValueError("No relevant metals found in API response")
        
    except httpx.TimeoutException:
        logging.error("Metal API request timed out after 10 seconds")
        return None
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
        logging.error(f"Error fetching metal prices: {e}")
        return None

    s3_key = f"{config['bronze_prefix']}/{timestamp}/metal_prices-{timestamp}.json"

    upload_success = upload_response_to_s3(data, bucket_name, s3_key)
    if not upload_success:
        logging.error("Failed to upload metal prices to S3.")
    else:
        logging.info("Metal prices uploaded to S3 successfully.")
        return s3_key
    
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_currency_rate(ssm_keyword) -> Optional[str]:

    currency_url_key = get_secret_ssm(ssm_keyword)
    url = currency_url_key["url"]
    api_key = currency_url_key["api_key"]

    try:
        if not api_key:
            raise ValueError("API key cannot be empty")
            
        logging.info("Fetching currency rates from API.")  # Log tujuannya saja
        response = httpx.get(
            url,
            params={"apikey": api_key},
            timeout=10
        )
        
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
        
        # # Validate required fields
        # rates = data.get("rates")
        # if not rates:
        #     raise ValueError("Missing 'rates' field in response")
        
        # if not isinstance(rates, dict) or len(rates) == 0:
        #     raise ValueError("Rates field is empty or invalid")
        
        # # Check for critical currency (IDR for Indonesia)
        # if rates.get("IDR") is None:
        #     logging.warning("IDR rate is missing from response")
        
        # # Check for null values in rates
        # null_rates = [k for k, v in rates.items() if v is None]
        # if null_rates:
        #     logging.warning(f"Null rates found for: {null_rates}")
            
        # processed_data_model = CurrencyRateModel(**data)
        # return processed_data_model.model_dump()

        logging.info("Currency rates fetched successfully")

    except httpx.TimeoutException:
        print("Currency API request timed out after 10 seconds")
        return None
    except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
        print(f"Error fetching currency rates: {e}")
        return None

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
    s3_key = f"{config['bronze_prefix']}/{timestamp}/currency_rates-{timestamp}.json"

    upload_success = upload_response_to_s3(data, bucket_name, s3_key)
    if not upload_success:
        logging.error("Failed to upload currency rates to S3.")
    else:        
        logging.info("Currency rates uploaded to S3 successfully.")
        return s3_key

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_fred_data(ssm_keyword: str) -> Optional[str]:

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

        fred_url_key = get_secret_ssm(ssm_keyword)
        url = fred_url_key["url"]
        api_key = fred_url_key["api_key"]

        try:
            if not api_key:
                raise ValueError("API key cannot be empty")
                
            logging.info(f"Fetching FRED data for series: {series_id}")  # Log tujuannya saja
            response = httpx.get(
                url,
                params={
                    "series_id": series_id,
                    "api_key": api_key,
                    "file_type": "json",
                    "observation_start": str(week_ago),
                    "observation_end": str(now)
                },
                timeout=10
            )
            
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

            logging.info(f"FRED data fetched successfully for {series_id}")
            
            macro_datas[series_id] = data
            # # Validate required fields
            # observations = data.get("observations")
            # if observations is None:
            #     raise ValueError(f"Missing 'observations' field for {series_id}")
            
            # if not isinstance(observations, list):
            #     raise ValueError(f"Observations field is not a list for {series_id}")
            
            # # Check if observations is empty (count < 1)
            # count = data.get("count", 0)
            # if count < 1:
            #     raise ValueError(f"No observations available for {series_id} (count={count})")
            
            # # Warn if observations have null values
            # null_observations = [obs for obs in observations if obs.get("value") is None]
            # if null_observations:
            #     logging.warning(f"Found {len(null_observations)} null values in {series_id}")
                
            # processed_data_model = FredDataModel(**data)
            # data = processed_data_model.model_dump()
            # macro_datas[series_id] = data
            # logging.info(f"FRED data fetched for {series_id} ({count} observations)")
            
        except httpx.TimeoutException:
            logging.error(f"FRED API request timed out for {series_id}")
            return None
        except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
            logging.error(f"Error fetching FRED data for {series_id}: {e}")
            return None

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        s3_key = f"{config['bronze_prefix']}/{timestamp}/fred_data-{timestamp}.json"

        upload_success = upload_response_to_s3(macro_datas, bucket_name, s3_key)
        if not upload_success:
            logging.error("Failed to upload FRED data to S3.")
        else:
            logging.info("FRED data uploaded to S3 successfully.")
            
    return s3_key

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
)
def fetch_news(ssm_keyword: str) -> Optional[str]:
    """Fetch raw news article data from NewsAPI for configured keywords.
    
    Fetches raw API responses for each keyword from config.yaml.
    Returns consolidated raw responses as-is (no transformation).
    Automatically retries up to 3 times with exponential backoff on network errors.
    
    Args:
        ssm_keyword: SSM path keyword to retrieve NewsAPI credentials
                    (e.g., "news" → /global-commodity/datasource/news/)
        
    Returns:
        S3 path (string) where raw news data was uploaded
        Format: {keyword1: {raw_response}, keyword2: {raw_response}, ...}
        Returns None if any API request fails
        
    Raises:
        httpx.HTTPStatusError: If API returns error status (after retries exhausted)
        ValueError: If SSM config is invalid or response is malformed
        
    Example:
        >>> s3_path = fetch_news("news")
        >>> print(s3_path)
        "bronze/2026-04-08 15:00:00/news_data-2026-04-08 15:00:00.json"
    """
    news_raw_response: Dict[str, Any] = {}
    keywords = config["news_keywords"]
    
    news_config = get_secret_ssm(ssm_keyword)
    url = news_config["url"]
    api_key = news_config["api_key"]
    
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
    now = datetime.datetime.now().date()
    
    try:
        if not api_key:
            raise ValueError("API key cannot be empty")
        if not keywords or not isinstance(keywords, list):
            raise ValueError("Keywords must be a non-empty list")
        
        for keyword in keywords:
            if not keyword:
                logging.warning("Skipping empty keyword")
                continue
            
            try:
                logging.info(f"Fetching news data for keyword: {keyword}")  # Log tujuannya saja
                response = httpx.get(
                    url,
                    params={
                        "q": keyword,
                        "from": str(now),
                        "to": str(now),
                        "apiKey": api_key
                    },
                    timeout=10
                )
                
                # Handle rate limiting with longer backoff
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
                
                # # VALIDATION COMMENTED OUT - BRONZE LAYER RAW DATA ONLY
                # # Validate required fields
                # status = data.get("status")
                # if status is None:
                #     raise ValueError(f"Missing 'status' field for keyword '{keyword}'")
                # 
                # if status != "ok":
                #     raise ValueError(f"API returned non-ok status '{status}' for keyword '{keyword}'")
                # 
                # total_results = data.get("totalResults")
                # if total_results is None:
                #     raise ValueError(f"Missing 'totalResults' field for keyword '{keyword}'")
                # 
                # if not isinstance(total_results, int) or total_results < 0:
                #     raise ValueError(f"Invalid totalResults value {total_results} for keyword '{keyword}'")
                
                # # TRANSFORMATION COMMENTED OUT - BRONZE LAYER RAW DATA ONLY
                # # No transformation - store raw response as-is
                # processed_data_model = NewsCountModel(**{
                #     "status": status,
                #     "totalResults": total_results,
                # })
                # count = processed_data_model.model_dump()["totalResults"]
                # news_counts[keyword] = count
                
                # Store raw response as-is (Bronze layer = raw data only)
                news_raw_response[keyword] = data
                logging.info(f"News data fetched successfully for keyword '{keyword}'")
                
            except httpx.TimeoutException:
                logging.error(f"NewsAPI request timed out for keyword '{keyword}'")
                return None
            except (httpx.RequestError, httpx.HTTPStatusError, ValueError) as e:
                logging.error(f"Error fetching news data for '{keyword}': {e}")
                return None

    except ValueError as e:
        logging.error(f"Configuration error: {e}")
        return None
    
    s3_key = f"{config['bronze_prefix']}/{timestamp}/news_data-{timestamp}.json"

    upload_success = upload_response_to_s3(news_raw_response, bucket_name, s3_key)
    if not upload_success:
        logging.error("Failed to upload news data to S3.")
        return None
    else:
        logging.info("News data uploaded to S3 successfully.")
        return s3_key

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