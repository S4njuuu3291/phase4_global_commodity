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
from models import ProcessedMetalModel, CurrencyRateModel, FredDataModel, NewsCountModel

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
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def read_response_from_s3(
    bucket_name: str,
    s3_key: str
) -> Optional[Dict[str, Any]]:
    """

    Defensive function to read dictionary data from S3 (NDJSON/JSON format).
    
    Args:
        bucket_name: S3 bucket name
        s3_key: S3 object key/path
        
    Returns:
        Dictionary containing parsed JSON data
        Returns None if read fails
        
    Raises:
        ClientError: If S3 operation fails
        
    Example:
        >>> data = read_response_from_s3("my-bucket", "bronze/2026-04-08/data.json")
        >>> print(data)
    """
    _init_aws_clients()
    
    try:
        # 1. Pre-read Validation
        if not bucket_name or not s3_key:
            logging.error("Bucket name and S3 key cannot be empty")
            return None
        
        # 2. AWS Client Setup
        s3_client = boto3.client('s3')
        
        logging.info(f"Reading data from s3://{bucket_name}/{s3_key}")
        
        # 3. Get Object from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        
        # 4. Read Body and Parse JSON
        body = response['Body'].read().decode('utf-8')
        data = json.loads(body)
        
        logging.info("Read successful.")
        return data
        
    except ClientError as e:
        # Menangkap error spesifik AWS
        error_code = e.response['Error']['Code']
        logging.error(f"AWS Error [{error_code}]: {e}")
        return None
        
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse JSON from S3: {e}")
        return None
        
    except Exception as e:
        # Menangkap error tak terduga
        logging.error(f"Unexpected error during S3 read: {e}")
        return None

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
                        "from": str(now - datetime.timedelta(days=1)),
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
    if not isinstance(record, dict):
        raise TypeError(f"Expected dict, got {type(record).__name__}")
        
    try:
        return json.dumps(record) + "\n"
    except (TypeError, ValueError) as e:
        logging.error(f"Error serializing record to JSON: {e}")
        raise


def sanitize_keys(obj: Any) -> Any:
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

# silver

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def transform_metal_prices(metal_s3_key: str) -> Optional[str]:
    """Transform raw metal prices data from bronze to silver layer.
    
    Args:
        metal_s3_key: S3 path to raw bronze metal prices data
        
    Returns:
        S3 path to processed silver layer data
        Returns None if transformation fails
    """
    logging.info(f"[TRANSFORM] Starting metal prices transformation from: {metal_s3_key}")
    
    try:
        # Read bronze layer data
        logging.debug("Reading raw metal prices from S3")
        data = read_response_from_s3(bucket_name, metal_s3_key)
        if not data:
            logging.error("Failed to read metal prices data from S3")
            return None
        
        # Transform records
        logging.info("Transforming metal prices records")
        processed_records = []
        metal_list = config.get("news_keywords", [])
        
        for metal in metal_list:
            metal_records = {}
            metal_records["event_timestamp"] = data.get("timestamps", {}).get("metal")
            metal_records["metal_symbol"] = metal
            metal_records["price_usd"] = data.get("metals", {}).get(metal)
            metal_records["currency_base"] = data.get("currency")
            metal_records["unit"] = data.get("unit")

            try:
                model_instance = ProcessedMetalModel(**metal_records)
                processed_records.append(model_instance.model_dump(mode="json"))
                logging.debug(f"Transformed metal price for: {metal}")
            except Exception as e:
                logging.warning(f"[WARN] Skipping metal '{metal}': {e}")
        
        logging.info(f"Successfully transformed {len(processed_records)} metal price records")
        
        # Upload to silver layer
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        s3_key = f"{config['silver_prefix']}/{timestamp}/processed_metal_prices-{timestamp}.json"
        
        logging.info(f"Uploading transformed data to silver layer: {s3_key}")
        upload_success = upload_response_to_s3(processed_records, bucket_name, s3_key)
        
        if not upload_success:
            logging.error("[ERROR] Failed to upload processed metal prices to S3")
            return None
        else:
            logging.info(f"[SUCCESS] Metal prices transformation complete: {s3_key}")
            return s3_key
            
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error in metal prices transformation: {e}", exc_info=True)
        raise

# print(transform_metal_prices("bronze/2026-04-08 10:00:00/metal_prices-2026-04-08 10:00:00.json"))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def transform_currency_rates(currency_s3_key: str) -> Optional[str]:
    """Transform raw currency rates data from bronze to silver layer.
    
    Args:
        currency_s3_key: S3 path to raw bronze currency rates data
        
    Returns:
        S3 path to processed silver layer data
        Returns None if transformation fails
    """
    logging.info(f"[TRANSFORM] Starting currency rates transformation from: {currency_s3_key}")
    
    try:
        # Read bronze layer data
        logging.debug("Reading raw currency rates from S3")
        data = read_response_from_s3(bucket_name, currency_s3_key)
        if not data:
            logging.error("Failed to read currency rates data from S3")
            return None
        
        # Transform records
        logging.info("Transforming currency rates records")
        currency_list = config.get("currency", [])
        processed_records = []
        
        for currency in currency_list:
            currency_records = {}
            currency_records["rate_date"] = data.get("date")
            currency_records["currency_code"] = currency
            currency_records["exchange_rate"] = float(data.get("rates", {}).get(currency))
            currency_records["base_currency"] = data.get("base")

            try:
                model_instance = CurrencyRateModel(**currency_records)
                processed_records.append(model_instance.model_dump(mode="json"))
                logging.debug(f"Transformed currency rate for: {currency}")
            except Exception as e:
                logging.warning(f"[WARN] Skipping currency '{currency}': {e}")

        logging.info(f"Successfully transformed {len(processed_records)} currency rate records")
        
        # Upload to silver layer
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        s3_key = f"{config['silver_prefix']}/{timestamp}/processed_currency_rates-{timestamp}.json"
        
        logging.info(f"Uploading transformed data to silver layer: {s3_key}")
        upload_success = upload_response_to_s3(processed_records, bucket_name, s3_key)
        
        if not upload_success:
            logging.error("[ERROR] Failed to upload processed currency rates to S3")
            return None
        else:
            logging.info(f"[SUCCESS] Currency rates transformation complete: {s3_key}")
            return s3_key
            
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error in currency rates transformation: {e}", exc_info=True)
        raise

# print(transform_currency_rates("bronze/2026-04-08 10:00:00/currency_rates-2026-04-08 10:00:00.json"))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def transform_fred_data(fred_s3_key: str) -> Optional[str]:
    """Transform raw FRED data from bronze to silver layer.
    
    Args:
        fred_s3_key: S3 path to raw bronze FRED macro data
        
    Returns:
        S3 path to processed silver layer data
        Returns None if transformation fails
    """
    logging.info(f"[TRANSFORM] Starting FRED data transformation from: {fred_s3_key}")
    
    try:
        # Read bronze layer data
        logging.debug("Reading raw FRED data from S3")
        data = read_response_from_s3(bucket_name, fred_s3_key)
        if not data:
            logging.error("Failed to read FRED data from S3")
            return None
        
        # Transform records
        logging.info("Transforming FRED macro data records")
        series_ids = config.get("fred_series_ids", [])
        processed_records = []
        
        for series_id in series_ids:
            units = data.get(series_id, {}).get("units")
            observations = data.get(series_id, {}).get("observations", [])
            logging.debug(f"Processing FRED series '{series_id}' with {len(observations)} observations")

            for obs in observations:
                fred_records = {}
                fred_records["series_id"] = series_id
                fred_records["observation_date"] = obs.get("date")
                fred_records["observation_values"] = obs.get("value")
                fred_records["units"] = units

                try:
                    model_instance = FredDataModel(**fred_records)
                    processed_records.append(model_instance.model_dump(mode="json"))
                except Exception as e:
                    logging.warning(f"[WARN] Skipping observation for series '{series_id}': {e}")

        logging.info(f"Successfully transformed {len(processed_records)} FRED data records")
        
        # Upload to silver layer
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        s3_key = f"{config['silver_prefix']}/{timestamp}/processed_fred_data-{timestamp}.json"
        
        logging.info(f"Uploading transformed data to silver layer: {s3_key}")
        upload_success = upload_response_to_s3(processed_records, bucket_name, s3_key)
        
        if not upload_success:
            logging.error("[ERROR] Failed to upload processed FRED data to S3")
            return None
        else:
            logging.info(f"[SUCCESS] FRED data transformation complete: {s3_key}")
            return s3_key
            
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error in FRED data transformation: {e}", exc_info=True)
        raise

# print(transform_fred_data("bronze/2026-04-08 10:00:00/fred_data-2026-04-08 10:00:00.json"))

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def transform_news(news_s3_key: str) -> Optional[str]:
    """Transform raw news data from bronze to silver layer.
    
    Args:
        news_s3_key: S3 path to raw bronze news data
        
    Returns:
        S3 path to processed silver layer data
        Returns None if transformation fails
    """
    logging.info(f"[TRANSFORM] Starting news data transformation from: {news_s3_key}")
    
    try:
        # Read bronze layer data
        logging.debug("Reading raw news data from S3")
        data = read_response_from_s3(bucket_name, news_s3_key)
        if not data:
            logging.error("Failed to read news data from S3")
            return None
        
        # Transform records
        logging.info("Transforming news records")
        keywords = config.get("news_keywords", [])
        processed_records = []
        
        for keyword in keywords:
            news_data = data.get(keyword, {})
            total_mentions = news_data.get("totalResults", 0)
            status = news_data.get("status", "unknown")
            
            logging.debug(f"Processing keyword '{keyword}': {total_mentions} mentions")

            news_records = {
                "news_timestamp": datetime.datetime.now(),
                "keywords": keyword,
                "total_mentions": total_mentions,
                "status": status
            }

            try:
                model_instance = NewsCountModel(**news_records)
                processed_records.append(model_instance.model_dump(mode="json"))
            except Exception as e:
                logging.warning(f"[WARN] Skipping keyword '{keyword}': {e}")

        logging.info(f"Successfully transformed {len(processed_records)} news records")
        
        # Upload to silver layer
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:00:00")
        s3_key = f"{config['silver_prefix']}/{timestamp}/processed_news_data-{timestamp}.json"
        
        logging.info(f"Uploading transformed data to silver layer: {s3_key}")
        upload_success = upload_response_to_s3(processed_records, bucket_name, s3_key)
        
        if not upload_success:
            logging.error("[ERROR] Failed to upload processed news data to S3")
            return None
        else:
            logging.info(f"[SUCCESS] News data transformation complete: {s3_key}")
            return s3_key
            
    except Exception as e:
        logging.error(f"[ERROR] Unexpected error in news data transformation: {e}", exc_info=True)
        raise
# print(transform_news("bronze/2026-04-08 11:00:00/news_data-2026-04-08 11:00:00.json"))
