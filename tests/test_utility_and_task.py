from unittest.mock import MagicMock, patch

from openai import api_key
from dags.utils import fetch_currency_rate, get_secret, fetch_metal_prices, fetch_fred_data, fetch_news_count, to_ndjson, sanitize_keys
from dags.dags import get_all_keys
from dags.models import ProcessedMetalModel,CurrencyRateModel,FredDataModel, NewsCountModel
from dags.exceptions import SecretManagerError
import pytest
from pytest_httpx import HTTPXMock
import yaml
import datetime
config = yaml.safe_load(open("dags/config.yaml"))

@patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_get_secret(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    mock_response = MagicMock()
    mock_response.payload.data.decode.return_value = "mocked_secret_value"
    mock_client.access_secret_version.return_value = mock_response

    secret_name = "test_secret"
    project_id = "test_project"
    secret_value = get_secret(secret_name, project_id)
    assert secret_value == "mocked_secret_value"
    mock_client.access_secret_version.assert_called_once_with(
        request={"name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"}
    )
# Kode diatas adalah unit test untuk fungsi get_secret di utils.py yang menggunakan mocking untuk mensimulasikan interaksi dengan Google Cloud Secret Manager. Alur nya adalah sebagai berikut:
# 1. Patch kelas SecretManagerServiceClient untuk menggantinya dengan mock.
# 2. Buat instance mock dari client dan atur return value untuk method access_secret_version agar mengembalikan mock response yang berisi secret yang diinginkan.
# 3. Panggil fungsi get_secret dengan nama secret dan project_id yang diinginkan 
# 4. Verifikasi bahwa nilai secret yang dikembalikan sesuai dengan yang diharapkan dan bahwa method access_secret_version dipanggil dengan parameter yang benar.

@patch("google.cloud.secretmanager.SecretManagerServiceClient")
def test_get_secret_fail(mock_client_cls):
    mock_client = MagicMock()
    mock_client_cls.return_value = mock_client

    mock_client.access_secret_version.side_effect = Exception("Secret not found")

    secret_name = "non_existent_secret"
    project_id = "test_project"

    with pytest.raises(SecretManagerError) as e:
        get_secret(secret_name, project_id)
    assert str(e.value) == f"Failed to fetch secret '{secret_name}' from project '{project_id}': Secret not found"
    mock_client.access_secret_version.assert_called_once_with(
        request={"name": f"projects/{project_id}/secrets/{secret_name}/versions/latest"}
        )

def test_get_all_keys_success():
    with patch('dags.dags.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = [
            'mocked_metal_key',
            'mocked_currency_key',
            'mocked_fred_key',
            'mocked_news_key'
        ]

        keys = get_all_keys()

        assert keys == {
            "metal_key": 'mocked_metal_key',
            "currency_key": 'mocked_currency_key',
            "fred_key": 'mocked_fred_key',
            "news_key": 'mocked_news_key',
        }
        assert mock_get_secret.call_count == 4

def test_get_all_keys_failure():
    with patch('dags.dags.get_secret') as mock_get_secret:
        mock_get_secret.side_effect = Exception("Secret not found")

        with pytest.raises(Exception) as e:
            get_all_keys()
        assert str(e.value) == "Secret not found"
        assert mock_get_secret.call_count == 1

@pytest.fixture
def sample_metals():
    return {
    "status": "success",
    "currency": "USD",
    "unit": "g",
    "metals": {
        "gold": 178.0129,
        "silver": 3.7662,
        "platinum": 87.7146,
        "copper": 0.014,
        "nickel": 0.019,
    },
    "timestamps": {
        "metal": "2026-01-29T08:21:02.932Z",
        "currency": "2026-01-29T08:19:16.890Z"
    }
}

# ===========================
# METAL MODEL TESTS
# ===========================

@pytest.fixture
def processed_metal():
    return {
        "timestamp": "2026-01-29T08:21:02.932Z",
        "status": "success",
        "metals": {
            "gold": 178.0129,
            "silver": 3.7662,
            "platinum": 87.7146,
            "copper": 0.014,
            "nickel": 0.019,
        },
        "currency_base": "USD"
    }

def test_processed_metal_model(processed_metal):
    model = ProcessedMetalModel(**processed_metal).model_dump()
    assert model["timestamp"] == "2026-01-29T08:21:02.932Z"
    assert model["status"] == "success"
    assert model["currency_base"] == "USD"
    assert model["metals"]["gold"] == 178.0129
    assert model["metals"]["silver"] == 3.7662
    assert model["metals"]["platinum"] == 87.7146
    assert model["metals"]["copper"] == 0.014
    assert model["metals"]["nickel"] == 0.019

def test_processed_metal_model_invalid_negative_value(processed_metal):
    processed_metal["metals"]["gold"] = -100.0
    with pytest.raises(ValueError) as e:
        ProcessedMetalModel(**processed_metal)
    assert "greater than" in str(e.value)

def test_processed_metal_model_missing_field(processed_metal):
    del processed_metal["currency_base"]
    with pytest.raises(ValueError) as e:
        ProcessedMetalModel(**processed_metal)
    assert "field required" in str(e.value).lower()

def test_processed_metal_model_invalid_type(processed_metal):
    processed_metal["metals"]["silver"] = "not_a_float"
    with pytest.raises(ValueError) as e:
        ProcessedMetalModel(**processed_metal)
    assert "valid number" in str(e.value).lower()

def test_processed_metal_model_empty_metals(processed_metal):
    processed_metal["metals"] = {}
    with pytest.raises(ValueError) as e:
        ProcessedMetalModel(**processed_metal)
    assert "must not be empty" in str(e.value).lower()

def test_fetch_metal_prices_success(httpx_mock: HTTPXMock, sample_metals):
    api_key = "test_api_key"
    url = f"{config['api_url']['metal']}?api_key={api_key}&currency=USD&unit=g"
    httpx_mock.add_response(
        method="GET",
        url=url,
        json=sample_metals,
        status_code=200
    )
    result = fetch_metal_prices(api_key)
    assert result["status"] == "success"
    assert result["currency_base"] == "USD"
    assert "gold" in result["metals"]
    assert "silver" in result["metals"]
    assert "platinum" in result["metals"]
    assert "copper" in result["metals"]
    assert "nickel" in result["metals"]

def test_fetch_metal_prices_failure(httpx_mock: HTTPXMock):
    api_key = "invalid_api_key"
    url = f"{config['api_url']['metal']}?api_key={api_key}&currency=USD&unit=g"
    httpx_mock.add_response(
        method="GET",
        url=url,
        status_code=503
    )
    result = fetch_metal_prices(api_key)

    assert result is None

# ===========================
# CURRENCY RATE MODEL TESTS
# ===========================
@pytest.fixture
def currency_rate():
    return {
        "date": "2025-12-12 00:00:00+00",
        "base": "USD",
        "rates": {
            "IDR": 16663.0,
            "EUR": 0.9415,
            "CNY": 7.2025
        }
    }

def test_currency_rate_model(currency_rate):
    model = CurrencyRateModel(**currency_rate).model_dump()
    assert model["date"] == "2025-12-12 00:00:00+00"
    assert model["base"] == "USD"
    assert model["rates"]["IDR"] == 16663.0
    assert model["rates"]["EUR"] == 0.9415
    assert model["rates"]["CNY"] == 7.2025

def test_currency_rate_model_invalid_negative_value(currency_rate):
    currency_rate["rates"]["EUR"] = -0.5
    with pytest.raises(ValueError) as e:
        CurrencyRateModel(**currency_rate)
    assert "greater than" in str(e.value)

def test_currency_rate_model_missing_field(currency_rate):
    del currency_rate["base"]
    with pytest.raises(ValueError) as e:
        CurrencyRateModel(**currency_rate)
    assert "field required" in str(e.value).lower()

def test_currency_rate_model_invalid_type(currency_rate):
    currency_rate["rates"]["CNY"] = "not_a_float"
    with pytest.raises(ValueError) as e:
        CurrencyRateModel(**currency_rate)
    assert "valid number" in str(e.value).lower()

def test_currency_rate_model_empty_rates(currency_rate):
    currency_rate["rates"] = {}
    with pytest.raises(ValueError) as e:
        CurrencyRateModel(**currency_rate)
    assert "must not be empty" in str(e.value).lower()

def test_fetch_currency_rate_success(httpx_mock: HTTPXMock, currency_rate):
    api_key = "test_currency_api_key"
    url = f"{config['api_url']['currency']}?apikey={api_key}"

    httpx_mock.add_response(
        method="GET",
        url=url,
        json=currency_rate,
        status_code=200
    )
    result = fetch_currency_rate(api_key)
    assert result["base"] == "USD"
    assert result["rates"]["IDR"] == 16663.0
    assert result["rates"]["EUR"] == 0.9415
    assert result["rates"]["CNY"] == 7.2025

def test_fetch_currency_rate_failure(httpx_mock: HTTPXMock):
    api_key = "invalid_currency_api_key"
    url = f"{config['api_url']['currency']}?apikey={api_key}"
    httpx_mock.add_response(
        method="GET",
        url=url,
        status_code=404
    )
    result = fetch_currency_rate(api_key)
    assert result is None

# ===========================
# FRED DATA MODEL TESTS
# ===========================
@pytest.fixture
def fred_data():
    return {
      "realtime_start": "2026-01-28",
      "realtime_end": "2026-01-28",
      "observation_start": "2026-01-22",
      "observation_end": "2026-01-29",
      "units": "lin",
      "output_type": 1,
      "file_type": "json",
      "order_by": "observation_date",
      "sort_order": "asc",
      "count": 4,
      "offset": 0,
      "limit": 100000,
      "observations": [
        {
          "realtime_start": "2026-01-28",
          "realtime_end": "2026-01-28",
          "date": "2026-01-22",
          "value": "3.5"
        },
        {
          "realtime_start": "2026-01-28",
          "realtime_end": "2026-01-28",
          "date": "2026-01-23",
          "value": "3.6"
        }
      ]
    }

def test_fred_data_model(fred_data):
    model = FredDataModel(**fred_data).model_dump()
    assert model["realtime_start"] == "2026-01-28"
    assert model["realtime_end"] == "2026-01-28"
    assert model["observation_start"] == "2026-01-22"
    assert model["observation_end"] == "2026-01-29"
    assert model["units"] == "lin"
    assert model["file_type"] == "json"
    assert model["order_by"] == "observation_date"
    assert model["sort_order"] == "asc"
    assert model["count"] == 4
    assert len(model["observations"]) == 2
    assert model["observations"][0]["date"] == "2026-01-22"
    assert model["observations"][0]["value"] == "3.5"
    assert model["observations"][1]["date"] == "2026-01-23"
    assert model["observations"][1]["value"] == "3.6"

def test_fred_data_model_invalid_negative_count(fred_data):
    fred_data["count"] = -1
    with pytest.raises(ValueError) as e:
        FredDataModel(**fred_data)
    assert "greater than" in str(e.value)

def test_fred_data_model_missing_field(fred_data):
    del fred_data["observations"]
    with pytest.raises(ValueError) as e:
        FredDataModel(**fred_data)
    assert "field required" in str(e.value).lower()

def test_fetch_fred_data_success(httpx_mock: HTTPXMock, fred_data):
    api_key = "test_fred_api_key"
    series_ids = config["fred_series_ids"]

    now = datetime.datetime.now().date()
    week_ago = now - datetime.timedelta(days=7)

    for series_id in series_ids:
        if series_id == "CPIAUCSL":
            week_ago = now - datetime.timedelta(days=180)
        httpx_mock.add_response(
            method="GET",
            url=f"{config['api_url']['fred']}?series_id={series_id}&api_key={api_key}&file_type=json&observation_start={week_ago}&observation_end={now}",
            json=fred_data,
            status_code=200
        )
    result = fetch_fred_data(api_key)
    for series_id in series_ids:
        assert result[series_id]["count"] == 4
        assert len(result[series_id]["observations"]) == 2
    
def test_fetch_fred_data_failure(httpx_mock: HTTPXMock):
    api_key = "invalid_fred_api_key"
    now = datetime.datetime.now().date()
    week_ago = now - datetime.timedelta(days=7)

    httpx_mock.add_response(
        method="GET",
        url=f"{config['api_url']['fred']}?series_id=DGS10&api_key={api_key}&file_type=json&observation_start={week_ago}&observation_end={now}",
        status_code=500
    )
    result = fetch_fred_data(api_key)
    assert result is None

# ===========================
# NEWS COUNT MODEL TESTS
# ===========================
@pytest.fixture
def news_count_model():
    return {
        "status": "ok",
        "totalResults": 150
    }

@pytest.fixture
def news_count():
    return {
        "timestamp": "2026-01-29",
        "gold": 150,
        "silver": 120,
        "platinum": 80,
        "copper": 60,
        "nickel": 40,
    }

def test_news_count_model(news_count_model):
    model = NewsCountModel(**news_count_model).model_dump()
    assert model["status"] == "ok"
    assert model["totalResults"] == 150

def test_news_count_model_invalid_status(news_count_model):
    news_count_model["status"] = "error"
    with pytest.raises(ValueError) as e:
        NewsCountModel(**news_count_model)
    assert 'status must be "ok"' in str(e.value)

def test_news_count_model_negative_total_results(news_count_model):
    news_count_model["totalResults"] = -10
    with pytest.raises(ValueError) as e:
        NewsCountModel(**news_count_model)
    assert "greater than" in str(e.value)

def test_fetch_news_count_success(httpx_mock: HTTPXMock, news_count_model):
    api_key = "test_news_api_key"
    keywords = config["news_keywords"]
    now = datetime.datetime.now().date()
    for keyword in keywords:
        url = f"{config['api_url']['news_api']}?q={keyword}&from={now}&to={now}&apiKey={api_key}"
        httpx_mock.add_response(
            method="GET",
            url=url,
            json=news_count_model,
            status_code=200
        )
    result = fetch_news_count(api_key, keywords)
    assert result["timestamp"] == datetime.datetime.now().strftime("%Y-%m-%d")
    for keyword in keywords:
        assert result[keyword] == 150
    
def test_fetch_news_count_failure(httpx_mock: HTTPXMock):
    api_key = "invalid_news_api_key"
    keywords = config["news_keywords"]
    now = datetime.datetime.now().date()
    keyword = keywords[0]
    url = f"{config['api_url']['news_api']}?q={keyword}&from={now}&to={now}&apiKey={api_key}"
    httpx_mock.add_response(
        method="GET",
        url=url,
        status_code=403
    )
    result = fetch_news_count(api_key, keywords)
    assert result is None

@pytest.fixture
def commodity_data(processed_metal, currency_rate, fred_data):
    return {
        "timestamp": "2026-01-29 10:00:00",
        "metals": processed_metal,
        "currency": currency_rate,
        "macro": {
            "DGS10": fred_data,
            "DTWEXBGS": fred_data,
            "CPIAUCSL": fred_data,
        },
        "news": {
            "timestamp": "2026-01-29",
            "gold": 150,
            "silver": 120,
            "platinum": 80,
            "copper": 60,
            "nickel": 40,
        }
    }

def test_to_ndjson(commodity_data):
    ndjson_str = to_ndjson(commodity_data)
    lines = ndjson_str.strip().split("\n")
    assert len(lines) == 1
    import json
    record = json.loads(lines[0])
    assert record["timestamp"] == "2026-01-29 10:00:00"
    assert "metals" in record
    assert "currency" in record
    assert "macro" in record
    assert "news" in record

def test_sanitize_keys():
    raw_data = {
        "some Key!": 123,
        "another@Key#": {
            "nested Key$": 456
        },
        "list Key%": [
            {"item Key^": 789}
        ],
        "1stKey": "starts with digit"
    }
    sanitized = sanitize_keys(raw_data)
    assert "some_Key_" in sanitized
    assert "another_Key_" in sanitized
    assert "nested_Key_" in sanitized["another_Key_"]
    assert "list_Key_" in sanitized
    assert "item_Key_" in sanitized["list_Key_"][0]
    assert "_1stKey" in sanitized
