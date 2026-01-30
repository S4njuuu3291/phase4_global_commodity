"""
Test suite for extract_val_load_gcs task in global_commodity DAG

This tests the core ETL logic:
1. Fetch from 4 APIs
2. Validate each response
3. Transform & combine data
4. Upload to GCS
"""
from unittest.mock import patch, MagicMock
import pytest
import pendulum


@pytest.fixture
def mock_keys():
    """Sample API keys dict"""
    return {
        "metal_key": "test_metal_key",
        "currency_key": "test_currency_key",
        "fred_key": "test_fred_key",
        "news_key": "test_news_key",
    }


@pytest.fixture
def valid_metal_data():
    """Valid metal prices response"""
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


@pytest.fixture
def valid_currency_data():
    """Valid currency rates response"""
    return {
        "date": "2026-01-30",
        "base": "USD",
        "rates": {
            "IDR": 16663.0,
            "EUR": 0.9415,
            "CNY": 7.2025
        }
    }


@pytest.fixture
def valid_fred_data():
    """Valid FRED data response"""
    return {
        "realtime_start": "2026-01-30",
        "realtime_end": "2026-01-30",
        "observation_start": "2026-01-23",
        "observation_end": "2026-01-30",
        "units": "lin",
        "file_type": "json",
        "order_by": "observation_date",
        "sort_order": "asc",
        "count": 4,
        "offset": 0,
        "limit": 100000,
        "observations": [
            {
                "realtime_start": "2026-01-30",
                "realtime_end": "2026-01-30",
                "date": "2026-01-23",
                "value": "3.5"
            },
            {
                "realtime_start": "2026-01-30",
                "realtime_end": "2026-01-30",
                "date": "2026-01-30",
                "value": "3.6"
            }
        ]
    }


@pytest.fixture
def valid_news_data():
    """Valid news count response"""
    return {
        "timestamp": "2026-01-30",
        "gold": 150,
        "silver": 120,
        "platinum": 80,
        "copper": 60,
        "nickel": 40,
    }


# ===========================
# TEST 1: HAPPY PATH - SUCCESS
# ===========================

@patch('dags.dags.pendulum')
@patch('dags.dags.GCSHook')
@patch('dags.dags.fetch_news_count')
@patch('dags.dags.fetch_fred_data')
@patch('dags.dags.fetch_currency_rate')
@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_success(
    mock_fetch_metal,
    mock_fetch_currency,
    mock_fetch_fred,
    mock_fetch_news,
    mock_gcs_hook_cls,
    mock_pendulum,
    mock_keys,
    valid_metal_data,
    valid_currency_data,
    valid_fred_data,
    valid_news_data,
):
    """
    Test happy path: All APIs return valid data, transformation successful,
    GCS upload successful
    """
    # Setup all mocks to return valid data
    mock_fetch_metal.return_value = valid_metal_data
    mock_fetch_currency.return_value = valid_currency_data
    
    # FRED returns dict with 3 series
    mock_fetch_fred.return_value = {
        "DGS10": valid_fred_data,
        "DTWEXBGS": valid_fred_data,
        "CPIAUCSL": valid_fred_data,
    }
    
    mock_fetch_news.return_value = valid_news_data

    # Mock GCS hook
    mock_gcs_instance = MagicMock()
    mock_gcs_hook_cls.return_value = mock_gcs_instance

    # Mock pendulum to return consistent date
    mock_now = MagicMock()
    mock_now.to_datetime_string.return_value = "2026-01-30T10:00:00+00:00"
    mock_now.to_date_string.return_value = "2026-01-30"
    mock_pendulum.now.return_value = mock_now

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    result = task_func(mock_keys)

    # Assert
    assert result == "global_commodity/date=2026-01-30/commodity_data.json"
    
    # Verify all APIs were called
    mock_fetch_metal.assert_called_once_with(mock_keys["metal_key"])
    mock_fetch_currency.assert_called_once_with(mock_keys["currency_key"])
    mock_fetch_fred.assert_called_once_with(mock_keys["fred_key"])
    mock_fetch_news.assert_called_once()
    
    # Verify GCS upload was called
    mock_gcs_instance.upload.assert_called_once()
    call_args = mock_gcs_instance.upload.call_args
    assert call_args.kwargs["object_name"] == "global_commodity/date=2026-01-30/commodity_data.json"
    assert call_args.kwargs["mime_type"] == "application/json"


# ===========================
# TEST 2: METAL PRICES FAIL
# ===========================

@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_metal_fails(
    mock_fetch_metal,
    mock_keys,
):
    """
    Test failure: Metal prices API returns failed status
    Expected: ValueError raised, task stops immediately
    """
    # Setup metal to return failed status
    mock_fetch_metal.return_value = {
        "status": "failed"
    }

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    # Assert
    with pytest.raises(ValueError) as exc:
        task_func(mock_keys)
    
    assert "Failed to fetch metal prices" in str(exc.value)
    mock_fetch_metal.assert_called_once()


# ===========================
# TEST 3: CURRENCY RATE MISSING IDR
# ===========================

@patch('dags.dags.fetch_currency_rate')
@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_currency_missing_idr(
    mock_fetch_metal,
    mock_fetch_currency,
    mock_keys,
    valid_metal_data,
):
    """
    Test failure: Currency rates response missing IDR
    Expected: ValueError raised, task stops
    """
    # Metal OK
    mock_fetch_metal.return_value = valid_metal_data
    
    # Currency missing IDR
    mock_fetch_currency.return_value = {
        "date": "2026-01-30",
        "base": "USD",
        "rates": {
            "EUR": 0.9415,
            "CNY": 7.2025
            # ← No IDR!
        }
    }

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    # Assert
    with pytest.raises(ValueError) as exc:
        task_func(mock_keys)
    
    assert "Failed to fetch currency rates" in str(exc.value)
    mock_fetch_metal.assert_called_once()
    mock_fetch_currency.assert_called_once()


# ===========================
# TEST 4: FRED DATA EMPTY
# ===========================

@patch('dags.dags.fetch_fred_data')
@patch('dags.dags.fetch_currency_rate')
@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_fred_empty(
    mock_fetch_metal,
    mock_fetch_currency,
    mock_fetch_fred,
    mock_keys,
    valid_metal_data,
    valid_currency_data,
):
    """
    Test failure: FRED data has count < 1
    Expected: ValueError raised, task stops
    """
    # Metal & Currency OK
    mock_fetch_metal.return_value = valid_metal_data
    mock_fetch_currency.return_value = valid_currency_data
    
    # FRED with empty data (count = 0)
    mock_fetch_fred.return_value = {
        "DGS10": {
            "count": 0,  # ← EMPTY!
            "observations": []
        },
        "DTWEXBGS": {
            "count": 0,
            "observations": []
        },
        "CPIAUCSL": {
            "count": 0,
            "observations": []
        },
    }

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    # Assert
    with pytest.raises(ValueError) as exc:
        task_func(mock_keys)
    
    assert "Failed to fetch FRED data" in str(exc.value)


# ===========================
# TEST 5: NEWS COUNT FAILS
# ===========================

@patch('dags.dags.fetch_news_count')
@patch('dags.dags.fetch_fred_data')
@patch('dags.dags.fetch_currency_rate')
@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_news_fails(
    mock_fetch_metal,
    mock_fetch_currency,
    mock_fetch_fred,
    mock_fetch_news,
    mock_keys,
    valid_metal_data,
    valid_currency_data,
    valid_fred_data,
):
    """
    Test failure: News count API returns None (failed)
    Expected: ValueError raised, task stops
    """
    # Metal, Currency, FRED OK
    mock_fetch_metal.return_value = valid_metal_data
    mock_fetch_currency.return_value = valid_currency_data
    mock_fetch_fred.return_value = {
        "DGS10": valid_fred_data,
        "DTWEXBGS": valid_fred_data,
        "CPIAUCSL": valid_fred_data,
    }
    
    # News fails
    mock_fetch_news.return_value = None

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    # Assert
    with pytest.raises(ValueError) as exc:
        task_func(mock_keys)
    
    assert "Failed to fetch news count" in str(exc.value)


# ===========================
# TEST 6: GCS UPLOAD FAILS
# ===========================

@patch('dags.dags.pendulum')
@patch('dags.dags.GCSHook')
@patch('dags.dags.fetch_news_count')
@patch('dags.dags.fetch_fred_data')
@patch('dags.dags.fetch_currency_rate')
@patch('dags.dags.fetch_metal_prices')
def test_extract_val_load_gcs_gcs_upload_fails(
    mock_fetch_metal,
    mock_fetch_currency,
    mock_fetch_fred,
    mock_fetch_news,
    mock_gcs_hook_cls,
    mock_pendulum,
    mock_keys,
    valid_metal_data,
    valid_currency_data,
    valid_fred_data,
    valid_news_data,
):
    """
    Test failure: GCS upload raises exception
    Expected: Exception propagated to caller
    """
    # All APIs OK
    mock_fetch_metal.return_value = valid_metal_data
    mock_fetch_currency.return_value = valid_currency_data
    mock_fetch_fred.return_value = {
        "DGS10": valid_fred_data,
        "DTWEXBGS": valid_fred_data,
        "CPIAUCSL": valid_fred_data,
    }
    mock_fetch_news.return_value = valid_news_data

    # GCS upload fails
    mock_gcs_instance = MagicMock()
    mock_gcs_hook_cls.return_value = mock_gcs_instance
    mock_gcs_instance.upload.side_effect = Exception("Connection timeout to GCS")

    # Mock pendulum
    mock_now = MagicMock()
    mock_now.to_datetime_string.return_value = "2026-01-30T10:00:00+00:00"
    mock_now.to_date_string.return_value = "2026-01-30"
    mock_pendulum.now.return_value = mock_now

    # Execute
    from dags.dags import global_commodity_dag
    dag = global_commodity_dag()
    task_func = dag.task_dict['extract_val_load_gcs'].python_callable

    # Assert
    with pytest.raises(Exception) as exc:
        task_func(mock_keys)
    
    assert "Connection timeout to GCS" in str(exc.value)
