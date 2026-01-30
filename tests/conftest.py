"""
Shared pytest fixtures untuk integration tests
"""
import pytest
from unittest.mock import MagicMock, patch
import json


@pytest.fixture
def sample_ndjson_data():
    """Sample NDJSON data untuk testing GCS to BQ"""
    data = {
        "timestamp": "2026-01-30T10:00:00",
        "metals": {
            "status": "success",
            "timestamp": "2026-01-30T10:00:00",
            "currency_base": "USD",
            "metals": {
                "gold": 75.50,
                "silver": 28.30,
                "platinum": 1050.00,
                "nickel": 8.50,
                "copper": 9.25
            }
        },
        "currency": {
            "rates": {
                "IDR": 15850.00,
                "EUR": 0.92,
                "CNY": 7.25,
                "JPY": 150.50
            }
        },
        "macro": {
            "DGS10": {
                "count": 1,
                "observations": [{"date": "2026-01-30", "value": "4.25"}]
            },
            "DTWEXBGS": {
                "count": 1,
                "observations": [{"date": "2026-01-30", "value": "125.50"}]
            },
            "CPIAUCSL": {
                "count": 1,
                "observations": [{"date": "2026-01-30", "value": "310.50"}]
            }
        },
        "news": {
            "gold": 125,
            "silver": 95,
            "platinum": 45,
            "nickel": 32,
            "copper": 78
        }
    }
    return json.dumps(data)


@pytest.fixture
def mock_gcs_hook():
    """Mock GCSHook untuk testing"""
    with patch("dags.dags.GCSHook") as mock:
        yield mock


@pytest.fixture
def mock_bigquery_client():
    """Mock BigQuery client"""
    with patch("google.cloud.bigquery.Client") as mock:
        mock_client = MagicMock()
        mock.return_value = mock_client
        yield mock_client


@pytest.fixture
def mock_bash_operator():
    """Mock BashOperator untuk testing"""
    with patch("airflow.operators.bash.BashOperator") as mock:
        yield mock


@pytest.fixture
def mock_subprocess_run():
    """Mock subprocess.run untuk mocking bash commands"""
    with patch("subprocess.run") as mock:
        yield mock


@pytest.fixture
def airflow_context():
    """Sample Airflow context untuk testing"""
    return {
        "dag": MagicMock(dag_id="global_commodity"),
        "task": MagicMock(task_id="test_task"),
        "execution_date": "2026-01-30T00:00:00+00:00",
        "ti": MagicMock(xcom_push=MagicMock(), xcom_pull=MagicMock()),
    }


@pytest.fixture
def gcs_file_path():
    """Sample GCS file path"""
    return "global_commodity/date=2026-01-30/commodity_data.json"


@pytest.fixture
def bigquery_table():
    """Sample BigQuery table reference"""
    return "project-global-commodityy.dwh_commodity.commodity_data"
