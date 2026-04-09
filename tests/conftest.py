"""
Pytest configuration and shared fixtures with moto AWS mocking
"""
import pytest
import sys
import os
from datetime import datetime
from unittest.mock import MagicMock, patch
from moto import mock_aws
import boto3
import json

# Add dags folder and parent to path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dags_path = os.path.join(project_root, 'dags')
if project_root not in sys.path:
    sys.path.insert(0, project_root)
if dags_path not in sys.path:
    sys.path.insert(0, dags_path)


# ============================================================================
# MOCK CONFIG.YAML - Patch at conftest import time
# ============================================================================

# Patch config.yaml loading before any module imports
import yaml as yaml_module

MOCK_CONFIG_DATA = {
    'bucket_name': 'test-commodity-bucket',
    'bronze_prefix': 'bronze',
    'silver_prefix': 'silver',
    'api_url': {
        'metal': 'https://api.metals.dev/v1/latest',
        'currency': 'https://api.currencyfreaks.com/v2.0/rates/latest',
        'fred': 'https://api.stlouisfed.org/fred/series/observations',
        'news_api': 'https://newsapi.org/v2/everything'
    }
}

# Monkey-patch yaml.safe_load early
_original_safe_load = yaml_module.safe_load

def _mock_safe_load(stream=None):
    """Mock safe_load to return test config for config.yaml"""
    if hasattr(stream, 'name') and 'config.yaml' in stream.name:
        return MOCK_CONFIG_DATA
    return _original_safe_load(stream)

yaml_module.safe_load = _mock_safe_load


# ============================================================================
# SAMPLE API RESPONSES
# ============================================================================

@pytest.fixture
def sample_metal_response():
    """Sample metal prices API response"""
    return {
        'timestamp': '2026-04-09T10:00:00.000Z',
        'status': 'success',
        'metals': {
            'gold': 2100.50,
            'silver': 28.75,
            'platinum': 1050.30,
            'copper': 10.25,
            'nickel': 8.50
        },
        'currency_base': 'USD'
    }


@pytest.fixture
def sample_currency_response():
    """Sample currency rates API response"""
    return {
        'date': '2026-04-09 00:00:00+00:00',
        'base': 'USD',
        'rates': {
            'IDR': '16600.50',
            'EUR': '0.94',
            'JPY': '150.25',
            'GBP': '0.79',
            'AUD': '1.52',
            'CAD': '1.36',
            'CNY': '7.20',
            'SGD': '1.34'
        }
    }


@pytest.fixture
def sample_fred_response():
    """Sample FRED data API response"""
    return {
        'realtime_start': '2026-04-09',
        'realtime_end': '2026-04-09',
        'observation_start': '2026-04-01',
        'observation_end': '2026-04-09',
        'units': 'lin',
        'file_type': 'json',
        'observations': [
            {
                'realtime_start': '2026-04-09',
                'realtime_end': '2026-04-09',
                'date': '2026-04-07',
                'value': '4.28'
            },
            {
                'realtime_start': '2026-04-09',
                'realtime_end': '2026-04-09',
                'date': '2026-04-08',
                'value': '4.30'
            },
            {
                'realtime_start': '2026-04-09',
                'realtime_end': '2026-04-09',
                'date': '2026-04-09',
                'value': '4.26'
            }
        ]
    }


@pytest.fixture
def sample_news_response():
    """Sample news API response"""
    return {
        'status': 'ok',
        'totalResults': 150,
        'articles': [
            {
                'title': 'Gold prices surge',
                'description': 'Commodity market rally',
                'publishedAt': '2026-04-09T08:00:00Z'
            },
            {
                'title': 'Silver outlook positive',
                'description': 'Demand from solar industry',
                'publishedAt': '2026-04-09T07:30:00Z'
            }
        ]
    }


# ============================================================================
# AWS MOTO DECORATORS - Use as test decorators
# ============================================================================


@pytest.fixture
def setup_aws_environment(aws_s3_bucket, aws_ssm_secrets):
    """Setup AWS environment for tests"""
    yield


@pytest.fixture
@mock_aws
def aws_s3_bucket():
    """Create mock S3 bucket for testing"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-1'
    
    conn = boto3.resource('s3', region_name='ap-southeast-1')
    conn.create_bucket(
        Bucket='test-commodity-bucket',
        CreateBucketConfiguration={'LocationConstraint': 'ap-southeast-1'}
    )
    return 'test-commodity-bucket'


@pytest.fixture
@mock_aws
def aws_ssm_secrets():
    """Setup mock SSM parameters"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = 'ap-southeast-1'
    
    client = boto3.client('ssm', region_name='ap-southeast-1')
    
    # Metal datasource parameters
    client.put_parameter(
        Name='/global-commodity/datasource/metal/url',
        Value='https://api.metals.dev/v1/latest',
        Type='String'
    )
    client.put_parameter(
        Name='/global-commodity/datasource/metal/api_key',
        Value='test-metal-key',
        Type='SecureString'
    )
    
    # Currency datasource parameters
    client.put_parameter(
        Name='/global-commodity/datasource/currency/url',
        Value='https://api.currencyfreaks.com/v2.0/rates/latest',
        Type='String'
    )
    client.put_parameter(
        Name='/global-commodity/datasource/currency/api_key',
        Value='test-currency-key',
        Type='SecureString'
    )
    
    # FRED datasource parameters
    client.put_parameter(
        Name='/global-commodity/datasource/fred/url',
        Value='https://api.stlouisfed.org/fred/series/observations',
        Type='String'
    )
    client.put_parameter(
        Name='/global-commodity/datasource/fred/api_key',
        Value='test-fred-key',
        Type='SecureString'
    )
    
    # News datasource parameters
    client.put_parameter(
        Name='/global-commodity/datasource/news_api/url',
        Value='https://newsapi.org/v2/everything',
        Type='String'
    )
    client.put_parameter(
        Name='/global-commodity/datasource/news_api/api_key',
        Value='test-news-key',
        Type='SecureString'
    )
    
    return client
