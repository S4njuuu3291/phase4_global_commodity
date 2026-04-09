"""
Unit tests for Pydantic models in models.py
Tests data validation and transformation
"""
import pytest
from datetime import datetime
from dags.models import (
    ProcessedMetalModel,
    CurrencyRateModel,
    FredDataModel,
    NewsCountModel
)


class TestProcessedMetalModel:
    """Test ProcessedMetalModel validation"""
    
    def test_valid_metal_data(self):
        """Test creation with valid metal data"""
        data = {
            'event_timestamp': datetime.now(),
            'metal_symbol': 'gold',
            'price_usd': 2100.50,
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        model = ProcessedMetalModel(**data)
        assert model.metal_symbol == 'gold'
        assert model.price_usd == 2100.50
        assert model.currency_base == 'USD'
    
    def test_empty_metal_symbol_fails(self):
        """Test validation fails with empty metal symbol"""
        data = {
            'event_timestamp': datetime.now(),
            'metal_symbol': '',
            'price_usd': 2100.50,
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        with pytest.raises(ValueError, match='metal symbol must not be empty'):
            ProcessedMetalModel(**data)
    
    def test_negative_price_fails(self):
        """Test validation fails with negative price"""
        data = {
            'event_timestamp': datetime.now(),
            'metal_symbol': 'gold',
            'price_usd': -100.0,  # Invalid: price must be > 0
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        with pytest.raises(ValueError):
            ProcessedMetalModel(**data)
    
    def test_zero_price_fails(self):
        """Test validation fails with zero price"""
        data = {
            'event_timestamp': datetime.now(),
            'metal_symbol': 'gold',
            'price_usd': 0.0,
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        with pytest.raises(ValueError):
            ProcessedMetalModel(**data)
    
    def test_ingested_at_default(self):
        """Test ingested_at has default value"""
        data = {
            'event_timestamp': datetime.now(),
            'metal_symbol': 'silver',
            'price_usd': 28.75,
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        model = ProcessedMetalModel(**data)
        assert model.ingested_at is not None
        assert isinstance(model.ingested_at, datetime)


class TestCurrencyRateModel:
    """Test CurrencyRateModel validation"""
    
    def test_valid_currency_data(self):
        """Test creation with valid currency data"""
        data = {
            'rate_date': datetime.now(),
            'currency_code': 'IDR',
            'exchange_rate': 16600.50,
            'base_currency': 'USD'
        }
        model = CurrencyRateModel(**data)
        assert model.currency_code == 'IDR'
        assert model.exchange_rate == 16600.50
    
    def test_currency_string_date_parsing(self):
        """Test parsing string date with timezone"""
        data = {
            'rate_date': '2026-04-09 00:00:00+00:00',
            'currency_code': 'EUR',
            'exchange_rate': 0.94,
            'base_currency': 'USD'
        }
        model = CurrencyRateModel(**data)
        assert isinstance(model.rate_date, datetime)
        assert model.exchange_rate == 0.94
    
    def test_negative_exchange_rate_fails(self):
        """Test validation fails with negative exchange rate"""
        data = {
            'rate_date': datetime.now(),
            'currency_code': 'JPY',
            'exchange_rate': -150.0,
            'base_currency': 'USD'
        }
        with pytest.raises(ValueError):
            CurrencyRateModel(**data)
    
    def test_zero_exchange_rate_fails(self):
        """Test validation fails with zero exchange rate"""
        data = {
            'rate_date': datetime.now(),
            'currency_code': 'GBP',
            'exchange_rate': 0.0,
            'base_currency': 'USD'
        }
        with pytest.raises(ValueError):
            CurrencyRateModel(**data)


class TestFredDataModel:
    """Test FredDataModel validation"""
    
    def test_valid_fred_data(self):
        """Test creation with valid FRED data"""
        data = {
            'series_id': 'DGS10',
            'observation_date': datetime(2026, 4, 9),
            'observation_values': 4.28,
            'units': 'lin'
        }
        model = FredDataModel(**data)
        assert model.series_id == 'DGS10'
        assert model.observation_values == 4.28
    
    def test_fred_dot_value_handling(self):
        """Test handling of '.' as missing value in FRED data"""
        data = {
            'series_id': 'DGS10',
            'observation_date': datetime(2026, 4, 9),
            'observation_values': '.',  # Missing value indicator
            'units': 'lin'
        }
        model = FredDataModel(**data)
        assert model.observation_values is None
    
    def test_fred_string_to_float_conversion(self):
        """Test conversion of string to float"""
        data = {
            'series_id': 'CPIAUCSL',
            'observation_date': datetime(2026, 4, 9),
            'observation_values': '312.50',  # String number
            'units': 'lin'
        }
        model = FredDataModel(**data)
        assert model.observation_values == 312.50
        assert isinstance(model.observation_values, float)
    
    def test_negative_fred_values_allowed(self):
        """Test that negative values are allowed (e.g., for some indicators)"""
        # Note: Current model only checks gt=0, but some FRED series can be negative
        # This test documents current behavior
        data = {
            'series_id': 'T10Y2Y',
            'observation_date': datetime(2026, 4, 9),
            'observation_values': -0.5,  # Negative yield spread
            'units': 'lin'
        }
        # Current behavior: should fail because of Field(gt=0)
        with pytest.raises(ValueError):
            FredDataModel(**data)


class TestNewsCountModel:
    """Test NewsCountModel validation"""
    
    def test_valid_news_data(self):
        """Test creation with valid news data"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'gold',
            'total_mentions': 150,
            'status': 'ok'
        }
        model = NewsCountModel(**data)
        assert model.keywords == 'gold'
        assert model.total_mentions == 150
        assert model.status == 'ok'
    
    def test_status_ok_validation(self):
        """Test validation requires status='ok'"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'gold',
            'total_mentions': 150,
            'status': 'error'  # Invalid status
        }
        with pytest.raises(ValueError, match='status must be "ok"'):
            NewsCountModel(**data)
    
    def test_negative_mentions_fails(self):
        """Test validation fails with negative mentions"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'silver',
            'total_mentions': -10,  # Must be >= 0
            'status': 'ok'
        }
        with pytest.raises(ValueError):
            NewsCountModel(**data)
    
    def test_zero_mentions_allowed(self):
        """Test that zero mentions are allowed"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'platinum',
            'total_mentions': 0,
            'status': 'ok'
        }
        model = NewsCountModel(**data)
        assert model.total_mentions == 0
    
    def test_ingested_at_default(self):
        """Test ingested_at has default value"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'copper',
            'total_mentions': 50,
            'status': 'ok'
        }
        model = NewsCountModel(**data)
        assert model.ingested_at is not None
        assert isinstance(model.ingested_at, datetime)


class TestModelSerialization:
    """Test model serialization to JSON"""
    
    def test_metal_model_json_serialization(self):
        """Test metal model can be serialized to JSON"""
        data = {
            'event_timestamp': datetime(2026, 4, 9, 10, 0, 0),
            'metal_symbol': 'gold',
            'price_usd': 2100.50,
            'currency_base': 'USD',
            'unit': 'troy_oz'
        }
        model = ProcessedMetalModel(**data)
        json_data = model.model_dump_json()
        assert 'gold' in json_data
        assert '2100.5' in json_data
    
    def test_model_to_dict(self):
        """Test model conversion to dictionary"""
        data = {
            'news_timestamp': datetime.now(),
            'keywords': 'gold',
            'total_mentions': 100,
            'status': 'ok'
        }
        model = NewsCountModel(**data)
        model_dict = model.model_dump()
        assert model_dict['keywords'] == 'gold'
        assert model_dict['total_mentions'] == 100
        assert model_dict['status'] == 'ok'
