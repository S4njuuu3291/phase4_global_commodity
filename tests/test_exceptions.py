"""
Test suite untuk custom exceptions

Verifies bahwa semua exception classes:
- Inherit dari base class dengan benar
- Menghasilkan error messages yang deskriptif
- Menyimpan attributes dengan benar
"""
import pytest
from dags.exceptions import (
    CommodityDAGException,
    APIFetchError,
    ValidationError,
    GCSUploadError,
    SecretManagerError,
    DataTransformationError,
    ConfigurationError,
    RetryExhaustedError,
    TimeoutError as CommodityTimeoutError,
)


# ===========================
# TEST 1: EXCEPTION HIERARCHY
# ===========================

def test_all_exceptions_inherit_from_commodity_dag_exception():
    """Verify semua exceptions inherit dari CommodityDAGException"""
    exception_classes = [
        APIFetchError,
        ValidationError,
        GCSUploadError,
        SecretManagerError,
        DataTransformationError,
        ConfigurationError,
        RetryExhaustedError,
        CommodityTimeoutError,
    ]
    
    for exc_class in exception_classes:
        assert issubclass(exc_class, CommodityDAGException), \
            f"{exc_class.__name__} should inherit from CommodityDAGException"


def test_commodity_dag_exception_inherits_from_exception():
    """Verify CommodityDAGException inherits dari standard Exception"""
    assert issubclass(CommodityDAGException, Exception)


# ===========================
# TEST 2: APIFetchError
# ===========================

def test_api_fetch_error_without_status_code():
    """Test APIFetchError dengan hanya api_name dan message"""
    exc = APIFetchError(
        api_name="metal_prices",
        message="Connection timeout"
    )
    
    assert exc.api_name == "metal_prices"
    assert exc.message == "Connection timeout"
    assert exc.status_code is None
    assert "[metal_prices]" in str(exc)
    assert "Connection timeout" in str(exc)


def test_api_fetch_error_with_status_code():
    """Test APIFetchError dengan status_code"""
    exc = APIFetchError(
        api_name="currency_rates",
        message="Invalid API key",
        status_code=401
    )
    
    assert exc.api_name == "currency_rates"
    assert exc.message == "Invalid API key"
    assert exc.status_code == 401
    assert "[currency_rates]" in str(exc)
    assert "Invalid API key" in str(exc)
    assert "401" in str(exc)


def test_api_fetch_error_is_raiseable():
    """Test APIFetchError dapat di-raise dan di-catch"""
    with pytest.raises(APIFetchError) as exc_info:
        raise APIFetchError("fred_data", "Rate limit exceeded", 429)
    
    assert "fred_data" in str(exc_info.value)
    assert "429" in str(exc_info.value)


# ===========================
# TEST 3: ValidationError
# ===========================

def test_validation_error():
    """Test ValidationError dengan field dan reason"""
    exc = ValidationError(
        field="currency_rates.IDR",
        reason="Missing required field"
    )
    
    assert exc.field == "currency_rates.IDR"
    assert exc.reason == "Missing required field"
    assert "currency_rates.IDR" in str(exc)
    assert "Missing required field" in str(exc)


def test_validation_error_negative_value():
    """Test ValidationError untuk negative value"""
    exc = ValidationError(
        field="metal_price.gold",
        reason="Value must be positive, got -100.5"
    )
    
    assert exc.field == "metal_price.gold"
    assert "-100.5" in str(exc)


def test_validation_error_is_raiseable():
    """Test ValidationError dapat di-raise"""
    with pytest.raises(ValidationError) as exc_info:
        raise ValidationError("fred_data.count", "Count must be >= 1")
    
    assert "fred_data.count" in str(exc_info.value)


# ===========================
# TEST 4: GCSUploadError
# ===========================

def test_gcs_upload_error():
    """Test GCSUploadError dengan bucket dan object_name"""
    exc = GCSUploadError(
        bucket="my-bucket",
        object_name="global_commodity/date=2026-01-30/data.json",
        message="Permission denied"
    )
    
    assert exc.bucket == "my-bucket"
    assert exc.object_name == "global_commodity/date=2026-01-30/data.json"
    assert exc.message == "Permission denied"
    assert "gs://my-bucket/global_commodity" in str(exc)
    assert "Permission denied" in str(exc)


def test_gcs_upload_error_network_timeout():
    """Test GCSUploadError untuk network timeout"""
    exc = GCSUploadError(
        bucket="data-bucket",
        object_name="file.json",
        message="Network timeout after 30s"
    )
    
    assert "data-bucket" in str(exc)
    assert "Network timeout" in str(exc)


def test_gcs_upload_error_is_raiseable():
    """Test GCSUploadError dapat di-raise"""
    with pytest.raises(GCSUploadError) as exc_info:
        raise GCSUploadError(
            bucket="test-bucket",
            object_name="test/data.json",
            message="Bucket not found"
        )
    
    assert "test-bucket" in str(exc_info.value)
    assert "Bucket not found" in str(exc_info.value)


# ===========================
# TEST 5: SecretManagerError
# ===========================

def test_secret_manager_error():
    """Test SecretManagerError dengan secret_name dan project_id"""
    exc = SecretManagerError(
        secret_name="metal_price_api_key",
        project_id="my-project",
        message="Secret not found"
    )
    
    assert exc.secret_name == "metal_price_api_key"
    assert exc.project_id == "my-project"
    assert exc.message == "Secret not found"
    assert "metal_price_api_key" in str(exc)
    assert "my-project" in str(exc)
    assert "Secret not found" in str(exc)


def test_secret_manager_error_permission_denied():
    """Test SecretManagerError untuk permission denied"""
    exc = SecretManagerError(
        secret_name="news_api_key",
        project_id="prod-project",
        message="Permission denied to access secret"
    )
    
    assert "news_api_key" in str(exc)
    assert "prod-project" in str(exc)
    assert "Permission denied" in str(exc)


# ===========================
# TEST 6: DataTransformationError
# ===========================

def test_data_transformation_error_sanitize_keys():
    """Test DataTransformationError untuk sanitize_keys operation"""
    exc = DataTransformationError(
        operation="sanitize_keys",
        reason="Input is not a dictionary"
    )
    
    assert exc.operation == "sanitize_keys"
    assert exc.reason == "Input is not a dictionary"
    assert "sanitize_keys" in str(exc)
    assert "not a dictionary" in str(exc)


def test_data_transformation_error_to_ndjson():
    """Test DataTransformationError untuk to_ndjson operation"""
    exc = DataTransformationError(
        operation="to_ndjson",
        reason="Circular reference detected in data"
    )
    
    assert exc.operation == "to_ndjson"
    assert "to_ndjson" in str(exc)
    assert "Circular reference" in str(exc)


# ===========================
# TEST 7: ConfigurationError
# ===========================

def test_configuration_error():
    """Test ConfigurationError untuk missing config"""
    exc = ConfigurationError(
        config_item="GCP_PROJECT_ID",
        message="Environment variable not set"
    )
    
    assert exc.config_item == "GCP_PROJECT_ID"
    assert exc.message == "Environment variable not set"
    assert "GCP_PROJECT_ID" in str(exc)
    assert "not set" in str(exc)


def test_configuration_error_invalid_value():
    """Test ConfigurationError untuk invalid config value"""
    exc = ConfigurationError(
        config_item="RETRY_MAX_ATTEMPTS",
        message="Must be a positive integer, got '-1'"
    )
    
    assert "RETRY_MAX_ATTEMPTS" in str(exc)
    assert "-1" in str(exc)


# ===========================
# TEST 8: RetryExhaustedError
# ===========================

def test_retry_exhausted_error():
    """Test RetryExhaustedError dengan max_attempts"""
    last_error = Exception("Connection refused")
    
    exc = RetryExhaustedError(
        api_name="metal_prices",
        max_attempts=3,
        last_error=last_error
    )
    
    assert exc.api_name == "metal_prices"
    assert exc.max_attempts == 3
    assert exc.last_error is last_error
    assert "metal_prices" in str(exc)
    assert "3" in str(exc)
    assert "Connection refused" in str(exc)


def test_retry_exhausted_error_with_timeout():
    """Test RetryExhaustedError ketika last_error adalah timeout"""
    last_error = TimeoutError("Request timeout")
    
    exc = RetryExhaustedError(
        api_name="currency_rates",
        max_attempts=5,
        last_error=last_error
    )
    
    assert "currency_rates" in str(exc)
    assert "5" in str(exc)
    assert "Request timeout" in str(exc)


# ===========================
# TEST 9: TimeoutError (Custom)
# ===========================

def test_timeout_error():
    """Test TimeoutError untuk custom timeout"""
    exc = CommodityTimeoutError(
        operation="fetch_metal_prices",
        timeout_seconds=10
    )
    
    assert exc.operation == "fetch_metal_prices"
    assert exc.timeout_seconds == 10
    assert "fetch_metal_prices" in str(exc)
    assert "10" in str(exc)


def test_timeout_error_gcs_upload():
    """Test TimeoutError untuk GCS upload operation"""
    exc = CommodityTimeoutError(
        operation="gcs_upload",
        timeout_seconds=30
    )
    
    assert exc.operation == "gcs_upload"
    assert exc.timeout_seconds == 30
    assert "gcs_upload" in str(exc)
    assert "30" in str(exc)


# ===========================
# TEST 10: EXCEPTION CATCHING
# ===========================

def test_catch_specific_exception():
    """Test dapat catch exception secara specific"""
    try:
        raise APIFetchError("test_api", "Test error")
    except APIFetchError as e:
        assert e.api_name == "test_api"
        assert "Test error" in str(e)


def test_catch_base_exception():
    """Test dapat catch semua exceptions dengan base class"""
    exceptions_to_test = [
        APIFetchError("api", "error"),
        ValidationError("field", "reason"),
        GCSUploadError("bucket", "obj", "msg"),
    ]
    
    for exc in exceptions_to_test:
        try:
            raise exc
        except CommodityDAGException as e:
            assert isinstance(e, CommodityDAGException)


def test_exception_chain():
    """Test exception chaining"""
    try:
        try:
            raise ValueError("Original error")
        except ValueError as e:
            raise APIFetchError("api", "Wrapped error") from e
    except APIFetchError as e:
        assert e.__cause__ is not None
        assert isinstance(e.__cause__, ValueError)


# ===========================
# TEST 11: ERROR MESSAGE FORMATTING
# ===========================

def test_api_fetch_error_message_format():
    """Test APIFetchError message formatting"""
    exc = APIFetchError("test_api", "test message", 500)
    message = str(exc)
    
    assert message.count("[") == 1
    assert message.count("]") == 1
    assert "[test_api]" in message


def test_validation_error_message_format():
    """Test ValidationError message formatting"""
    exc = ValidationError("test_field", "test reason")
    message = str(exc)
    
    assert "test_field" in message
    assert "test reason" in message
    assert "Validation failed" in message


def test_all_exceptions_have_descriptive_messages():
    """Verify semua exceptions menghasilkan descriptive messages"""
    exceptions_with_messages = [
        (APIFetchError("api", "msg"), "[api]"),
        (ValidationError("field", "reason"), "field"),
        (GCSUploadError("bucket", "obj", "msg"), "gs://bucket"),
        (SecretManagerError("secret", "proj", "msg"), "secret"),
        (DataTransformationError("op", "reason"), "op"),
        (ConfigurationError("config", "msg"), "config"),
        (RetryExhaustedError("api", 3, Exception("err")), "api"),
        (CommodityTimeoutError("op", 10), "op"),
    ]
    
    for exc, expected_in_message in exceptions_with_messages:
        assert expected_in_message in str(exc), \
            f"Expected '{expected_in_message}' in exception message: {str(exc)}"
