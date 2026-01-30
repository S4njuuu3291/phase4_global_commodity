"""
Custom exceptions untuk global_commodity DAG

Provides specific exception types untuk better error handling dan debugging.
"""


class CommodityDAGException(Exception):
    """Base exception untuk semua commodity DAG related errors"""
    pass


class APIFetchError(CommodityDAGException):
    """Raised ketika API call gagal atau response invalid"""
    
    def __init__(self, api_name: str, message: str, status_code: int = None):
        """
        Args:
            api_name: Nama API yang gagal (e.g., 'metal_prices', 'currency_rates')
            message: Error message deskriptif
            status_code: HTTP status code jika applicable
        """
        self.api_name = api_name
        self.message = message
        self.status_code = status_code
        
        full_message = f"[{api_name}] {message}"
        if status_code:
            full_message += f" (Status: {status_code})"
        
        super().__init__(full_message)


class ValidationError(CommodityDAGException):
    """Raised ketika data validation gagal"""
    
    def __init__(self, field: str, reason: str):
        """
        Args:
            field: Field name yang gagal validasi
            reason: Alasan validasi gagal
        """
        self.field = field
        self.reason = reason
        
        message = f"Validation failed for '{field}': {reason}"
        super().__init__(message)


class GCSUploadError(CommodityDAGException):
    """Raised ketika GCS upload gagal"""
    
    def __init__(self, bucket: str, object_name: str, message: str):
        """
        Args:
            bucket: GCS bucket name
            object_name: Object name di GCS
            message: Error message
        """
        self.bucket = bucket
        self.object_name = object_name
        self.message = message
        
        full_message = f"GCS upload failed to gs://{bucket}/{object_name}: {message}"
        super().__init__(full_message)


class SecretManagerError(CommodityDAGException):
    """Raised ketika fetch secret dari Google Secret Manager gagal"""
    
    def __init__(self, secret_name: str, project_id: str, message: str):
        """
        Args:
            secret_name: Nama secret yang tidak bisa di-fetch
            project_id: GCP project ID
            message: Error message
        """
        self.secret_name = secret_name
        self.project_id = project_id
        self.message = message
        
        full_message = f"Failed to fetch secret '{secret_name}' from project '{project_id}': {message}"
        super().__init__(full_message)


class DataTransformationError(CommodityDAGException):
    """Raised ketika data transformation/sanitization gagal"""
    
    def __init__(self, operation: str, reason: str):
        """
        Args:
            operation: Operasi yang gagal (e.g., 'sanitize_keys', 'to_ndjson')
            reason: Alasan kegagalan
        """
        self.operation = operation
        self.reason = reason
        
        message = f"Data transformation failed during '{operation}': {reason}"
        super().__init__(message)


class ConfigurationError(CommodityDAGException):
    """Raised ketika ada masalah dengan configuration"""
    
    def __init__(self, config_item: str, message: str):
        """
        Args:
            config_item: Configuration item yang bermasalah
            message: Error message
        """
        self.config_item = config_item
        self.message = message
        
        full_message = f"Configuration error for '{config_item}': {message}"
        super().__init__(full_message)


class RetryExhaustedError(CommodityDAGException):
    """Raised ketika retry attempts sudah habis"""
    
    def __init__(self, api_name: str, max_attempts: int, last_error: Exception):
        """
        Args:
            api_name: API name yang di-retry
            max_attempts: Jumlah retry attempts
            last_error: Last exception yang terjadi
        """
        self.api_name = api_name
        self.max_attempts = max_attempts
        self.last_error = last_error
        
        message = f"API '{api_name}' failed after {max_attempts} retry attempts. Last error: {str(last_error)}"
        super().__init__(message)


class TimeoutError(CommodityDAGException):
    """Raised ketika operation timeout"""
    
    def __init__(self, operation: str, timeout_seconds: int):
        """
        Args:
            operation: Operasi yang timeout
            timeout_seconds: Timeout duration dalam seconds
        """
        self.operation = operation
        self.timeout_seconds = timeout_seconds
        
        message = f"Operation '{operation}' timed out after {timeout_seconds} seconds"
        super().__init__(message)
