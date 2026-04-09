# Unit Testing Guide - Global Commodity Data Pipeline

## Overview

Comprehensive pytest test suite untuk data pipeline dengan coverage untuk:
- **Models**: Data validation dengan Pydantic
- **Utils**: Fetch, transform functions, dan S3 operations
- **DAGs**: Airflow DAG structure, tasks, dan dependencies

## Test Structure

```
tests/
├── conftest.py              # Shared fixtures dan configuration
├── test_models.py           # Tests untuk Pydantic models
├── test_utils.py            # Tests untuk utility functions
├── test_dags.py             # Tests untuk DAG definitions
└── __init__.py
```

### File Descriptions

#### `conftest.py`
Pytest configuration dengan shared fixtures:
- `mock_config`: Mock config.yaml
- `mock_ssm_response`: Mock AWS SSM responses
- `sample_metal_response`: Sample API responses untuk berbagai datasources
- `mock_aws_clients`: Mock S3 dan SSM clients
- `mock_httpx`: Mock HTTP client

**Kelebihan**: Fixtures dapat digunakan di semua test files

#### `test_models.py`
Tests untuk 4 Pydantic models:

**ProcessedMetalModel**
- Valid data creation ✅
- Empty symbol validation ❌
- Negative/zero price validation ❌
- Ingested_at default value ✅

**CurrencyRateModel**
- Valid currency data ✅
- String date parsing (timezone supported) ✅
- Exchange rate validation ✅

**FredDataModel**
- Valid FRED data ✅
- Handling "." as missing value ✅
- String-to-float conversion ✅

**NewsCountModel**
- Valid news data ✅
- Status validation (must be "ok") ✅
- Non-negative mentions validation ✅

#### `test_utils.py`
Tests untuk utility functions dengan mocking AWS dan HTTP:

**S3 Operations**
- Upload valid data to S3 ✅
- Empty data handling ✅
- AWS error handling ✅
- NDJSON format validation ✅
- Read JSON from S3 ✅
- Handle nonexistent keys ✅
- Invalid JSON handling ✅

**API Fetch Functions**
- `fetch_metal_prices()` ✅
- `fetch_currency_rate()` ✅
- `fetch_fred_data()` ✅
- `fetch_news()` ✅
- Missing secret handling ✅

**Transform Functions**
- `transform_metal_prices()` ✅
- `transform_currency_rates()` ✅
- `transform_fred_data()` ✅
- `transform_news()` ✅
- Invalid data handling ✅

**SSM Secret Retrieval**
- Get secrets successfully ✅
- Handle not found ✅
- Error handling ✅

#### `test_dags.py`
Tests untuk Airflow DAG structure:

**DAG Structure**
- DAG exists ✅
- Configuration (owner, retries, schedule) ✅
- Start date ✅
- Tags ✅

**Task Definitions**
- Bronze ingestion tasks (4) ✅
- Silver transformation tasks (4) ✅
- DBT tasks (2) ✅
- Total task count (10) ✅

**Dependencies & Flow**
- Bronze tasks have no upstream ✅
- Silver tasks depend on bronze ✅
- DBT tasks execute after transforms ✅
- No cycles (DAG is acyclic) ✅

**Task Configuration**
- Retry configuration ✅
- BashOperator type validation ✅
- DBT command syntax ✅

---

## Installation

### 1. Install Testing Dependencies

```bash
pip install -r requirements-dev.txt
```

Atau dengan conda:
```bash
conda install -c conda-forge pytest pytest-cov pytest-mock moto responses
```

### 2. Verify Installation

```bash
pytest --version
# atau
python -m pytest --version
```

---

## Running Tests

### Run All Tests

```bash
pytest
```

### Run Specific Test File

```bash
# Test models only
pytest tests/test_models.py

# Test utils only
pytest tests/test_utils.py

# Test DAGs only
pytest tests/test_dags.py
```

### Run Specific Test Class

```bash
pytest tests/test_models.py::TestProcessedMetalModel
pytest tests/test_utils.py::TestUploadResponseToS3
pytest tests/test_dags.py::TestDAGStructure
```

### Run Specific Test

```bash
pytest tests/test_models.py::TestProcessedMetalModel::test_valid_metal_data
pytest tests/test_utils.py::TestUploadResponseToS3::test_upload_valid_data
```

### Run with Verbose Output

```bash
pytest -v
```

### Run with Coverage Report

```bash
pytest --cov=dags --cov-report=html --cov-report=term-missing

# Open HTML report
open htmlcov/index.html
```

### Run with Markers

```bash
# Run only unit tests
pytest -m unit

# Run tests with mock
pytest -m mock
```

### Run with Different Output Formats

```bash
# JSON report
pytest --json-report

# HTML report
pytest --html=report.html

# JUnit XML (for CI/CD)
pytest --junit-xml=junit.xml
```

---

## Test Statistics

Total Tests: **60+**

### By File:
- `test_models.py`: ~20 tests
- `test_utils.py`: ~25 tests  
- `test_dags.py`: ~15 tests

### Coverage Goals:
- **Models**: 95%+ (critical for data validation)
- **Utils**: 85%+ (mocking external services)
- **DAGs**: 90%+ (Airflow structure)

---

## Mocking Strategy

### AWS Services (S3, SSM)

Using `unittest.mock.MagicMock` untuk simulate AWS responses:

```python
@patch('utils.boto3.client')
def test_upload_valid_data(self, mock_boto3_client):
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # ... test code
```

### HTTP Requests

Using `unittest.mock` untuk mock httpx responses:

```python
@patch('utils.httpx.Client')
def test_fetch_metal_prices_success(self, mock_httpx_client):
    mock_response = MagicMock()
    mock_response.json.return_value = sample_data
```

### Benefits:
- ✅ No AWS credentials needed
- ✅ No external API calls
- ✅ Fast execution (~5-10 seconds)
- ✅ Deterministic results
- ✅ Isolation dari external dependencies

---

## Fixtures

### Sample Data Fixtures

```python
@pytest.fixture
def sample_metal_response():
    return {
        'timestamp': '2026-04-09T10:00:00.000Z',
        'metals': {...},
        'currency_base': 'USD'
    }
```

Usage:
```python
def test_fetch_metals(self, sample_metal_response):
    # Use fixture
    mock_response.json.return_value = sample_metal_response
```

### AWS Client Fixtures

```python
@pytest.fixture
def mock_aws_clients():
    with patch('utils.boto3.client') as mock_boto3:
        # Setup mocks
        yield {'s3': mock_s3, 'ssm': mock_ssm}
```

---

## Common Issues & Solutions

### Issue 1: ModuleNotFoundError pada import

**Error**: `ModuleNotFoundError: No module named 'utils'`

**Solution**:
```python
import sys
import os
dags_path = os.path.join(os.path.dirname(__file__), '../dags')
sys.path.insert(0, dags_path)
```

### Issue 2: AWS Credentials tidak ada

**Solution**: Tests menggunakan mocking, tidak perlu AWS credentials

### Issue 3: Slow test execution

**Solution**:
```bash
# Run in parallel
pytest -n auto

# Run subset
pytest tests/test_models.py -x  # Stop on first failure
```

### Issue 4: Import errors untuk models/exceptions

**Solution**: Pastikan `conftest.py` di `tests/` folder (parent dari test files)

---

## CI/CD Integration

### GitHub Actions Example

```yaml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Set up Python
        uses: actions/setup-python@v2
        with:
          python-version: 3.12
      - name: Install dependencies
        run: pip install -r requirements-dev.txt
      - name: Run tests
        run: pytest --cov=dags --cov-report=xml
      - name: Upload coverage
        uses: codecov/codecov-action@v2
```

---

## Best Practices

1. **Mocking External Services**: Selalu mock AWS, HTTP calls
2. **Naming Conventions**: `test_<function>_<scenario>`
3. **Fixtures**: Reusable melalui conftest.py
4. **Assertions**: Clear dan specific
5. **Error Cases**: Test both success dan failure paths
6. **Documentation**: Docstrings untuk test functions

---

## Performance Tips

```bash
# Run tests in parallel (requires pytest-xdist)
pip install pytest-xdist
pytest -n auto

# Run fastest tests first
pytest --durations=10

# Stop after first failure
pytest -x

# Stop after N failures
pytest --maxfail=3
```

---

## Extending Tests

### Adding New Model Test

```python
class TestNewModel:
    def test_valid_data(self):
        data = {...}
        model = NewModel(**data)
        assert model.field == expected_value
    
    def test_invalid_data(self):
        with pytest.raises(ValueError):
            NewModel(**invalid_data)
```

### Adding New Utils Test

```python
@patch('utils.boto3.client')
def test_new_function(self, mock_boto3):
    mock_s3 = MagicMock()
    mock_boto3.return_value = mock_s3
    
    result = new_function()
    
    assert result is not None
    mock_s3.method.assert_called_once()
```

---

## Documentation

- [Pytest Documentation](https://docs.pytest.org/)
- [unittest.mock Documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Pydantic Testing](https://docs.pydantic.dev/latest/)
- [Airflow Testing](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing)

---

**Last Updated**: April 9, 2026
**Maintained By**: Sanju
**Status**: ✅ Active & Maintained
