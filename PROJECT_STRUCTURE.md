# Project Structure

## Overview
```
project_4-global-commodity/
├── dags/                           # Airflow DAG & utilities
│   ├── __init__.py
│   ├── dags.py                     # Main DAG definition
│   ├── utils.py                    # Utility functions (API calls, transformations)
│   ├── exceptions.py               # Custom exception classes
│   ├── models.py                   # Pydantic data models
│   └── config.yaml                 # API endpoints configuration
│
├── tests/                          # Test suite (115 tests)
│   ├── __init__.py
│   ├── conftest.py                 # Shared pytest fixtures
│   ├── test_dag_structure.py       # DAG structure validation (20+ tests)
│   ├── test_exceptions.py          # Exception tests (22+ tests)
│   ├── test_task_get_all_keys.py   # Task 1 integration tests (3 tests)
│   ├── test_task_extract_val_load_gcs.py  # Task 2 integration tests (6 tests)
│   ├── test_task_gcs_to_bq.py      # Task 3 integration tests (6 tests)
│   ├── test_task_dbt_run.py        # Task 4 integration tests (8 tests)
│   ├── test_task_dbt_test.py       # Task 5 integration tests (10 tests)
│   └── test_utility_and_task.py    # Unit tests (40+ tests)
│
├── global_commodity_dbt/           # dbt transformation project
│   ├── dbt_project.yml             # dbt project configuration
│   ├── profiles.yml                # BigQuery connection profile
│   ├── models/
│   │   ├── _sources.yml            # Source definitions
│   │   ├── staging/                # Staging models
│   │   │   ├── staging.yml         # Staging tests & docs
│   │   │   ├── stg_commodity_prices.sql
│   │   │   ├── stg_currency_exchange_rates.sql
│   │   │   └── stg_fred_macro_observations.sql
│   │   ├── dim/                    # Dimension tables
│   │   │   ├── dim_commodity.sql
│   │   │   ├── dim_currency.sql
│   │   │   └── dim_macro_series.sql
│   │   ├── fact/                   # Fact tables
│   │   │   ├── fact_commodity_prices.sql
│   │   │   ├── fact_commodity_news.sql
│   │   │   ├── fact_currency_exchange_rates.sql
│   │   │   └── fact_macro_observations.sql
│   │   └── mart/                   # Data marts
│   │       └── mart_global_commodity_market.sql
│   ├── macros/                     # dbt macros
│   ├── tests/                      # dbt test definitions
│   ├── seeds/                      # Static CSV data
│   └── snapshots/                  # dbt snapshots
│
├── config/                         # Airflow configuration
│   └── airflow.cfg                 # Airflow settings
│
├── keys/                           # Service account keys (gitignored)
│   └── sa_key.json
│
├── logs/                           # Airflow execution logs (gitignored)
│
├── .venv/                          # Python virtual environment (gitignored)
│
├── docker-compose.yaml             # Docker compose for Airflow
├── Dockerfile                      # Docker image definition
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Poetry configuration
├── poetry.lock                     # Locked dependency versions
├── pyrightconfig.json              # Type checking configuration
│
├── validate_e2e.py                 # E2E validation script
├── E2E_TESTING_GUIDE.md            # E2E testing documentation
├── README.md                       # Main project documentation
│
├── .env                            # Environment variables (gitignored)
├── .gitignore                      # Git ignore rules
└── .pytest_cache/                  # Pytest cache (gitignored)
```

## Directory Purposes

### `/dags/`
**Purpose:** Airflow DAG definitions and utilities
**Key Files:**
- `dags.py` - Main DAG with 5 tasks (get_keys, extract_val_load, gcs_to_bq, dbt_run, dbt_test)
- `utils.py` - API fetch functions, data transformation utilities
- `exceptions.py` - 8 custom exception types for error handling
- `models.py` - 4 Pydantic models for data validation
- `config.yaml` - API endpoint URLs

**Standards:**
- ✅ Type hints on all functions
- ✅ Google-style docstrings
- ✅ Retry logic with exponential backoff
- ✅ Comprehensive error handling

### `/tests/`
**Purpose:** Comprehensive test suite (115 tests)
**Coverage:**
- Unit tests for all utility functions (40+ tests)
- Integration tests for all 5 DAG tasks (33 tests)
- DAG structure validation (20+ tests)
- Exception handling tests (22+ tests)

**Standards:**
- ✅ pytest framework
- ✅ Mock external dependencies (API calls, GCS, BigQuery)
- ✅ Shared fixtures in conftest.py
- ✅ All tests passing with zero warnings

### `/global_commodity_dbt/`
**Purpose:** dbt transformation project for BigQuery
**Architecture:**
- **Staging:** Raw data cleaning and type casting
- **Dim:** Dimension tables (commodity, currency, macro indicators)
- **Fact:** Fact tables with transactions/observations
- **Mart:** Final analytical tables for dashboards

**Standards:**
- ✅ Incremental models for performance
- ✅ Partitioning and clustering
- ✅ Data quality tests (not_null, unique)
- ✅ Proper documentation

### `/config/`
**Purpose:** Configuration files
- `airflow.cfg` - Airflow scheduler/executor settings

### Root Files

**Python Dependencies:**
- `requirements.txt` - pip dependencies
- `pyproject.toml` + `poetry.lock` - Poetry dependency management

**Docker:**
- `docker-compose.yaml` - Local Airflow deployment
- `Dockerfile` - Custom Airflow image (if needed)

**Documentation:**
- `README.md` - Main project documentation
- `E2E_TESTING_GUIDE.md` - E2E testing instructions
- `validate_e2e.py` - Automated E2E validation script

**Configuration:**
- `.env` - Environment variables (GITIGNORED)
- `.gitignore` - Git ignore patterns
- `pyrightconfig.json` - Python type checker config

## Code Quality Standards

### Python Code
- ✅ Type hints (PEP 484)
- ✅ Docstrings (Google style)
- ✅ Error handling with custom exceptions
- ✅ Retry logic for external APIs
- ✅ Comprehensive logging
- ✅ 115 tests with full coverage

### dbt Models
- ✅ Staging → Dim/Fact → Mart layering
- ✅ Incremental materialization
- ✅ Partitioning by date
- ✅ Clustering for performance
- ✅ Data quality tests
- ✅ Documentation in YAML

### Testing
- ✅ Unit tests for utilities
- ✅ Integration tests for tasks
- ✅ DAG structure validation
- ✅ Exception handling tests
- ✅ E2E validation script

## Security

**Gitignored Files:**
- ❌ `.env` - Environment variables
- ❌ `keys/` - Service account keys
- ❌ `logs/` - Airflow logs (may contain sensitive data)
- ❌ `.venv/` - Virtual environment

**Best Practices:**
- ✅ API keys stored in GCP Secret Manager
- ✅ Service account key not committed to git
- ✅ Environment variables in .env (not committed)

## Deployment

**Local Development:**
```bash
# 1. Setup virtual environment
python -m venv .venv
source .venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run tests
pytest tests/ -v

# 4. Start Airflow
docker-compose up -d

# 5. Trigger DAG
airflow dags trigger global_commodity
```

**Production:**
- Deploy DAG files to Airflow instance
- Configure GCP service account
- Schedule DAG (@daily)
- Monitor execution logs

## Maintenance

**Regular Tasks:**
- Review Airflow logs weekly
- Monitor API rate limits
- Check BigQuery costs
- Update dependencies monthly
- Run E2E tests before deployments

**Emergency Procedures:**
- Check E2E_TESTING_GUIDE.md for troubleshooting
- Review exception logs for error patterns
- Contact API providers for rate limit issues
