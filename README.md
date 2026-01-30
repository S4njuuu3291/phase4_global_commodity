# 🌍 Global Commodity Data Pipeline

**Production-Grade Data Engineering Project using Apache Airflow, GCP, BigQuery, and dbt**

[![Tests](https://img.shields.io/badge/tests-115%20passing-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.12-blue)]()
[![Airflow](https://img.shields.io/badge/airflow-3.1.3-orange)]()
[![dbt](https://img.shields.io/badge/dbt-1.8+-purple)]()

---

## 📋 Project Summary

This repository contains an **end-to-end data engineering pipeline** that ingests, stores, transforms, and serves **global commodity market data** on a daily basis.

The project demonstrates **production-ready data engineering practices**, including:
- ✅ Comprehensive testing (115 unit + integration tests)
- ✅ Type hints and error handling
- ✅ Secret management with GCP Secret Manager
- ✅ Environment-based configuration
- ✅ Docker containerization
- ✅ Data quality enforcement
- ✅ Incremental transformations

---

## 🏗️ High-Level Architecture

```
External APIs (Metal Prices, Currency, FRED, News)
   ↓
Apache Airflow (Docker Compose)
   ├─ Task 1: Fetch API keys from Secret Manager
   ├─ Task 2: Extract & Load to GCS (NDJSON)
   ├─ Task 3: Load GCS → BigQuery (raw table)
   ├─ Task 4: dbt run (transform to mart)
   └─ Task 5: dbt test (data quality)
   ↓
Google Cloud Storage (Bronze Layer - date-partitioned)
   ↓
BigQuery (Data Warehouse)
   ├─ Raw: commodity_data
   ├─ Staging: stg_* views
   ├─ Dimensions: dim_* tables
   ├─ Facts: fact_* tables (partitioned + clustered)
   └─ Mart: mart_global_commodity_market
   ↓
Analytics-Ready Dataset
```

📷 **Architecture Diagram**

![global-commodity-architecture](img/architecture-diagram.png)

---

## 🛠️ Technology Stack

| Layer              | Technology                          | Purpose                          |
| ------------------ | ----------------------------------- | -------------------------------- |
| Orchestration      | Apache Airflow 3.1.3                | Task scheduling & monitoring     |
| Containerization   | Docker Compose                      | Local development environment    |
| Cloud Platform     | Google Cloud Platform (GCP)         | Cloud infrastructure             |
| Secrets Management | GCP Secret Manager                  | API key storage                  |
| Object Storage     | Google Cloud Storage                | Raw data lake (NDJSON)           |
| Data Warehouse     | BigQuery                            | Analytics warehouse              |
| Transformation     | dbt 1.8+ (BigQuery adapter)         | Data modeling & transformations  |
| Language           | Python 3.12                         | DAG & utility functions          |
| Testing            | pytest + pytest-mock                | Unit & integration tests         |
| HTTP Client        | httpx                               | Async API calls                  |
| Data Validation    | Pydantic v2                         | Schema validation                |
| Retry Logic        | tenacity 9.0.0                      | Exponential backoff              |

---

## 📊 Data Sources

| Domain         | Source             | Frequency | Description                                   | API Docs                        |
| -------------- | ------------------ | --------- | --------------------------------------------- | ------------------------------- |
| Metals         | Metals.dev API     | Real-time | Gold, Silver, Platinum, Copper, Nickel prices | [metals.dev](https://metals.dev) |
| FX Rates       | CurrencyFreaks API | Daily     | USD-based exchange rates (IDR, EUR, JPY, CNY) | [currencyfreaks.com](https://currencyfreaks.com) |
| Macroeconomics | FRED API           | Daily     | CPI, USD Index, 10Y Treasury Yield            | [fred.stlouisfed.org](https://fred.stlouisfed.org) |
| News           | NewsAPI            | Real-time | Commodity-related news volume                 | [newsapi.org](https://newsapi.org) |

---

## 🗄️ Data Lake Design (GCS)

Raw data is stored in **newline-delimited JSON (NDJSON)** format to ensure schema flexibility and historical traceability.

**Storage Structure:**
```
gs://data-lake-bronze-project-global-commodityy/
└── global_commodity/
    └── date=YYYY-MM-DD/
        └── commodity_data.json
```

**Benefits:**
- Date-partitioned for efficient querying
- NDJSON format supports schema evolution
- Raw data preserved for reprocessing
- Enables data lineage tracking

📷 **Sample Raw Data (NDJSON)**
```json
{
  "timestamp": "2026-01-30 08:39:12",
  "metals": {
    "timestamp": "2026-01-30T08:38:03.570Z",
    "metals": {
      "gold": 137.1954,
      "silver": 2.0311,
      "platinum": 54.2414,
      "nickel": 9.8765,
      "copper": 5.4321
    },
    "currency_base": "USD"
  },
  "currency": {
    "date": "2026-01-30",
    "base": "USD",
    "rates": {
      "IDR": 16663.0,
      "JPY": 155.505,
      "EUR": 0.851716,
      "CNY": 7.234
    }
  },
  "macro": {
    "CPIAUCSL": {
      "observations": [
        { "date": "2025-09-01", "value": "324.368" }
      ]
    },
    "DTWEXBGS": {
      "observations": [
        { "date": "2026-01-29", "value": "102.45" }
      ]
    },
    "DGS10": {
      "observations": [
        { "date": "2026-01-29", "value": "4.23" }
      ]
    }
  },
  "news": {
    "gold": 19459,
    "silver": 10398,
    "platinum": 8234,
    "nickel": 5678,
    "copper": 12345
  }
}
```
---

## 🔄 Airflow Orchestration

**DAG ID:** `global_commodity`  
**Schedule:** `@daily`  
**Start Date:** `2026-01-28`  
**Catchup:** `False` (only run latest)

### DAG Task Flow

```
┌──────────────────┐
│ get_all_keys     │  ← Fetch 4 API keys from Secret Manager
└────────┬─────────┘
         ↓
┌──────────────────────┐
│ extract_val_load_gcs │  ← Fetch data from APIs → validate → upload to GCS
└────────┬─────────────┘
         ↓
┌──────────────────┐
│ task_gcs_to_bq   │  ← Load NDJSON from GCS → BigQuery raw table
└────────┬─────────┘
         ↓
┌──────────────────┐
│ dbt_run          │  ← Execute 11 dbt models (staging → dim → fact → mart)
└────────┬─────────┘
         ↓
┌──────────────────┐
│ dbt_test         │  ← Run data quality tests
└──────────────────┘
```

### Task Details

| Task ID | Type | Duration | Description |
|---------|------|----------|-------------|
| `get_all_keys` | @task | ~3s | Fetch secrets: `metal_price_api_key`, `currency_rates_api_key`, `fred_api_key`, `news_api_key` |
| `extract_val_load_gcs` | @task | ~15s | API calls → validation → GCS upload |
| `task_gcs_to_bq` | @task | ~2s | BigQuery LoadJob with autodetect schema |
| `dbt_run` | BashOperator | ~30s | Transform 11 models with `--full-refresh` |
| `dbt_test` | BashOperator | ~5s | Data quality validation |

**Key Features:**
- Hybrid **TaskFlow API + Operators**
- Secrets fetched dynamically from GCP Secret Manager
- Direct `google.cloud.storage.Client()` and `google.cloud.bigquery.Client()` (no Airflow connections needed)
- Environment variables from `.env` file
- Comprehensive error logging
- Type-safe with Pydantic validation

📷 **Airflow DAG Graph View**
![global_commodity-graph](img/global_commodity-graph.png)

---

## 🔐 Secrets & Configuration

### Secret Management
API keys stored securely in **GCP Secret Manager**:
```python
from google.cloud import secretmanager

def get_secret(secret_id: str, project_id: str) -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")
```

### Environment Configuration
Managed through `.env` file (not committed to git):
```bash
# .env
AIRFLOW_UID=1000
GCP_PROJECT_ID=project-global-commodityy
DWH_DATASET_ID=dwh_commodity
BUCKET_NAME=data-lake-bronze-project-global-commodityy
```

### Docker Compose Integration
Environment variables loaded into Airflow containers:
```yaml
environment:
  GOOGLE_APPLICATION_CREDENTIALS: /opt/airflow/keys/key.json
  GOOGLE_CLOUD_PROJECT: ${GCP_PROJECT_ID}
  BUCKET_NAME: ${BUCKET_NAME}
  GCP_PROJECT_ID: ${GCP_PROJECT_ID}
  DWH_DATASET_ID: ${DWH_DATASET_ID}
```

### Authentication
Development: **Application Default Credentials (ADC)**
```bash
gcloud auth application-default login
```

---

## 🔄 Data Transformation with dbt

The warehouse follows a **layered modeling approach** to ensure clarity, scalability, and maintainability.

### dbt Model Layers

| Layer     | Materialization | Purpose                                                | Models Count |
| --------- | --------------- | ------------------------------------------------------ | ------------ |
| Staging   | View            | Flattening, casting, and schema normalization          | 3            |
| Dimension | Table           | Reference entities (commodity, currency, macro series) | 3            |
| Fact      | Incremental     | Time-series measurements (partitioned + clustered)     | 4            |
| Mart      | Table           | Analytics-ready denormalized dataset                   | 1            |

**Total Models:** 11

### Model Details

#### Staging Models (Views)
- `stg_commodity_prices` - Flatten metal prices from nested JSON
- `stg_currency_exchange_rates` - Extract FX rates
- `stg_fred_macro_observations` - Flatten FRED macro data with SAFE_CAST for invalid values

#### Dimension Tables
- `dim_commodity` - 5 commodities (gold, silver, platinum, nickel, copper)
- `dim_currency` - 5 currencies (USD, IDR, EUR, JPY, CNY)
- `dim_macro_series` - 3 series (CPIAUCSL, DTWEXBGS, DGS10)

#### Fact Tables (Incremental + Partitioned)
- `fact_commodity_prices` - Daily prices, partitioned by DATE
- `fact_commodity_news` - News volume, partitioned by DATE
- `fact_currency_exchange_rates` - FX rates, partitioned by DATE
- `fact_macro_observations` - Macro indicators, partitioned by DATE (with SAFE_CAST for data quality)

#### Mart Table
- `mart_global_commodity_market` - Denormalized dataset with:
  - Commodity prices (USD)
  - Currency-converted prices (IDR, JPY, CNY, EUR)
  - News volume
  - Macro indicators (CPI, USD Index, Treasury Yield) with LOCF

📷 **dbt Lineage Graph**
![dbt_dag](img/dbt-dag.png)

### dbt Configuration

**profiles.yml** (BigQuery adapter):
```yaml
global_commodity_dbt:
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: project-global-commodityy
      dataset: dwh_commodity
      location: asia-southeast2
      threads: 4
  target: dev
```

**Execution:**
```bash
# Inside Airflow container
cd /opt/airflow/global_commodity_dbt
poetry run dbt run --full-refresh
poetry run dbt test
```

---

## 📐 Core Data Models

### Dimensions

**dim_commodity**
| Column | Type | Example |
|--------|------|---------|
| commodity_id | STRING | gold |
| commodity_name | STRING | Gold |
| commodity_type | STRING | precious |
| unit | STRING | USD/g |
| price_currency | STRING | USD |
| source | STRING | API |

**dim_currency**
| Column | Type | Example |
|--------|------|---------|
| currency_code | STRING | USD |
| currency_name | STRING | US Dollar |
| region | STRING | United States |
| is_base_reference | BOOL | TRUE |
| source | STRING | API |

**dim_macro_series**
| Column | Type | Example |
|--------|------|---------|
| series_id | STRING | CPIAUCSL |
| series_name | STRING | Consumer Price Index |
| frequency | STRING | daily |
| units | STRING | raw_value |
| source | STRING | FRED |

---

### Facts

**fact_commodity_prices** (Partitioned by `date`, Clustered by `commodity_id`)
| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Price observation date |
| commodity_id | STRING | Commodity identifier |
| price | FLOAT64 | Price in USD |
| currency_base | STRING | Base currency |

**fact_macro_observations** (Partitioned by `date_col`, Clustered by `series_id`)
| Column | Type | Description |
|--------|------|-------------|
| date_col | DATE | Observation date (for partitioning) |
| date_str | STRING | Original date string |
| series_id | STRING | FRED series ID |
| value | FLOAT64 | Macro indicator value (SAFE_CAST applied) |

**fact_currency_exchange_rates** (Partitioned by `date`)
| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Exchange rate date |
| base_currency | STRING | Base currency (USD) |
| target_currency | STRING | Target currency |
| rate | FLOAT64 | Exchange rate |

**fact_commodity_news** (Partitioned by `date`)
| Column | Type | Description |
|--------|------|-------------|
| date | DATE | News count date |
| commodity_id | STRING | Commodity identifier |
| news_count | INT64 | Number of news articles |

---

### Mart

**mart_global_commodity_market** (Partitioned by `date`, Clustered by `commodity_id`)

| Column Name | Type | Description |
|------------|------|-------------|
| date | DATE | Observation date |
| commodity_id | STRING | Commodity identifier (gold, silver, etc.) |
| price | FLOAT64 | Commodity price in USD |
| news_count | INT64 | Number of related news articles |
| idr_price | FLOAT64 | Price converted to IDR |
| jpy_price | FLOAT64 | Price converted to JPY |
| cny_price | FLOAT64 | Price converted to CNY |
| eur_price | FLOAT64 | Price converted to EUR |
| cpi_value | NUMERIC | Consumer Price Index value |
| cpi_date | DATE | CPI observation date |
| usd_index_value | NUMERIC | USD index value |
| usd_index_date | DATE | USD index observation date |
| treasury_yield_value | NUMERIC | 10Y Treasury yield |
| treasury_yield_date | DATE | Treasury yield observation date |

**Features:**
- Currency conversion via FX rate pivot
- Macro indicators joined via **Last Observation Carried Forward (LOCF)**
- Handles missing/late-arriving macro data
- Deduplication logic for FX rates

---

## ✅ Data Quality & Reliability

### Testing Strategy

**Total Tests:** 115 (all passing ✅)

#### Unit Tests (73 tests)
- `test_utils.py` - 44 tests for utility functions
- `test_models.py` - 13 tests for Pydantic schemas
- `test_exceptions.py` - 8 tests for custom exceptions
- `test_dag_structure.py` - 8 tests for DAG validation

#### Integration Tests (42 tests)
- `test_task_gcs_to_bq.py` - 6 tests for BigQuery loading
- `test_task_dbt_run.py` - 18 tests for dbt execution
- `test_task_dbt_test.py` - 18 tests for dbt testing

**Run Tests:**
```bash
pytest -v
# Output: 115 passed in 12.34s
```

### Data Quality Features

**1. Input Validation (Pydantic v2)**
```python
class MetalPriceResponse(BaseModel):
    status: str
    metals: Dict[str, float]
    currency_base: str
    timestamp: str
```

**2. Custom Exception Hierarchy**
- `DataFetchError` - API call failures
- `DataValidationError` - Schema validation errors
- `SecretFetchError` - Secret Manager issues
- `GCSUploadError` - Storage errors
- `BigQueryLoadError` - Warehouse loading errors
- 3 more specialized exceptions

**3. Retry Logic (tenacity)**
```python
@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(httpx.RequestError)
)
def fetch_data(url: str) -> dict:
    ...
```

**4. dbt Data Quality**
- Incremental models with `unique_key` for deduplication
- Date-based partitioning for query performance
- Schema validation via `autodetect=True`
- SAFE_CAST for handling invalid values (e.g., "." in FRED data)
- NOT NULL constraints on critical fields

**5. Error Handling**
- Comprehensive logging at each pipeline stage
- Pipeline fails fast on upstream errors (no silent failures)
- Type-safe transformations with explicit CAST operations
- Late-arriving data handled via LOCF (Last Observation Carried Forward)

**6. Data Lineage**
- Raw data preserved in GCS (immutable)
- Date-partitioned for historical replay
- Full transformation chain visible in dbt lineage graph

---

## 🚀 Running the Project Locally

### Prerequisites
- Docker & Docker Compose
- GCP account with:
  - Secret Manager API enabled
  - BigQuery API enabled
  - Cloud Storage API enabled
- Service account key (stored as `keys/key.json`)
- API keys for:
  - Metals.dev
  - CurrencyFreaks
  - FRED
  - NewsAPI

### Setup Steps

**1. Clone Repository**
```bash
git clone <repo-url>
cd project_4-global-commodity
```

**2. Configure Environment**
```bash
# Create .env file
cat > .env <<EOF
AIRFLOW_UID=1000
GCP_PROJECT_ID=your-project-id
DWH_DATASET_ID=dwh_commodity
BUCKET_NAME=your-bucket-name
EOF
```

**3. Add GCP Service Account Key**
```bash
# Place your service account key
cp /path/to/service-account.json keys/key.json
```

**4. Store API Keys in Secret Manager**
```bash
# Create secrets in GCP Secret Manager
echo -n "your-metal-api-key" | gcloud secrets create metal_price_api_key --data-file=-
echo -n "your-currency-api-key" | gcloud secrets create currency_rates_api_key --data-file=-
echo -n "your-fred-api-key" | gcloud secrets create fred_api_key --data-file=-
echo -n "your-news-api-key" | gcloud secrets create news_api_key --data-file=-
```

**5. Start Services**
```bash
docker compose up -d
```

**6. Access Airflow**
- URL: `http://localhost:8080`
- Username: `airflow`
- Password: `airflow`

**7. Trigger DAG**
- Navigate to DAGs → `global_commodity`
- Click "Trigger DAG" button
- Monitor task execution in Graph View

### Verify Setup

**Check Containers:**
```bash
docker compose ps
# Should show 4 running containers:
# - airflow-scheduler
# - airflow-apiserver
# - airflow-dag-processor
# - postgres
```

**Check Logs:**
```bash
docker compose logs -f airflow-scheduler
```

**Run Tests:**
```bash
docker compose exec airflow-scheduler pytest /opt/airflow/tests -v
```

---

## 📊 Output Dataset

### Final Analytics Table

**Location:**
```
project-global-commodityy.dwh_commodity.mart_global_commodity_market
```

### Sample Queries

**1. Latest Commodity Prices (Multi-Currency)**
```sql
SELECT 
  date,
  commodity_id,
  price AS usd_price,
  idr_price,
  jpy_price,
  eur_price,
  cny_price,
  news_count
FROM `project-global-commodityy.dwh_commodity.mart_global_commodity_market`
WHERE date = CURRENT_DATE()
ORDER BY commodity_id;
```

**2. Gold Price Trend with Macro Indicators**
```sql
SELECT 
  date,
  price AS gold_usd,
  cpi_value,
  usd_index_value,
  treasury_yield_value
FROM `project-global-commodityy.dwh_commodity.mart_global_commodity_market`
WHERE commodity_id = 'gold'
  AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
ORDER BY date DESC;
```

**3. Currency Conversion Analysis**
```sql
SELECT 
  commodity_id,
  AVG(price) AS avg_usd_price,
  AVG(idr_price) AS avg_idr_price,
  AVG(jpy_price) AS avg_jpy_price,
  COUNT(*) AS observation_count
FROM `project-global-commodityy.dwh_commodity.mart_global_commodity_market`
WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
GROUP BY commodity_id;
```

📷 **Sample Query Result**
![query-result](img/ss_query_mart.png)

---

## 🎯 Key Engineering Highlights

### Production-Grade Practices

**1. Comprehensive Testing**
- 115 unit + integration tests (100% passing)
- Pytest with fixtures and mocks
- Test coverage for all critical paths
- CI-ready test suite

**2. Type Safety & Validation**
- Python type hints throughout
- Pydantic v2 for runtime validation
- Custom exception hierarchy (8 exception types)
- Schema validation at ingestion

**3. Error Handling & Resilience**
- Exponential backoff retry logic (tenacity)
- Graceful degradation for missing data
- SAFE_CAST for handling invalid values
- Comprehensive error logging
- Fail-fast on critical errors

**4. Configuration Management**
- Environment-based configuration (.env)
- No hardcoded credentials
- Secret Manager for API keys
- Docker Compose for local orchestration

**5. Data Engineering Best Practices**
- Immutable raw data layer (GCS)
- Incremental transformations (dbt)
- Date partitioning for performance
- Clustering on high-cardinality dimensions
- LOCF for late-arriving macro data
- Deduplication via unique keys

**6. Code Quality**
- Modular utility functions
- Clear separation of concerns
- Docstrings for all public functions
- Consistent naming conventions
- DRY principles applied

**7. Observability**
- Structured logging at each stage
- Task-level execution metrics
- dbt model compilation artifacts
- BigQuery job monitoring

---

## 📁 Project Structure

```
project_4-global-commodity/
├── dags/
│   ├── dags.py                    # Main Airflow DAG (5 tasks)
│   ├── utils.py                   # API clients & utilities
│   ├── models.py                  # Pydantic schemas
│   ├── exceptions.py              # Custom exceptions
│   └── config.yaml                # DAG configuration
├── global_commodity_dbt/          # dbt project
│   ├── models/
│   │   ├── staging/              # 3 staging views
│   │   ├── dim/                  # 3 dimension tables
│   │   ├── fact/                 # 4 fact tables (incremental)
│   │   └── mart/                 # 1 mart table
│   ├── dbt_project.yml
│   └── profiles.yml
├── tests/                         # 115 pytest tests
│   ├── test_utils.py
│   ├── test_models.py
│   ├── test_exceptions.py
│   ├── test_dag_structure.py
│   ├── test_task_gcs_to_bq.py
│   ├── test_task_dbt_run.py
│   └── test_task_dbt_test.py
├── keys/
│   └── key.json                   # GCP service account (gitignored)
├── docker-compose.yaml            # Airflow + Postgres
├── Dockerfile                     # Custom Airflow image
├── requirements.txt               # Python dependencies
├── pyproject.toml                 # Poetry config (dbt)
├── .env                           # Environment variables (gitignored)
├── .gitignore                     # Comprehensive ignore rules
└── README.md                      # This file
```

---

## 🔧 Dependencies

### Python Packages
```txt
apache-airflow==3.1.3
httpx==0.28.1                    # Async HTTP client
pydantic==2.10.5                 # Data validation
tenacity==9.0.0                  # Retry logic
google-cloud-secret-manager
google-cloud-storage
google-cloud-bigquery
python-dotenv
pytest
pytest-mock
```

### dbt Packages
```toml
[tool.poetry.dependencies]
dbt-bigquery = "^1.8.0"
```

---

## 🚧 Known Limitations & Future Improvements

### Current Limitations
1. **Google Trends ingestion disabled** - Rate limiting issues (commented out in code)
2. **WRITE_TRUNCATE mode** - Replaces data instead of incremental append (dev mode)
3. **No SLA monitoring** - Relies on manual DAG monitoring
4. **Single environment** - No staging/prod separation

### Planned Enhancements

**Phase 1: Data Quality**
- [ ] Add dbt schema tests (not_null, unique, relationships)
- [ ] Implement data freshness checks
- [ ] Add dbt source freshness monitoring
- [ ] Switch to WRITE_APPEND with deduplication logic

**Phase 2: Observability**
- [ ] Email/Slack alerts on DAG failure
- [ ] Custom metrics export to Prometheus
- [ ] Grafana dashboard for pipeline monitoring
- [ ] SLA violation alerts

**Phase 3: Scalability**
- [ ] Google Trends integration with rate limiting
- [ ] Additional data sources (weather, logistics)
- [ ] Historical backfill automation
- [ ] Multi-region data ingestion

**Phase 4: Productionization** (if deploying)
- [ ] CI/CD pipeline (GitHub Actions)
- [ ] Terraform for infrastructure as code
- [ ] Staging environment
- [ ] Blue-green deployments
- [ ] Automated integration tests on PR

**Phase 5: Analytics**
- [ ] BI dashboard (Looker/Tableau/Power BI)
- [ ] Predictive modeling (price forecasting)
- [ ] Anomaly detection
- [ ] Real-time streaming (Pub/Sub → Dataflow)

---

## 📚 Learning Resources

**Airflow:**
- [Official Docs](https://airflow.apache.org/docs/)
- [TaskFlow API Guide](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)

**dbt:**
- [Official Docs](https://docs.getdbt.com/)
- [BigQuery Adapter](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup)

**BigQuery:**
- [Best Practices](https://cloud.google.com/bigquery/docs/best-practices)
- [Partitioning & Clustering](https://cloud.google.com/bigquery/docs/partitioned-tables)

**Testing:**
- [Pytest Documentation](https://docs.pytest.org/)
- [Testing Airflow DAGs](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#testing)

---

## 📄 License

This project is for educational and portfolio purposes.

---

## 👤 Author

**Sanju**
- Portfolio: [Add your portfolio link]
- LinkedIn: [Add your LinkedIn]
- GitHub: [Add your GitHub profile]

---

## 🙏 Acknowledgments

- Apache Airflow community
- dbt Labs
- Google Cloud Platform
- API providers: Metals.dev, CurrencyFreaks, FRED, NewsAPI

---

**Built with ❤️ for learning modern data engineering practices**
