# 🌍 Global Commodity Data Pipeline

**End-to-end data pipeline for global commodity market analysis using Apache Airflow, GCP, BigQuery, and dbt**

[![Tests](https://img.shields.io/badge/tests-115%20passing-brightgreen)]()
[![Python](https://img.shields.io/badge/python-3.12-blue)]()
[![Airflow](https://img.shields.io/badge/airflow-3.1.3-orange)]()
[![dbt](https://img.shields.io/badge/dbt-1.8+-purple)]()

---

## 🎯 Overview

Production-ready data engineering project that:
- Ingests commodity data from 4 external APIs (metals, FX rates, macro indicators, news)
- Orchestrates with Apache Airflow (5 tasks)
- Stores raw data in GCS (date-partitioned NDJSON)
- Loads to BigQuery and transforms with dbt (11 models)
- Produces analytics-ready dataset with multi-currency prices and macro indicators

**Pipeline Flow:**
```
APIs → Airflow → GCS (Bronze) → BigQuery → dbt → Mart Table
```

---

## 🏗️ Architecture

```
External APIs
   ↓
┌─────────────────────────────────────┐
│ Airflow DAG (5 tasks)               │
│ 1. Fetch API keys (Secret Manager)  │
│ 2. Extract & Load to GCS            │
│ 3. GCS → BigQuery (raw)             │
│ 4. dbt run (11 models)              │
│ 5. dbt test                         │
└─────────────────────────────────────┘
   ↓
GCS: global_commodity/date=YYYY-MM-DD/commodity_data.json
   ↓
BigQuery: dwh_commodity
   ├─ staging (3 views)
   ├─ dim (3 tables)
   ├─ fact (4 partitioned tables)
   └─ mart (1 analytics table)
```

---

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| **Orchestration** | Apache Airflow 3.1.3 (Docker Compose) |
| **Cloud** | GCP (Secret Manager, GCS, BigQuery) |
| **Transformation** | dbt 1.8+ with BigQuery adapter |
| **Language** | Python 3.12 |
| **Testing** | pytest (115 tests) |
| **Data Validation** | Pydantic v2 |
| **Retry Logic** | tenacity 9.0.0 |

---

## ✨ Key Features

### Production-Grade Practices
- ✅ **115 unit + integration tests** (100% passing)
- ✅ **Type hints & Pydantic validation** for data quality
- ✅ **Custom exception hierarchy** (8 exception types)
- ✅ **Exponential backoff retry logic** for API resilience
- ✅ **Environment-based config** (.env + Secret Manager)
- ✅ **Incremental dbt models** (partitioned + clustered)
- ✅ **SAFE_CAST** for handling invalid data
- ✅ **Comprehensive logging** at each stage

### Data Engineering
- **Immutable raw layer** - NDJSON in GCS, date-partitioned
- **Schema evolution** - Autodetect + field additions
- **Late-arriving data** - LOCF (Last Observation Carried Forward)
- **Deduplication** - Unique keys on incremental models
- **Multi-currency conversion** - USD → IDR, JPY, CNY, EUR

---

## 📊 Data Sources

| API | Data | Update Frequency |
|-----|------|------------------|
| Metals.dev | Gold, Silver, Platinum, Nickel, Copper prices | Real-time |
| CurrencyFreaks | USD exchange rates (4 currencies) | Daily |
| FRED | CPI, USD Index, 10Y Treasury Yield | Daily |
| NewsAPI | Commodity news volume | Real-time |

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- GCP account with APIs enabled (Secret Manager, GCS, BigQuery)
- Service account key: `keys/key.json`
- API keys stored in GCP Secret Manager

### Run Locally
```bash
# 1. Clone repo
git clone <repo-url>
cd project_4-global-commodity

# 2. Configure .env
cat > .env <<EOF
AIRFLOW_UID=1000
GCP_PROJECT_ID=your-project-id
DWH_DATASET_ID=dwh_commodity
BUCKET_NAME=your-bucket-name
EOF

# 3. Start Airflow
docker compose up -d

# 4. Access UI
# URL: http://localhost:8080
# User/Pass: airflow/airflow

# 5. Trigger DAG
# Click "Trigger DAG" on global_commodity
```

### Verify
```bash
# Run tests
docker compose exec airflow-scheduler pytest /opt/airflow/tests -v

# Check output
python validate_e2e.py
```

---

## 📈 Output Dataset

**Mart Table:** `project-global-commodityy.dwh_commodity.mart_global_commodity_market`

**Sample Query:**
```sql
SELECT 
  date, commodity_id,
  price AS usd_price,
  idr_price, jpy_price, eur_price,
  news_count,
  cpi_value, usd_index_value, treasury_yield_value
FROM `project-global-commodityy.dwh_commodity.mart_global_commodity_market`
WHERE date = CURRENT_DATE()
ORDER BY commodity_id;
```

**Columns:** date, commodity_id, price (USD), multi-currency prices, news volume, macro indicators (CPI, USD Index, Treasury Yield with LOCF)

---

## 🔧 Project Structure

```
project_4-global-commodity/
├── dags/
│   ├── dags.py           # Main DAG (5 tasks)
│   ├── utils.py          # API clients & transformations
│   ├── models.py         # Pydantic schemas
│   └── exceptions.py     # 8 custom exceptions
├── global_commodity_dbt/ # dbt project (11 models)
│   ├── models/
│   │   ├── staging/      # 3 views
│   │   ├── dim/          # 3 tables
│   │   ├── fact/         # 4 incremental tables
│   │   └── mart/         # 1 analytics table
├── tests/                # 115 pytest tests
├── docker-compose.yaml   # Airflow + Postgres
├── .env                  # Environment variables
└── README.md
```

---

## ✅ Testing

**115 Tests (all passing):**
- 73 unit tests (utils, models, exceptions, DAG structure)
- 42 integration tests (all 5 tasks)

```bash
pytest -v
# 115 passed in 12.34s
```

---

## 🎓 Learning Highlights

**What this project demonstrates:**
- Building production-grade data pipelines
- Airflow orchestration best practices
- GCP cloud integration (Secret Manager, GCS, BigQuery)
- dbt modeling (staging → dim → fact → mart)
- Comprehensive testing strategies
- Error handling & retry mechanisms
- Data quality enforcement
- Type safety with Pydantic
- Environment-based configuration

---

## 📝 Future Enhancements

- [ ] Add dbt schema tests (not_null, unique, relationships)
- [ ] Switch to WRITE_APPEND with deduplication
- [ ] Google Trends integration (rate limiting)
- [ ] Email/Slack alerts on failures
- [ ] BI dashboard (Looker/Tableau/Power BI)

---

## 📄 License

Educational & portfolio project.

---

**Built with ❤️ for learning modern data engineering**
