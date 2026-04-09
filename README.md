# Global Commodity Data Pipeline

![Global Commodity Data Pipeline Hero](assets/images/hero.png)

![Python](https://img.shields.io/badge/python-3.12-3776AB?logo=python&logoColor=white) ![Airflow](https://img.shields.io/badge/airflow-2.10.2-017CEE?logo=apacheairflow&logoColor=white) ![dbt](https://img.shields.io/badge/dbt-athena--community-FF694B?logo=dbt&logoColor=white) ![AWS](https://img.shields.io/badge/aws-s3|athena|ssm-FF9900?logo=amazonaws&logoColor=white)

End-to-end data pipeline for commodity intelligence. Ingests metals prices, FX rates, macro indicators, and news; transforms with dbt; serves analytics via Athena.

### What It Does

- Ingests 4 live data sources: metals, currency rates, FRED, news
- Orchestrates with Apache Airflow (daily DAG)
- Stores Bronze (raw) → Silver (parquet) on S3
- Models with dbt (staging + marts)
- Exposes via Athena external tables

## Architecture

![Architecture](assets/images/architecture.png)

### Pipeline in Action

<details>
<summary><strong>📊 Airflow Orchestration</strong></summary>

![Airflow DAG Graph](assets/images/airflow-graph.png)
![Airflow Ingest Success](assets/images/airflow-log-success-ingest_metal.png)
![Airflow dbt Run](assets/images/airflow-log-success-dbt-run.png)
</details>

<details>
<summary><strong>🔄 dbt Transformation</strong></summary>

![dbt Lineage](assets/images/dbt-lineage.png)
![Fact Table Sample](assets/images/fact-table-sample.png)
</details>

## Tech & Stack

| Layer | Tools |
|-------|-------|
| **Orchestration** | Apache Airflow 2.10.2 |
| **Compute** | Python 3.12, dbt-athena-community |
| **Storage** | AWS S3, Glue Data Catalog, Athena |
| **Config** | AWS SSM Parameter Store |
| **Testing** | Pytest, Docker Compose |

## Project Structure

```
.
├── dags/                   # Airflow DAG, ingestion, transforms, Pydantic models
├── commodity_dbt/          # dbt: staging models, marts, tests
├── terraform-aws/          # Infrastructure: S3, SSM, Glue catalog
├── tests/                  # Unit tests (60+ tests)
├── ATHENA_SCHEMA.sql       # Athena external table DDL
└── docker-compose.yaml     # Local dev environment
```

## Quick Start

### Prerequisites

- Docker + Docker Compose
- AWS credentials in `~/.aws`
- SSM parameters at `/global-commodity/datasource/{metal|currency|fred|news_api}/{url|api_key}`

### Setup (5 min)

1. Clone & create `.env`:

```bash
cat > .env << EOF
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
AIRFLOW_UID=50000
EOF
```

2. Start services:

```bash
docker compose up airflow-init
docker compose up -d
```

3. Open Airflow:

```
http://localhost:8080
Username: admin
Password: admin
```

4. Trigger DAG:

- Enable `commodity_pipeline` in Airflow UI
- Click **Trigger DAG**
- Monitor: ingest → transform → dbt run → dbt test

## Data Architecture

### Layers

| Layer | Format | Location |
|-------|--------|----------|
| **Bronze** | JSON/NDJSON (raw) | `s3://.../bronze/{metal,currency,fred,news}/` |
| **Silver** | Parquet (cleaned) | `s3://.../silver/{metal,currency,fred,news}/` |
| **Analytics** | Catalog tables | `ds_commodity_staged.*` (Athena) |

### Mart: `fact_commodity_performance`

Output schema:
- `commodity_code`, `price_usd_per_gr`, `price_idr_per_gr`
- `usd_to_idr`, `news_hype_count`
- `yield_10y`, `usd_index`, `cpi_index`
- `trade_date`

### dbt Models

**Sources:** metal_prices, currency_rates, fred_data, news_data (from Athena Silver)

**Staging:** normalize types, dates, currencies

**Marts:** one fact table for commodity analytics

## Testing

### Run Tests

```bash
# All tests (60+ tests across models, utils, DAGs)
pytest tests -v

# Or with coverage
pytest tests --cov=dags --cov-report=html

# Helper script
./run_tests.sh all
./run_tests.sh coverage
```

### Test Coverage

- **test_models.py:** Pydantic validations, edge cases
- **test_utils.py:** S3 ops, API fetches, transforms (mocked)
- **test_dags.py:** DAG structure, dependencies, task config