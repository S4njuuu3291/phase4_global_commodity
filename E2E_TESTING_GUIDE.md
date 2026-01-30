# E2E Testing Guide - Local Airflow + GCP

## Prerequisites
```bash
# 1. Airflow running locally
docker-compose up -d  # or airflow standalone

# 2. GCP credentials configured
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/sa_key.json
export GCP_PROJECT_ID=project-global-commodityy
export BUCKET_NAME=your-bucket-name

# 3. Python dependencies installed
pip install google-cloud-storage google-cloud-bigquery google-cloud-secret-manager
```

## E2E Test Execution

### Step 1: Trigger DAG
```bash
# Method 1: CLI
airflow dags trigger global_commodity

# Method 2: Web UI
# Open http://localhost:8080
# Click "Trigger DAG" on global_commodity
```

### Step 2: Monitor Execution
```bash
# List recent runs
airflow dags list-runs -d global_commodity --state running

# View task logs (replace <RUN_ID> with actual run_id)
airflow tasks logs global_commodity task_get_all_keys <RUN_ID>
airflow tasks logs global_commodity extract_val_load_gcs <RUN_ID>
airflow tasks logs global_commodity gcs_to_bq <RUN_ID>
airflow tasks logs global_commodity dbt_run <RUN_ID>
airflow tasks logs global_commodity dbt_test <RUN_ID>
```

### Step 3: Validate Outputs
```bash
# Run validation script
python validate_e2e.py

# Or manual validation:

# Check GCS
gsutil ls gs://$BUCKET_NAME/global_commodity/date=$(date +%Y-%m-%d)/

# Check BigQuery
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) FROM \`project-global-commodityy.dwh_commodity.commodity_data\` 
   WHERE DATE(timestamp) = CURRENT_DATE()"

# Check dbt models
bq ls project-global-commodityy:dwh_commodity
```

## Expected Results

### ✅ Successful E2E Test
```
Task 1: get_all_keys          ✅ Success (4 secrets fetched)
Task 2: extract_val_load_gcs  ✅ Success (data in GCS)
Task 3: gcs_to_bq             ✅ Success (data in BigQuery)
Task 4: dbt_run               ✅ Success (10+ models)
Task 5: dbt_test              ✅ Success (20+ tests)
```

### 📊 Data Validation
```bash
# GCS file should exist
gs://your-bucket/global_commodity/date=2026-01-30/commodity_data.json

# BigQuery tables should have data
- commodity_data (raw)
- stg_commodity_prices (staging)
- fact_commodity_prices (fact)
- mart_global_commodity_market (mart)

# dbt tests should pass
PASS=20 WARN=0 ERROR=0
```

## Troubleshooting

### Issue: "Secret not found"
```bash
# Verify secrets exist
gcloud secrets list --project=$GCP_PROJECT_ID

# Add missing secret
gcloud secrets create metal_price_api_key \
  --data-file=- <<< "your-api-key"
```

### Issue: "Permission denied"
```bash
# Check service account permissions
gcloud projects get-iam-policy $GCP_PROJECT_ID \
  --flatten="bindings[].members" \
  --filter="bindings.members:serviceAccount:*"

# Grant required permissions
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member=serviceAccount:YOUR_SA@PROJECT.iam.gserviceaccount.com \
  --role=roles/storage.admin
```

### Issue: "DAG not found"
```bash
# Check DAG files in correct folder
airflow config get-value core dags_folder

# List available DAGs
airflow dags list | grep global_commodity

# Refresh DAGs
airflow dags reserialize
```

### Issue: "API rate limited (429)"
```bash
# Wait and retry (retry logic will handle this)
# Or check API quota in provider dashboard
```

## Performance Benchmarks

### Expected Execution Times
```
Task 1: get_all_keys         ~5 seconds
Task 2: extract_val_load_gcs ~30 seconds (with retries: up to 90s)
Task 3: gcs_to_bq            ~15 seconds
Task 4: dbt_run              ~5 minutes (full refresh)
Task 5: dbt_test             ~2 minutes

Total: ~8-10 minutes
```

## Next Steps After Successful E2E

1. ✅ Schedule DAG (daily at midnight UTC)
   ```bash
   # Already configured in dags.py:
   schedule="@daily"
   ```

2. ✅ Setup monitoring/alerting
   - Configure Airflow email notifications
   - Monitor task failure rates
   - Set up GCP monitoring for BigQuery costs

3. ✅ Document deployment
   - Create runbook for common issues
   - Document API key rotation process

4. ✅ Production handoff
   - Share access with team
   - Setup backup strategy
   - Create disaster recovery plan

## Quick Commands Cheat Sheet

```bash
# Trigger DAG
airflow dags trigger global_commodity

# Pause DAG
airflow dags pause global_commodity

# Unpause DAG
airflow dags unpause global_commodity

# Clear failed tasks
airflow tasks clear global_commodity -t extract_val_load_gcs

# Validate E2E
python validate_e2e.py

# Run tests
pytest tests/ -v
```
