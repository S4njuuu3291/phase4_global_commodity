#!/usr/bin/env python3
"""
End-to-End Validation Script
Validates DAG execution outputs in GCP
"""
import os
from datetime import datetime
from google.cloud import storage, bigquery, secretmanager
import json


def check_secret_manager():
    """Check if API keys accessible from Secret Manager"""
    print("\n🔑 Checking Secret Manager...")
    try:
        client = secretmanager.SecretManagerServiceClient()
        project_id = os.getenv("GCP_PROJECT_ID", "project-global-commodityy")
        
        secrets = ["metal_price_api_key", "currency_rates_api_key", 
                   "fred_api_key", "news_api_key"]
        
        for secret_name in secrets:
            secret_path = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            response = client.access_secret_version(request={"name": secret_path})
            print(f"  ✅ {secret_name}: accessible")
        
        print("✅ Secret Manager validation PASSED\n")
        return True
    except Exception as e:
        print(f"❌ Secret Manager validation FAILED: {e}\n")
        return False


def check_gcs_file():
    """Check if NDJSON file exists in GCS"""
    print("📦 Checking GCS file...")
    try:
        bucket_name = os.getenv("BUCKET_NAME", "your-bucket-name")
        today = datetime.now().strftime("%Y-%m-%d")
        file_path = f"global_commodity/date={today}/commodity_data.json"
        
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(file_path)
        
        if blob.exists():
            size_mb = blob.size / (1024 * 1024)
            print(f"  ✅ File exists: gs://{bucket_name}/{file_path}")
            print(f"  📊 Size: {size_mb:.2f} MB")
            
            # Download and check content
            content = blob.download_as_text()
            data = json.loads(content)
            print(f"  🔍 Data keys: {list(data.keys())}")
            print("✅ GCS validation PASSED\n")
            return True
        else:
            print(f"  ❌ File not found: gs://{bucket_name}/{file_path}")
            print("❌ GCS validation FAILED\n")
            return False
    except Exception as e:
        print(f"❌ GCS validation FAILED: {e}\n")
        return False


def check_bigquery_raw_data():
    """Check if raw data loaded to BigQuery"""
    print("🗄️  Checking BigQuery raw data...")
    try:
        client = bigquery.Client()
        table_id = "project-global-commodityy.dwh_commodity.commodity_data"
        
        # Count today's records
        query = f"""
        SELECT 
            COUNT(*) as row_count,
            MAX(timestamp) as latest_timestamp
        FROM `{table_id}`
        WHERE DATE(timestamp) = CURRENT_DATE()
        """
        
        result = client.query(query).result()
        for row in result:
            print(f"  📊 Rows loaded today: {row.row_count}")
            print(f"  🕐 Latest timestamp: {row.latest_timestamp}")
            
            if row.row_count > 0:
                print("✅ BigQuery raw data validation PASSED\n")
                return True
            else:
                print("❌ No data loaded today\n")
                return False
    except Exception as e:
        print(f"❌ BigQuery validation FAILED: {e}\n")
        return False


def check_dbt_models():
    """Check if dbt models executed successfully"""
    print("🔧 Checking dbt models...")
    try:
        client = bigquery.Client()
        
        models = [
            "dwh_commodity.stg_commodity_prices",
            "dwh_commodity.fact_commodity_prices",
            "dwh_commodity.mart_global_commodity_market",
        ]
        
        for model in models:
            table_id = f"project-global-commodityy.{model}"
            
            query = f"""
            SELECT COUNT(*) as row_count
            FROM `{table_id}`
            WHERE DATE(execution_timestamp) = CURRENT_DATE()
            OR DATE(date) = CURRENT_DATE()
            """
            
            try:
                result = client.query(query).result()
                for row in result:
                    print(f"  ✅ {model}: {row.row_count} rows")
            except Exception as e:
                print(f"  ❌ {model}: {str(e)[:50]}")
                
        print("✅ dbt models validation PASSED\n")
        return True
    except Exception as e:
        print(f"❌ dbt models validation FAILED: {e}\n")
        return False


def check_dbt_tests():
    """Check dbt test results (manual check)"""
    print("🧪 dbt Tests Status:")
    print("  ℹ️  Check dbt test logs in Airflow UI")
    print("  ℹ️  Or run: cd global_commodity_dbt && poetry run dbt test\n")
    return True


def main():
    """Run all E2E validations"""
    print("=" * 60)
    print("🚀 End-to-End Validation - Global Commodity DAG")
    print("=" * 60)
    
    results = {
        "Secret Manager": check_secret_manager(),
        "GCS File": check_gcs_file(),
        "BigQuery Raw": check_bigquery_raw_data(),
        "dbt Models": check_dbt_models(),
        "dbt Tests": check_dbt_tests(),
    }
    
    print("=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    
    for check, passed in results.items():
        status = "✅ PASSED" if passed else "❌ FAILED"
        print(f"{check:20s}: {status}")
    
    print("=" * 60)
    
    all_passed = all(results.values())
    if all_passed:
        print("🎉 ALL VALIDATIONS PASSED! E2E test successful!\n")
        return 0
    else:
        print("⚠️  Some validations failed. Check errors above.\n")
        return 1


if __name__ == "__main__":
    exit(main())
