output "service_account_email" {
  description = "Email Service Account yang dibuat untuk Airflow."
  value       = google_service_account.airflow_sa.email
}

output "bronze_bucket_name" {
  description = "Nama GCS Bucket (Bronze Layer)."
  value       = google_storage_bucket.bronze_bucket.name
}

output "bigquery_dataset_id" {
  description = "ID BigQuery Dataset."
  value       = google_bigquery_dataset.dwh_commodity.dataset_id
}
