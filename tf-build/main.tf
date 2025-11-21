# Deklarasi Provider Google Cloud
# Ini menghubungkan Terraform ke GCP
provider "google" {
  project = var.gcp_project_id
  region  = var.gcp_region
}

# ----------------------------------------------------
# 1. Google Service Account (SA) - Identitas Airflow
# ----------------------------------------------------
resource "google_service_account" "airflow_sa" {
  account_id   = "sa-airflow-data-pi"
  display_name = "Airflow Data Pipeline Service Account"
}

# ----------------------------------------------------
# 2. GCS Bucket (Bronze Layer - Data Lake)
# ----------------------------------------------------
resource "google_storage_bucket" "bronze_bucket" {
  # Nama harus unik global
  name          = "data-lake-bronze-${var.gcp_project_id}"
  location      = var.gcp_region
  force_destroy = true
}

resource "google_project_iam_member" "job_user" {
  project = var.gcp_project_id

  role   = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

resource "google_storage_bucket_iam_member" "airflow_bronze_admin" {
  bucket = google_storage_bucket.bronze_bucket.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow_sa.email}"
}

# ----------------------------------------------------
# 3. BigQuery Dataset (Data Warehouse)
# ----------------------------------------------------
resource "google_bigquery_dataset" "dwh_commodity" {
  dataset_id    = "dwh_commodity"
  friendly_name = "Data Warehouse Commodity Marts"
  location      = var.gcp_region

  # Berikan izin ke Service Account untuk menulis ke Dataset ini
  access {
    role          = "roles/bigquery.dataEditor"
    user_by_email = google_service_account.airflow_sa.email
  }

}

# ----------------------------------------------------
# 4. Service Account Key (Kredensial Login)
# ----------------------------------------------------
resource "google_service_account_key" "airflow_sa_key" {
  service_account_id = google_service_account.airflow_sa.name
}

resource "local_file" "airflow_sa_key_file" {
  # Nama file JSON yang akan dibuat
  filename = "../keys/sa_key.json"

  # Konten (private_key dikodekan dalam Base64, harus di-decode)
  content = base64decode(google_service_account_key.airflow_sa_key.private_key)
}
