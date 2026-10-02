terraform {
  required_providers { google = { source = "hashicorp/google", version = "~> 5.0" } }
}
provider "google" {
  project = var.project_id
  region  = var.region
}

# ---- Data lake ----
resource "google_storage_bucket" "lake" {
  name          = var.bucket_name
  location      = var.region
  force_destroy = true
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90 }
  }
}

# ---- Data warehouse dataset ----
resource "google_bigquery_dataset" "warehouse" {
  dataset_id = var.bq_dataset
  location   = var.region
}

# ---- Service account for Airflow ----
resource "google_service_account" "airflow" {
  account_id   = "airflow-runner"
  display_name = "Airflow pipeline runner"
}
resource "google_project_iam_member" "bq_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}
resource "google_project_iam_member" "bq_jobuser" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.airflow.email}"
}
resource "google_storage_bucket_iam_member" "lake_writer" {
  bucket = google_storage_bucket.lake.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.airflow.email}"
}
resource "google_service_account_key" "airflow_key" {
  service_account_id = google_service_account.airflow.name
}
