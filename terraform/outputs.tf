output "airflow_sa_email" { value = google_service_account.airflow.email }
output "airflow_sa_key_base64" {
  value     = google_service_account_key.airflow_key.private_key
  sensitive = true
}
output "gcs_bucket" { value = google_storage_bucket.lake.name }
output "bq_dataset" { value = google_bigquery_dataset.warehouse.dataset_id }
