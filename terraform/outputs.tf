output "raw_bucket_name" {
  description = "Name of the GCS bucket that stores immutable raw market data."
  value       = google_storage_bucket.raw.name
}

output "raw_bucket_url" {
  description = "GCS URL of the raw market-data bucket."
  value       = google_storage_bucket.raw.url
}

output "bigquery_dataset_ids" {
  description = "BigQuery dataset IDs keyed by logical layer."
  value = {
    for layer, dataset in google_bigquery_dataset.datasets : layer => dataset.dataset_id
  }
}

output "location" {
  description = "Shared location used by GCS and BigQuery."
  value       = var.location
}

output "project_id" {
  description = "Google Cloud project that owns the development resources."
  value       = var.project_id
}
