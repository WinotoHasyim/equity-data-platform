terraform {
  required_version = ">= 1.6, < 2.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 8.3"
    }
  }
}

locals {
  application_name = "equity-data-platform"
  raw_bucket_name  = coalesce(var.raw_bucket_name, "${var.project_id}-${var.environment}-equity-raw")

  required_services = toset([
    "bigquery.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "storage.googleapis.com",
  ])

  bigquery_datasets = {
    raw = {
      dataset_id    = var.raw_dataset_id
      friendly_name = "Equity Data Platform Raw"
      description   = "Raw Alpaca observations and ingestion metadata."
    }
    analytics = {
      dataset_id    = var.analytics_dataset_id
      friendly_name = "Equity Data Platform Analytics"
      description   = "Canonical market data and analytical marts managed by dbt."
    }
  }

  common_labels = merge(
    {
      application = local.application_name
      environment = var.environment
      managed_by  = "terraform"
    },
    var.labels,
  )
}

provider "google" {
  project = var.project_id
  region  = var.location

  default_labels = local.common_labels
}

resource "google_project_service" "required" {
  for_each = local.required_services

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

resource "google_storage_bucket" "raw" {
  name                        = local.raw_bucket_name
  project                     = var.project_id
  location                    = var.location
  storage_class               = var.gcs_storage_class
  force_destroy               = var.force_destroy
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "AbortIncompleteMultipartUpload"
    }

    condition {
      age = 7
    }
  }

  depends_on = [google_project_service.required]
}

resource "google_bigquery_dataset" "datasets" {
  for_each = local.bigquery_datasets

  project                    = var.project_id
  dataset_id                 = each.value.dataset_id
  friendly_name              = each.value.friendly_name
  description                = each.value.description
  location                   = var.location
  max_time_travel_hours      = 168
  delete_contents_on_destroy = var.force_destroy

  depends_on = [google_project_service.required]
}
