variable "project_id" {
  description = "Google Cloud project ID in which resources will be created."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be a valid lowercase Google Cloud project ID."
  }
}

variable "environment" {
  description = "Deployment environment used in resource names and labels."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be one of: dev, staging, prod."
  }
}

variable "location" {
  description = "Shared GCS and BigQuery location. Keep both services co-located."
  type        = string
  default     = "asia-southeast2"
}

variable "raw_bucket_name" {
  description = "Optional globally unique raw-data bucket name. Defaults to <project_id>-<environment>-equity-raw."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition = var.raw_bucket_name == null || can(regex(
      "^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$",
      var.raw_bucket_name,
    ))
    error_message = "raw_bucket_name must be null or a valid 3-63 character GCS bucket name."
  }
}

variable "gcs_storage_class" {
  description = "Storage class for raw market-data objects."
  type        = string
  default     = "STANDARD"

  validation {
    condition = contains(
      ["STANDARD", "NEARLINE", "COLDLINE", "ARCHIVE"],
      var.gcs_storage_class,
    )
    error_message = "gcs_storage_class must be STANDARD, NEARLINE, COLDLINE, or ARCHIVE."
  }
}

variable "raw_dataset_id" {
  description = "BigQuery dataset ID for raw observations and ingestion metadata."
  type        = string
  default     = "equity_raw"

  validation {
    condition     = can(regex("^[A-Za-z0-9_]+$", var.raw_dataset_id))
    error_message = "raw_dataset_id may contain only letters, numbers, and underscores."
  }
}

variable "analytics_dataset_id" {
  description = "BigQuery dataset ID for canonical models and analytical marts."
  type        = string
  default     = "equity_analytics"

  validation {
    condition     = can(regex("^[A-Za-z0-9_]+$", var.analytics_dataset_id))
    error_message = "analytics_dataset_id may contain only letters, numbers, and underscores."
  }
}

variable "force_destroy" {
  description = "Allow Terraform to delete non-empty data resources. Keep false outside disposable environments."
  type        = bool
  default     = false
}

variable "labels" {
  description = "Additional labels applied to supported Google Cloud resources."
  type        = map(string)
  default     = {}
}
