variable "aws_region" {
  type    = string
  default = "us-east-1"
}

variable "environment" {
  type    = string
  default = "dev"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name. Immutable after initial deployment."
  type        = string
}

variable "databricks_aws_principal_arn" {
  description = "Trusted AWS IAM principal ARN supplied by Databricks storage credential setup."
  type        = string
}

variable "databricks_external_id" {
  description = "External ID from the Databricks Unity Catalog storage credential. Never hard-code."
  type        = string
  sensitive   = true
}
