variable "aws_region" {
  description = "AWS region for the portfolio data lake."
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "Globally unique S3 bucket name supplied by the user."
  type        = string
}
