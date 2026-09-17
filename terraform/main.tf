resource "aws_s3_bucket" "banking_lake" {
  bucket = var.bucket_name
  tags = {
    Project     = "enterprise-banking-data-platform"
    DataClass   = "synthetic"
    Environment = "portfolio"
  }
}

resource "aws_s3_bucket_versioning" "banking_lake" {
  bucket = aws_s3_bucket.banking_lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "banking_lake" {
  bucket = aws_s3_bucket.banking_lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "banking_lake" {
  bucket                  = aws_s3_bucket.banking_lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

output "data_lake_bucket" {
  value = aws_s3_bucket.banking_lake.id
}
