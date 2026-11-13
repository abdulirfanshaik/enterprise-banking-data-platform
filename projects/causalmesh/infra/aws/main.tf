data "aws_partition" "current" {}

data "aws_iam_policy_document" "assume_databricks" {
  statement {
    sid     = "DatabricksExternalIdTrust"
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.databricks_aws_principal_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [var.databricks_external_id]
    }
  }
}

resource "aws_s3_bucket" "lake" {
  bucket = var.bucket_name
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
    bucket_key_enabled = true
  }
}

data "aws_iam_policy_document" "tls_only" {
  statement {
    sid     = "DenyInsecureTransport"
    effect  = "Deny"
    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.lake.arn,
      "${aws_s3_bucket.lake.arn}/*"
    ]
    principals {
      type        = "*"
      identifiers = ["*"]
    }
    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }
}

resource "aws_s3_bucket_policy" "tls_only" {
  bucket = aws_s3_bucket.lake.id
  policy = data.aws_iam_policy_document.tls_only.json
  depends_on = [aws_s3_bucket_public_access_block.lake]
}

resource "aws_glue_catalog_database" "causalmesh" {
  name = "causalmesh_${var.environment}"
}

resource "aws_cloudwatch_log_group" "causalmesh" {
  name              = "/data-platform/causalmesh/${var.environment}"
  retention_in_days = 30
}

resource "aws_iam_role" "databricks_lake" {
  name               = "causalmesh-${var.environment}-lake-access"
  assume_role_policy = data.aws_iam_policy_document.assume_databricks.json
}

data "aws_iam_policy_document" "lake_rw" {
  statement {
    sid     = "BucketMetadata"
    effect  = "Allow"
    actions = ["s3:GetBucketLocation", "s3:ListBucket"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid     = "DataObjectReadWrite"
    effect  = "Allow"
    actions = [
      "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
      "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"
    ]
    resources = [
      "${aws_s3_bucket.lake.arn}/landing/*",
      "${aws_s3_bucket.lake.arn}/delta/*",
      "${aws_s3_bucket.lake.arn}/checkpoints/*"
    ]
  }
}

resource "aws_iam_role_policy" "lake_rw" {
  name   = "causalmesh-lake-prefix-access"
  role   = aws_iam_role.databricks_lake.id
  policy = data.aws_iam_policy_document.lake_rw.json
}
