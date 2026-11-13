output "bucket_name" {
  value = aws_s3_bucket.lake.id
}
output "landing_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/landing/"
}
output "delta_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/delta/"
}
output "checkpoint_uri" {
  value = "s3://${aws_s3_bucket.lake.id}/checkpoints/"
}
output "databricks_role_arn" {
  value = aws_iam_role.databricks_lake.arn
}
