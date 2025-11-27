resource "aws_s3_bucket" "data_lake" {
  bucket        = var.s3_bucket_name
  force_destroy = false
  tags = {
    Project = "sp500-analytics"
    Env     = var.env
    Owner   = var.owner_tag
  }
}

resource "aws_s3_bucket_versioning" "versioning" {
  bucket = aws_s3_bucket.data_lake.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "block" {
  bucket                  = aws_s3_bucket.data_lake.id
  block_public_acls       = true
  block_public_policy     = true
  restrict_public_buckets = true
  ignore_public_acls      = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "encryption" {
  bucket = aws_s3_bucket.data_lake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "raw_prefix" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "raw/"
}

resource "aws_s3_object" "clean_prefix" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "silver/"
}

resource "aws_s3_object" "curated_prefix" {
  bucket = aws_s3_bucket.data_lake.bucket
  key    = "gold/"
}