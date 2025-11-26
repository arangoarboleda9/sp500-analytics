resource "aws_iam_role" "etl_role" {
  name = "sp500-etl-role-test-${var.env}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = {
    Project = "sp500-analytics"
    Env     = var.env
  }
}

resource "aws_iam_policy" "etl_s3_policy" {
  name        = "sp500-etl-s3-policy-${var.env}"
  description = "Permite acceso al Data Lake S3 para la instancia ETL"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [aws_s3_bucket.data_lake.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"]
        Resource = ["${aws_s3_bucket.data_lake.arn}/*"]
      }
    ]
  })

  tags = {
    Project = "sp500-analytics"
    Env     = var.env
  }
}

resource "aws_iam_role_policy_attachment" "etl_role_attach" {
  role       = aws_iam_role.etl_role.name
  policy_arn = aws_iam_policy.etl_s3_policy.arn
}

resource "aws_iam_instance_profile" "etl_instance_profile" {
  name = "sp500-etl-instance-profile-${var.env}"
  role = aws_iam_role.etl_role.name
}
