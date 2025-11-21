provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project = "sp500-analytics"
      Env     = var.env
    }
  }
}