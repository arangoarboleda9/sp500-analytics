terraform {
  backend "s3" {
    bucket = "sp500-terraform-state-henry"
    key    = "prod/terraform.tfstate"
    region = "us-east-1"
  }
}