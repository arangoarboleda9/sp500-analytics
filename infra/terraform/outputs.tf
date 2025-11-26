output "bucket_name" {
  description = "Nombre del bucket S3 creado"
  value       = aws_s3_bucket.data_lake.bucket
}

output "bucket_arn" {
  description = "ARN del bucket S3"
  value       = aws_s3_bucket.data_lake.arn
}

output "vpc_id" {
  description = "ID de la VPC creada"
  value       = aws_vpc.main.id
}

output "ec2_public_ip" {
  description = "IP pública de la instancia EC2 del servidor ETL"
  value       = aws_instance.etl_server.public_ip
}

output "etl_public_ip" {
  value       = aws_eip.etl_ip.public_ip
  description = "IP pública fija (Elastic IP) del servidor ETL"
}
