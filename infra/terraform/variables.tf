variable "dockerhub_username" {
  description = "Usuario de Docker Hub donde se publica la imagen sp500-etl"
  type        = string
}

variable "aws_region" {
  description = "Región AWS donde se despliega la infraestructura del Data Lake para el proyecto SP500 Analytics. Se usa para asegurar consistencia entre servicios (S3, RDS, IAM, etc.)."
  type        = string
  default     = "us-east-1"
}

variable "env" {
  description = "Entorno lógico del despliegue (dev, test o prod). Permite diferenciar infraestructura entre ambientes y seguir buenas prácticas de DevOps."
  type        = string
  default     = "dev"
}

variable "s3_bucket_name" {
  description = "Nombre del bucket S3 que funcionará como Data Lake del proyecto."
  type        = string
}

variable "owner_tag" {
  description = "Etiqueta que identifica al responsable del despliegue o al equipo del proyecto. Útil para auditoría, organización y gobernanza en AWS."
  type        = string
  default     = "sp500-henry-team"
}

variable "allowed_ip" {
  description = "IP pública desde la que se permitirá el acceso a la instancia (SSH / Airflow). Sin /32, solo la IP."
  type        = string
}

variable "vpc_cidr" {
  description = "CIDR de la VPC principal"
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_1a_cidr" {
  description = "CIDR de la subnet pública 1a"
  type        = string
  default     = "10.0.1.0/24"
}
variable "instance_type" {
  description = "Tipo de instancia EC2 para el servidor ETL"
  type        = string
  default     = "c7i-flex.large"
}

variable "ami_id" {
  description = "AMI para la instancia ETL"
  type        = string
  default     = "ami-0c02fb55956c7d316"
}