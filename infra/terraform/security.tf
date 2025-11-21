resource "aws_security_group" "etl_sg" {
  name        = "sp500-etl-sg-${var.env}"
  description = "Acceso a la instancia ETL / Airflow"
  vpc_id      = aws_vpc.main.id

  # SSH
  ingress {
    description = "SSH"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["${var.allowed_ip}/32"]
  }

  # Airflow UI (opcional, puerto 8080)
  ingress {
    description = "Airflow UI"
    from_port   = 8080
    to_port     = 8080
    protocol    = "tcp"
    cidr_blocks = ["${var.allowed_ip}/32"]
  }

  # Salida a Internet
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "sp500-etl-sg-${var.env}"
    Project = "sp500-analytics"
    Env     = var.env
  }
}