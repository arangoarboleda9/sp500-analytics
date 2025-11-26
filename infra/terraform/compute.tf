resource "aws_instance" "etl_server" {
  ami                    = var.ami_id
  instance_type          = var.instance_type
  subnet_id              = aws_subnet.public_1a.id
  vpc_security_group_ids = [aws_security_group.etl_sg.id]
  iam_instance_profile   = aws_iam_instance_profile.etl_instance_profile.name

  user_data = <<-EOF
    #!/bin/bash
    apt-get update -y
    apt-get install -y docker.io
    systemctl enable docker
    systemctl start docker

    docker pull ${DOCKERHUB_USERNAME}/sp500-etl:latest
    docker run -d --name sp500-etl -p 8080:8080 ${DOCKERHUB_USERNAME}/sp500-etl:latest
  EOF

  tags = {
    Name    = "sp500-etl-${var.env}"
    Project = "sp500-analytics"
    Env     = var.env
  }
}