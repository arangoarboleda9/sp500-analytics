resource "aws_cloudwatch_log_group" "etl_logs" {
  name              = "/sp500/etl"
  retention_in_days = 7

  tags = {
    Project = "sp500-analytics"
    Env     = var.env
  }
}

resource "aws_cloudwatch_metric_alarm" "etl_high_cpu" {
  alarm_name          = "sp500-etl-ec2-high-cpu-${var.env}"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 300
  statistic           = "Average"
  threshold           = 80

  alarm_description = "CPU mayor al 80% por más de 10 minutos en la instancia ETL"

  dimensions = {
    InstanceId = aws_instance.etl_server.id
  }

  tags = {
    Project = "sp500-analytics"
    Env     = var.env
  }
}
