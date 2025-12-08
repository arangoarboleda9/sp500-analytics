data "archive_file" "stop_airflow_zip" {
  type        = "zip"
  source_file = "${path.module}/../lambda/stop_airflow_if_idle.py"
  output_path = "${path.module}/../lambda/stop_airflow_if_idle.zip"
}