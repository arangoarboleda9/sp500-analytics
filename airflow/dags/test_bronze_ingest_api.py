from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import io
import boto3
import os

S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
API_BASE_URL = os.getenv("API_BASE_URL")


def ingest_api_to_s3():
    try:
        response = requests.get(API_BASE_URL, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        raise RuntimeError(f"Error al consultar API: {e}")

    data = response.json()
    df = pd.json_normalize(data)  # convierte JSON a dataframe plano

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer, index=False)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        endpoint_url=S3_ENDPOINT,
    )
    date_format = datetime.utcnow().strftime('%Y/%m/%d')

    filename = f"api_data_{date_format}.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET, Key=filename, Body=parquet_buffer.getvalue()
    )
    print(f"Archivo {filename} cargado en S3/MinIO.")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="bronze_ingest_api",
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule_interval="@daily",
    catchup=False,
    tags=["bronze", "test"],
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_api",
        python_callable=ingest_api_to_s3,
    )

    ingest_task
