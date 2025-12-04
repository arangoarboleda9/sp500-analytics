"""Airflow DAG to generate SP500 GOLD dataset (daily enriched prices)."""

from datetime import datetime, timedelta

import boto3
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from config import Config
from scripts_loader import ScriptLoader

from airflow import DAG


def wait_for_s3_key(bucket, key_prefix, **context):
    """Wait until at least one file exists in the given prefix."""
    s3 = boto3.client("s3")
    execution_date = context["execution_date"]

    rendered_prefix = key_prefix.replace(
        "{{ ds.replace('-', '/') }}",
        execution_date.strftime("%Y/%m/%d"),
    )

    resp = s3.list_objects_v2(Bucket=bucket, Prefix=rendered_prefix)

    return "Contents" in resp and len(resp["Contents"]) > 0


GOLD_SCRIPT = "/opt/airflow/pipeline/gold/scripts/sp500/enrich_gold_loader.py"

DATE_TEMPLATE = "{{ ds.replace('-', '/') }}"

S3_SILVER_PREFIXES = {
    "sp500": f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SP500}/{DATE_TEMPLATE}/",
    "spy": f"{Config.S3_SILVER_DIR_PREFIX}/{Config.S3_SILVER_PREFIX_SPY}/{DATE_TEMPLATE}/",
}


def run_gold_loader(**context):
    """Run the GOLD enrichment script for SP500."""
    ScriptLoader(GOLD_SCRIPT).load_and_run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "gold_daily_sp500_process",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=["gold", "etl", "s3"],
) as dag:
    wait_sp500_silver = PythonSensor(
        task_id="wait_sp500_silver",
        python_callable=wait_for_s3_key,
        op_kwargs={
            "bucket": Config.S3_BUCKET,
            "key_prefix": S3_SILVER_PREFIXES["sp500"],
        },
        poke_interval=30,
        timeout=3600,
    )

    wait_spy_silver = PythonSensor(
        task_id="wait_spy_silver",
        python_callable=wait_for_s3_key,
        op_kwargs={
            "bucket": Config.S3_BUCKET,
            "key_prefix": S3_SILVER_PREFIXES["spy"],
        },
        poke_interval=30,
        timeout=3600,
    )

    gold_enrich = PythonOperator(
        task_id="generate_sp500_gold",
        python_callable=run_gold_loader,
        provide_context=True,
    )

    [wait_sp500_silver, wait_spy_silver] >> gold_enrich
