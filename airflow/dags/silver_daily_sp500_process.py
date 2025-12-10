"""Airflow DAG for processing daily SP500 data into Silver layer."""

from datetime import datetime, timedelta
from uuid import uuid4

import boto3
from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from config import Config
from scripts_loader import ScriptLoader

from airflow import DAG


def wait_for_s3_key(bucket, key_prefix, **context):
    """Wait for a specific S3 key prefix to have at least one object."""
    s3 = boto3.client("s3")

    execution_date = context["execution_date"]
    rendered_prefix = key_prefix.replace(
        "{{ execution_date.strftime('%Y/%m/%d') }}", execution_date.strftime("%Y/%m/%d")
    )
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=rendered_prefix)

    return "Contents" in resp and len(resp["Contents"]) > 0


SP500_SILVER_SCRIPT = (
    "/opt/airflow/pipeline/silver/scripts/sp500/sp500_silver_loader.py"
)

SPY_SILVER_SCRIPT = "/opt/airflow/pipeline/silver/scripts/sp500/spy_silver_loader.py"
DATE_TEMPLATE = "{{ ds.replace('-', '/') }}"
S3_PATH_TEMPLATE = {
    "sp500_bronze": f"{Config.S3_BRONZE_DIR_PREFIX}/{Config.S3_BRONZE_PREFIX_SP500}/{DATE_TEMPLATE}/",
    "spy_bronze": f"{Config.S3_BRONZE_DIR_PREFIX}/{Config.S3_BRONZE_PREFIX_SPY}/{DATE_TEMPLATE}/",
}


def run_sp500_silver(**context):
    """Run the SP500 Silver Loader."""
    ScriptLoader(SP500_SILVER_SCRIPT).load_and_run()


def run_spy_silver(**context):
    """Run the SPY Silver Loader."""
    ScriptLoader(SPY_SILVER_SCRIPT).load_and_run()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "silver_daily_sp500_process",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 12, 3),
    catchup=False,
    tags=["silver", "etl", "s3"],
) as dag:
    wait_sp500 = PythonSensor(
        task_id="wait_sp500_bronze",
        python_callable=wait_for_s3_key,
        op_kwargs={
            "bucket": Config.S3_BUCKET,
            "key_prefix": S3_PATH_TEMPLATE["sp500_bronze"],
        },
        poke_interval=60,
        timeout=3600,
    )

    wait_spy = PythonSensor(
        task_id="wait_spy_bronze",
        python_callable=wait_for_s3_key,
        op_kwargs={
            "bucket": Config.S3_BUCKET,
            "key_prefix": S3_PATH_TEMPLATE["spy_bronze"],
        },
        poke_interval=60,
        timeout=3600,
    )

    sp500_silver = PythonOperator(
        task_id="sp500_silver_load",
        python_callable=run_sp500_silver,
        provide_context=True,
    )

    spy_silver = PythonOperator(
        task_id="spy_silver_load",
        python_callable=run_spy_silver,
        provide_context=True,
    )

    def trigger_gold_dag(**context):
        """Trigger gold dag action"""
        trigger_dag(
            dag_id="gold_daily_sp500_process",
            run_id=f"gold_daily_sp500_process__{context['ds']}_{str(uuid4())}",
            conf={"triggered_by": "silver_daily_sp500_process"},
        )

    trigger_gold = PythonOperator(
        task_id="trigger_gold",
        python_callable=trigger_gold_dag,
        provide_context=True,
        dag=dag,
    )

    wait_sp500 >> sp500_silver
    wait_spy >> spy_silver
    [sp500_silver, spy_silver] >> trigger_gold
