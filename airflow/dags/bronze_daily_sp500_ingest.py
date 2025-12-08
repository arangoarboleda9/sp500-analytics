"""Airflow DAG to ingest daily SP500 and SPY raw data into S3 Bronze."""

from datetime import datetime, timedelta
from uuid import uuid4

from airflow.api.common.experimental.trigger_dag import trigger_dag
from airflow.operators.python import PythonOperator
from scripts_loader import ScriptLoader

from airflow import DAG

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

SP500_SCRIPT_PATH = (
    "/opt/airflow/pipeline/bronze/scripts/ingest_sp500/sp500_raw_loader.py"
)
SPY_SCRIPT_PATH = (
    "/opt/airflow/pipeline/bronze/scripts/ingest_sp500/spy_holdings_raw_loader.py"
)


def load_sp500_raw(**context):
    """Load the raw SP500 data into S3 Bronze."""
    loader = ScriptLoader(SP500_SCRIPT_PATH)
    loader.load_and_run()


def load_spy_top10_raw(**context):
    """Load the raw SPY Top 10 Holdings data into S3 Bronze."""
    loader = ScriptLoader(SPY_SCRIPT_PATH)
    loader.load_and_run()


with DAG(
    "bronze_daily_sp500_ingest",
    default_args=default_args,
    description="DAG para descargar y cargar archivos RAW (SP500, SPY) a S3",
    schedule_interval="12 7 * * *",  # Esto es 5:56 AM UTC
    start_date=datetime(2025, 12, 6),
    catchup=True,
    tags=["bronze", "raw", "s3"],
) as dag:
    load_sp500_task = PythonOperator(
        task_id="load_sp500_raw",
        python_callable=load_sp500_raw,
        provide_context=True,
        dag=dag,
    )

    load_spy_task = PythonOperator(
        task_id="load_spy_top10_raw",
        python_callable=load_spy_top10_raw,
        provide_context=True,
        dag=dag,
    )

    def trigger_silver_dag(**context):
        """Trigger silver dag action"""
        trigger_dag(
            dag_id="silver_daily_sp500_process",
            run_id=f"bronze_trigger__{context['ds']}_{str(uuid4())}",
            conf={},
        )

    trigger_silver = PythonOperator(
        task_id="trigger_silver",
        python_callable=trigger_silver_dag,
        provide_context=True,
        dag=dag,
    )

    load_sp500_task >> load_spy_task >> trigger_silver
