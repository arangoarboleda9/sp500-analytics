from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import importlib.util
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


SCRIPTS_DIR = "/opt/airflow/pipeline/bronze/scripts/ingest_rds"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def run_script(script_path, **context):
    log = LoggingMixin().log
    log.info(f"Executing script: {script_path}")

    module_name = os.path.basename(script_path).replace(".py", "")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "main"):
        raise AttributeError(f"Script {script_path} does not define main()")

    module.main()
    log.info(f"Script completed: {script_path}")


def discover_scripts():
    """Returns the list of .py scripts inside the RDS ingestion folder."""
    return sorted(
        [
            os.path.join(SCRIPTS_DIR, f)
            for f in os.listdir(SCRIPTS_DIR)
            if f.endswith(".py")
        ]
    )


with DAG(
    dag_id="bronze_process_to_rds",
    default_args=default_args,
    start_date=datetime(2025, 11, 19),
    schedule=None,
    catchup=False,
    tags=["bronze", "rds", "s3", "json"],
) as dag:

    script_tasks = []

    for script_path in discover_scripts():
        task = PythonOperator(
            task_id=f"run_{os.path.basename(script_path).replace('.py', '')}",
            python_callable=run_script,
            op_kwargs={"script_path": script_path},
        )
        script_tasks.append(task)

trigger_silver = TriggerDagRunOperator(
    task_id="trigger_silver_pipeline",
    trigger_dag_id="silver_process",
    wait_for_completion=False,
)

script_tasks >> trigger_silver
