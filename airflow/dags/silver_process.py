from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import os
import importlib.util

SILVER_DIR = "/opt/airflow/pipeline/silver/scripts"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def run_script(script_path, **context):
    log = LoggingMixin().log
    log.info(f"Executing Silver script: {script_path}")

    module_name = os.path.basename(script_path).replace(".py", "")
    spec = importlib.util.spec_from_file_location(module_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    if not hasattr(module, "main"):
        raise AttributeError(f"{script_path} must define main()")

    module.main()
    log.info(f"Finished {script_path}")


with DAG(
    dag_id="silver_process",
    default_args=default_args,
    start_date=datetime(2025, 11, 27),
    schedule=None,
    catchup=False,
    tags=["silver", "transform"],
) as dag:

    SCRIPT_MAP = {
        "company_info_silver.py": "T1_INFO",
        "company_index_silver.py": "T2_INDEX",
        "company_stocks.py": "T3_STOCKS",
        "company_reviews_silver.py": "T5_REVIEWS",
        "company_riesgos_silver.py": "T6_RIESGOS",
    }

    tasks = {}

    for script, task_id in SCRIPT_MAP.items():
        script_path = os.path.join(SILVER_DIR, script)
        if not os.path.exists(script_path):
            continue  # permite que el DAG no falle si todavía no están todos

        tasks[task_id] = PythonOperator(
            task_id=task_id,
            python_callable=run_script,
            op_kwargs={"script_path": script_path},
        )

    # T1 must run before T5 and T6
    if "T1_INFO" in tasks:
        for dep in ["T5_REVIEWS", "T6_RIESGOS"]:
            if dep in tasks:
                tasks["T1_INFO"] >> tasks[dep]
