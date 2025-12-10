#!/bin/bash
set -e

# Variables
IMAGE_NAME="airflow-custom:latest"
NETWORK_NAME="sp500_network"

# Directorios locales
DAGS_DIR="./airflow/dags"
PIPELINE_DIR="./pipeline"
LOGS_DIR="./airflow/logs"
PLUGINS_DIR="./airflow/plugins"
MINIO_DIR="./minio/data"
POSTGRES_DIR="./postgres/data"

echo "=== 1. Construyendo imagen Airflow ==="
docker build -t $IMAGE_NAME .

echo "=== 2. Creando red y volúmenes ==="
docker network inspect $NETWORK_NAME >/dev/null 2>&1 || docker network create $NETWORK_NAME
docker volume inspect airflow_logs >/dev/null 2>&1 || docker volume create airflow_logs
docker volume inspect airflow_dags >/dev/null 2>&1 || docker volume create airflow_dags
docker volume inspect airflow_pipeline >/dev/null 2>&1 || docker volume create airflow_pipeline
docker volume inspect airflow_plugins >/dev/null 2>&1 || docker volume create airflow_plugins
docker volume inspect postgres_data >/dev/null 2>&1 || docker volume create postgres_data

echo "=== 3. Levantando Postgres ==="
docker run -d --name postgres --network $NETWORK_NAME \
  -e POSTGRES_USER=airflow \
  -e POSTGRES_PASSWORD=airflow \
  -e POSTGRES_DB=airflow \
  -v $POSTGRES_DIR:/var/lib/postgresql/data \
  -p 5432:5432 \
  postgres:15

echo "=== 4. Levantando Redis ==="
docker run -d --name redis --network $NETWORK_NAME redis:7

echo "=== 5. Levantando MinIO ==="
docker run -d --name minio --network $NETWORK_NAME \
  -e MINIO_ROOT_USER=admin \
  -e MINIO_ROOT_PASSWORD=admin123 \
  -p 9000:9000 -p 9001:9001 \
  -v $MINIO_DIR:/data \
  minio/minio server /data --console-address ":9001"

echo "=== 6. Inicializando Airflow ==="
docker run --rm --name airflow-init --network $NETWORK_NAME \
  -v $DAGS_DIR:/opt/airflow/dags \
  -v $PIPELINE_DIR:/opt/airflow/pipeline \
  -v $LOGS_DIR:/opt/airflow/logs \
  -v $PLUGINS_DIR:/opt/airflow/plugins \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -e AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0 \
  -e AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
  -e PYTHONPATH=/opt/airflow/pipeline \
  $IMAGE_NAME \
  /bin/bash -c "airflow db init && airflow users create --username admin --password admin123 --role Admin --firstname admin --lastname admin --email admin@example.com"

echo "=== 7. Levantando Airflow Webserver ==="
docker run -d --name airflow-webserver --network $NETWORK_NAME \
  -p 8080:8080 \
  -v $DAGS_DIR:/opt/airflow/dags \
  -v $PIPELINE_DIR:/opt/airflow/pipeline \
  -v $LOGS_DIR:/opt/airflow/logs \
  -v $PLUGINS_DIR:/opt/airflow/plugins \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -e AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0 \
  -e AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
  -e PYTHONPATH=/opt/airflow/pipeline \
  $IMAGE_NAME webserver

echo "=== 8. Levantando Scheduler, Worker y Triggerer ==="
docker run -d --name airflow-scheduler --network $NETWORK_NAME \
  -v $DAGS_DIR:/opt/airflow/dags \
  -v $PIPELINE_DIR:/opt/airflow/pipeline \
  -v $LOGS_DIR:/opt/airflow/logs \
  -v $PLUGINS_DIR:/opt/airflow/plugins \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -e AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0 \
  -e AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
  -e PYTHONPATH=/opt/airflow/pipeline \
  $IMAGE_NAME scheduler

docker run -d --name airflow-worker --network $NETWORK_NAME \
  -v $DAGS_DIR:/opt/airflow/dags \
  -v $PIPELINE_DIR:/opt/airflow/pipeline \
  -v $LOGS_DIR:/opt/airflow/logs \
  -v $PLUGINS_DIR:/opt/airflow/plugins \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -e AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0 \
  -e AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
  -e PYTHONPATH=/opt/airflow/pipeline \
  $IMAGE_NAME celery worker

docker run -d --name airflow-triggerer --network $NETWORK_NAME \
  -v $DAGS_DIR:/opt/airflow/dags \
  -v $PIPELINE_DIR:/opt/airflow/pipeline \
  -v $LOGS_DIR:/opt/airflow/logs \
  -v $PLUGINS_DIR:/opt/airflow/plugins \
  -e AIRFLOW__CORE__EXECUTOR=CeleryExecutor \
  -e AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow \
  -e AIRFLOW__CELERY__BROKER_URL=redis://redis:6379/0 \
  -e AIRFLOW__CELERY__RESULT_BACKEND=db+postgresql://airflow:airflow@postgres/airflow \
  -e PYTHONPATH=/opt/airflow/pipeline \
  $IMAGE_NAME triggerer

echo "=== Airflow stack levantada correctamente ==="
echo "Webserver: http://<EC2_PUBLIC_IP>:8080"


[flake8]
# Configuración de flake8 para el proyecto
max-line-length = 88
extend-ignore =
    # E203: whitespace before ':' (conflicto con black)
    E203,
    # W503: line break before binary operator (conflicto con black)
    W503,
    # E501: line too long (manejado por black)
    E501,
    D100, D400
exclude =
    .git,
    __pycache__,
    .venv,
    venv,
    env,
    .env,
    build,
    dist,
    *.egg-info,
    .eggs,
    migrations,
    node_modules,
    .pytest_cache,
    .mypy_cache,
    .tox
max-complexity = 10
per-file-ignores =
    __init__.py: F401,D100,D101,D102,D103,D104,D105,D107
    tests/*:S101
    */migrations/*:E501
