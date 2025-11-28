FROM apache/airflow:2.8.2-python3.10
LABEL maintainer="lucianachamorro87@gmail.com"

# Usamos root para instalar cosas y tocar permisos
USER root

# -----------------------------------
# 1. Instalar dependencias Python
# -----------------------------------
COPY requirements.txt /requirements.txt
RUN chown airflow:0 /requirements.txt

# Volvemos a usuario airflow (el que usa la imagen oficial)
USER airflow

# Para que Airflow vea tu pipeline
ENV PYTHONPATH=/opt/airflow/pipeline:$PYTHONPATH

RUN pip install --user --upgrade pip && \
    pip install --user -r /requirements.txt

# -----------------------------------
# 2. Copiar pipeline + dags DENTRO DE AIRFLOW
# -----------------------------------
COPY pipeline/ /opt/airflow/pipeline/
COPY airflow/dags/ /opt/airflow/dags/

# No defino CMD acá porque docker-compose ya usa `command: webserver/scheduler/...`
