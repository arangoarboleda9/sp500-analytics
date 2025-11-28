FROM apache/airflow:2.8.2-python3.10
LABEL maintainer="lucianachamorro87@gmail.com"

# -----------------------------------
# 1. Instalar dependencias del sistema (si hiciera falta)
# -----------------------------------
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# -----------------------------------
# 2. Instalar dependencias Python
# -----------------------------------
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt

# -----------------------------------
# 3. Copiar pipeline + dags (solo producción)
#    *NO pongas volúmenes encima en AWS*
# -----------------------------------
COPY pipeline/ /opt/airflow/pipeline/
COPY airflow/dags/ /opt/airflow/dags/

# -----------------------------------
# 4. Ajustar permisos para usuario airflow
# -----------------------------------
RUN chown -R airflow:0 /opt/airflow

# -----------------------------------
# 5. Volver al usuario estándar de Airflow
# -----------------------------------
USER airflow

# -----------------------------------
# 6. Asegurar que Airflow vea tu pipeline
# -----------------------------------
ENV PYTHONPATH=/opt/airflow/pipeline:${PYTHONPATH}

# -----------------------------------
# 7. Comando por defecto (para debug local)
#    En producción se usa docker-compose / ECS task override
# -----------------------------------
CMD ["airflow", "version"]