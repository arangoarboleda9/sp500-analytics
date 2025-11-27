FROM apache/airflow:2.8.2-python3.10
LABEL maintainer="lucianachamorro87@gmail.com"

# Airflow usa este usuario
USER airflow

# Para que Airflow vea tus módulos Python
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/pipeline"

# Instalar dependencias
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip && \
    pip install --user -r /requirements.txt

# Copiar DAGs dentro de la imagen
COPY dags/ /opt/airflow/dags/

# Arrancar Airflow
CMD ["airflow", "standalone"]