FROM apache/airflow:2.8.2-python3.10
LABEL maintainer="lucianachamorro87@gmail.com"

# -----------------------------------
# 2. Instalar dependencias Python como 'airflow'
# -----------------------------------
COPY requirements.txt /requirements.txt

# Dar permisos al usuario airflow sobre el archivo (lo hace root)
RUN chown airflow:0 /requirements.txt

# Cambiar a usuario airflow para usar pip
USER airflow
RUN pip install --user --upgrade pip && \
    pip install --user -r /requirements.txt
    
# -----------------------------------
# 3. Copiar pipeline + dags
# -----------------------------------
COPY pipeline/ /opt/airflow/pipeline/
COPY airflow/dags/ /opt/airflow/dags/

# -----------------------------------
# 4. Asegurar que Airflow vea tu pipeline
# -----------------------------------
ENV PYTHONPATH=/opt/airflow/pipeline:${PYTHONPATH}

# -----------------------------------
# 5. Comando por defecto (para debug)
# -----------------------------------
CMD ["airflow", "version"]
