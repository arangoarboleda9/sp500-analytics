FROM apache/airflow:2.8.2-python3.10
LABEL maintainer="lucianachamorro87@gmail.com"
USER airflow
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/pipeline"
COPY requirements.txt /requirements.txt

RUN pip install --user --upgrade pip && \
    pip install --user -r /requirements.txt

CMD ["airflow", "standalone"]
