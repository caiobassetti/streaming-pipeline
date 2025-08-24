# airflow/Dockerfile
FROM apache/airflow:2.9.3-python3.11

USER root
RUN pip install --no-cache-dir \
    "dbt-core==1.7.14" \
    "dbt-postgres==1.7.14"

USER airflow
