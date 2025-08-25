FROM apache/airflow:2.9.3-python3.11

# Create folders as root
USER root
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins /opt/airflow/dbt_project /opt/airflow/dbt_profiles \
    && mkdir -p /opt/dbt \
    && chown -R airflow:root /opt/airflow /opt/dbt

# Switch to airflow user for pip installs
USER airflow

# Change default pip user to allow installation
ENV PIP_USER=false

# Create venv and install dbt
RUN python -m venv /opt/dbt \
    && /opt/dbt/bin/pip install --no-cache-dir \
       dbt-core==1.7.14 dbt-postgres==1.7.14

# Make the venv bin visible to all Airflow processes
ENV PATH="/opt/dbt/bin:${PATH}"
