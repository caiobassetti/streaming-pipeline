from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import psycopg2

# Connect to Postgres using Airflow Connection "pg_demo"
def _pg_conn():
    conn = BaseHook.get_connection("pg_demo")
    return psycopg2.connect(
        host=conn.host, port=conn.port, dbname=conn.schema,
        user=conn.login, password=conn.password
    )

# Materialize/refresh fct_events_by_action in Postgre
def aggregate_events():
    sql = """
    CREATE TABLE IF NOT EXISTS fct_events_by_action (
        action text PRIMARY KEY,
        n bigint
    );
    TRUNCATE fct_events_by_action;
    INSERT INTO fct_events_by_action (action, n)
    SELECT action, COUNT(*) AS n
    FROM raw_events
    GROUP BY action
    ORDER BY n DESC;
    """
    with _pg_conn() as db:
        with db.cursor() as cur:
            cur.execute(sql)
            db.commit()

default_args = {"owner": "you", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="clickstream_batch",
    description="Batch aggregation over streamed events (Kafka -> Postgres) + dbt",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["demo", "postgres", "dbt", "batch"],
) as dag:

    start = EmptyOperator(task_id="start")

    aggregate = PythonOperator(
        task_id="aggregate_events",
        python_callable=aggregate_events,
    )

    # Run dbt inside Airflow container
    dbt_run = BashOperator(
    task_id="dbt_run",
    bash_command=(
        "cd /opt/airflow/dbt_project && "
        "DBT_PROFILES_DIR=/opt/airflow/dbt_profiles "
        "/opt/dbt/bin/dbt deps && /opt/dbt/bin/dbt run"
    ),
    env={"DBT_PROFILES_DIR": "/opt/airflow/dbt_profiles"},
)

    done = EmptyOperator(task_id="done")

    start >> aggregate >> dbt_run >> done
