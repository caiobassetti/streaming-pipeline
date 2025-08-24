from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
import os
import pandas as pd
import psycopg2

# Helper: connect to your demo Postgres using Airflow Connection "pg_demo"
def _pg_conn():
    conn = BaseHook.get_connection("pg_demo")
    return psycopg2.connect(
        host=conn.host, port=conn.port, dbname=conn.schema,
        user=conn.login, password=conn.password
    )

def aggregate_events():
    """
    Materialize/refresh fct_events_by_action in Postgres.
    - CREATE TABLE IF NOT EXISTS ... (once)
    - TRUNCATE + INSERT (each run) for a fresh snapshot
    """
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

def export_report_csv():
    """
    Export the batch table to a CSV artifact so non-technical viewers
    (and recruiters) can see a tangible output.
    """
    os.makedirs("/opt/airflow/reports", exist_ok=True)
    with _pg_conn() as db:
        df = pd.read_sql_query(
            "SELECT action, n FROM fct_events_by_action ORDER BY n DESC;", db
        )
    out = f"/opt/airflow/reports/events_by_action_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.csv"
    df.to_csv(out, index=False)
    print(f"Wrote {out} with {len(df)} rows")

default_args = {"owner": "you", "depends_on_past": False, "retries": 0}

with DAG(
    dag_id="clickstream_batch",
    description="Batch aggregation over streamed events (Kafka -> Postgres) + dbt",
    start_date=datetime(2025, 8, 1),
    schedule_interval=None,     # manual trigger for demo; set cron later if desired
    catchup=False,
    default_args=default_args,
    tags=["demo", "postgres", "dbt", "batch"],
) as dag:

    start = EmptyOperator(task_id="start")

    aggregate = PythonOperator(
        task_id="aggregate_events",
        python_callable=aggregate_events,
    )

    # Run dbt inside the Airflow container
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=("""
      set -euo pipefail
      cd /opt/airflow/dbt_project
      DBT_PROFILES_DIR=/opt/airflow/dbt_profiles /home/airflow/.local/bin/dbt deps
      DBT_PROFILES_DIR=/opt/airflow/dbt_profiles /home/airflow/.local/bin/dbt run
    """
        ),
        do_xcom_push=False,
    )

    export = PythonOperator(
        task_id="export_report_csv",
        python_callable=export_report_csv,
    )

    done = EmptyOperator(task_id="done")

    start >> aggregate >> dbt_run >> export >> done
