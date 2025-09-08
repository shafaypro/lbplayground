import os
from datetime import timedelta

from _utils import run_sql_file
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from master_dag_datasets import RAW_DATASET

from airflow import DAG

dag_id = os.path.splitext(os.path.basename(__file__))[0]

default_args = {
    "owner": "lbplayground",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description="Raw ingestion of LB JSONL into DuckDB",
    schedule_interval="@daily",  # kicks off daily
    start_date=days_ago(1),
    catchup=False,
    tags=["duckdb", "listenbrainz", "raw"],
) as dag:

    raw_load = PythonOperator(
        task_id="raw_load_listens",
        python_callable=run_sql_file,
        op_args=["raw/load_listens.sql"],
        outlets=[RAW_DATASET],  # signals downstream DAGs
    )
