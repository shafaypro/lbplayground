import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from _utils import run_sql_file
from master_dag_datasets import RAW_DATASET, CURATED_DATASET

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
    description="Staging + Curated layer for LB",
    schedule=[RAW_DATASET],  # triggered by raw ingestion
    start_date=days_ago(1),
    catchup=False,
    tags=["duckdb","lbplayground","staging","curated", "dataengineering"],
) as dag:

    stage_listens = PythonOperator(
        task_id="staging_listens_flat",
        python_callable=run_sql_file,
        op_args=["staging/listens_flat.sql"],
    )

    dim_user = PythonOperator(
        task_id="curated_dim_user_scd2",
        python_callable=run_sql_file,
        op_args=["production/curated/dim_user_scd2.sql"],
    )

    fact_listen = PythonOperator(
        task_id="curated_fact_listen",
        python_callable=run_sql_file,
        op_args=["production/curated/fact_listen.sql"],
    )

    snapshots = PythonOperator(
        task_id="curated_snapshots_user_activity",
        python_callable=run_sql_file,
        op_args=["production/curated/snapshots_user_activity.sql"],
        outlets=[CURATED_DATASET],  # signals downstream DAGs
    )

    stage_listens >> dim_user >> fact_listen >> snapshots
