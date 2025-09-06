import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from _utils import run_sql_file
from master_dag_datasets import CURATED_DATASET

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
    description="Data Quality Checks for LB pipeline",
    schedule=[CURATED_DATASET],   # dataset-aware scheduling
    start_date=days_ago(1),
    catchup=False,
    tags=["duckdb", "dq", "dataquality"],
    max_active_runs=1,   # ✅ only one run at a time
    concurrency=1,       # ✅ only one task at a time
) as dag:

    start = DummyOperator(task_id="start")

    # Adjust this list depending on where your DQ SQLs are stored
    dq_files = [
    "production/dq/init_monitoring.sql",        # ✅ bootstrap schema + table
    "production/dq/check_row_counts.sql",
    "production/dq/check_nulls.sql",
    "production/dq/check_duplicates.sql",
    "production/dq/check_future_dates.sql",
    "production/dq/check_dim_user_integrity.sql",
]


    dq_tasks = []
    for f in dq_files:
        # Sanitize filename to create a valid task_id
        task_id = "dq_" + f.replace("/", "_").replace(".sql", "")
        dq_tasks.append(
            PythonOperator(
                task_id=task_id,
                python_callable=run_sql_file,
                op_args=[f],
            )
        )

    end = DummyOperator(task_id="end")
    snapshots = PythonOperator(
        task_id="curated_snapshots_user_activity",
        python_callable=run_sql_file,
        op_args=["production/curated/snapshots_user_activity.sql"],
        outlets=[CURATED_DATASET],  # signals downstream DAGs
    )
    start >> dq_tasks >> end >> snapshots
