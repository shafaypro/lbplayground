import os
import shutil
from datetime import timedelta

from _utils import run_sql_file
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from master_dag_datasets import CURATED_DATASET

from airflow import DAG


def snapshot_duckdb():
    src = "/opt/duckdb/warehouse/lb.duckdb"
    dst = "/opt/duckdb/warehouse/lb_ro.duckdb"
    shutil.copy(src, dst)
    print(f"Snapshot created: {dst}")


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
    description="Marts + Reporting layer for LB",
    schedule=[CURATED_DATASET],  # triggered by curated output
    start_date=days_ago(1),
    catchup=False,
    tags=["duckdb", "listenbrainz", "marts", "reporting"],
    max_active_runs=1,  # ← serialize runs
    concurrency=1,  # ← serialize tasks within this DAG (optional but helps)
) as dag:

    mart_user_activity = PythonOperator(
        task_id="mart_user_activity",
        python_callable=run_sql_file,
        op_args=["production/marts/mart_user_activity.sql"],
    )

    mart_track_perf = PythonOperator(
        task_id="mart_track_performance",
        python_callable=run_sql_file,
        op_args=["production/marts/mart_track_performance.sql"],
    )
    # general reporting file references (will createa sub group here or a sub task group)
    report_files = [
        "report_top10_users.sql",
        "report_users_on_2019_03_01.sql",
        "report_first_song_per_user.sql",
        "report_top3_days_per_user.sql",
        "report_daily_active_users.sql",
    ]

    report_tasks = [
        PythonOperator(
            task_id=f"report_{f.replace('.sql','')}",
            python_callable=run_sql_file,
            op_args=[f"reporting/{f}"],
        )
        for f in report_files
    ]
    start = DummyOperator(task_id="start")
    marting_success = DummyOperator(task_id="marting_success")
    end = DummyOperator(task_id="end")

    # Add task
    snapshot_task = PythonOperator(
        task_id="snapshot_duckdb_ro",
        python_callable=snapshot_duckdb,
    )

    (
        start
        >> mart_user_activity
        >> mart_track_perf
        >> marting_success
        >> report_tasks
        >> snapshot_task
        >> end
    )
    # [mart_user_activity, mart_track_perf] >> report_tasks
