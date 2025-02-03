from airflow import DAG
from datetime import datetime, timedelta
from utils import on_failure_callbacks_list


with DAG(
    dag_id="service_operations_dag",
    start_date=datetime(2024, 7, 15),
    schedule_interval="0 0 * * 0",  # Runs every Sunday
    max_active_runs=1,
    default_args={"retries": 0, "on_failure_callback": on_failure_callbacks_list},
    catchup=False,
) as service_operations_dag:
    from airflow.operators.bash import BashOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import PythonOperator
    from cosmos.cache import delete_unused_dbt_ls_cache

    start = EmptyOperator(task_id="start")

    delete_unused_dbt_ls_cache_task = PythonOperator(
        dag=service_operations_dag,
        task_id='delete_unused_dbt_ls_cache_task',
        python_callable=delete_unused_dbt_ls_cache,
        op_kwargs={"max_age_last_usage": timedelta(days=5)}
    )

    delete_orphan_models = BashOperator(
        task_id='delete_orphan_models',
        bash_command=""" \
            export DBT_PROJECT_DIR=/home/airflow/gcs/dags/dbt/project_name \
            && export DBT_PROFILES_DIR=/home/airflow/gcs/dags/dbt/project_name \
            && set -a && source /home/airflow/gcs/dags/.env.composer && set +a \
            && dbt run-operation cleanup_dataset --args '{\"dry_run\": False}'
        """
    )

    end = EmptyOperator(task_id="end")

    (start >> [delete_unused_dbt_ls_cache_task, delete_orphan_models] >> end)
