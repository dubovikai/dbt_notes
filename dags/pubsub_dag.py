from datetime import datetime
from airflow import DAG
from utils import (
    publish_pubsub_event,
    on_failure_callbacks_list,
    control_last_execution_date
)


with DAG(
    dag_id="pubsub_dag",
    start_date=datetime(2024, 7, 4),
    schedule_interval="10 * * * *",
    max_active_runs=1,
    catchup=False,
    render_template_as_native_obj=True,
    default_args={"on_failure_callback": on_failure_callbacks_list},
) as pubsub_dag:

    import json

    from airflow.exceptions import AirflowSkipException
    from airflow.utils.trigger_rule import TriggerRule
    from airflow.utils.state import DagRunState
    from airflow.providers.google.cloud.operators.pubsub import PubSubPullOperator
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    from airflow.operators.empty import EmptyOperator
    from airflow.models import DagRun
    from cosmos import DbtTaskGroup
    from config import DBTConfig

    # check if full update dag is not in the process
    def check_daily_dag_success_today(**context):
        dag_runs = DagRun.find(dag_id='full_update_dag')

        if not dag_runs:
            print('No DAG runs found for full_update_dag. Continuing..')
            return
        dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
        latest_daily_dag_run = dag_runs[0]

        if latest_daily_dag_run.state in [DagRunState.RUNNING, DagRunState.QUEUED]:
            print('full_update_dag is in the process. Skipping..')
            raise AirflowSkipException

        print('full_update_dag is not in the process. Continuing..')

    check_daily_dag = PythonOperator(
        task_id='check_daily_dag', 
        python_callable=check_daily_dag_success_today,
    )

    # PubSub tasks
    def handle_messages(pulled_messages, context):
        model_task_ids = set()
        print(f'Number of received messages: {len(pulled_messages)}')
        for m in pulled_messages:
            attributes = m.message.attributes
            data = json.loads(m.message.data.decode('utf-8'))
            print(f"Decoded message: attributes={attributes}, data={data}")
            if attributes.get("event-type") != 'dbt':
                continue
            table_name = attributes.get("table-name")
            if not table_name:
                continue

            task_id = f"run_dbt_models.{table_name}_run"

            if not table_name or task_id not in pubsub_dag.task_ids:
                print(f"Table skipped {table_name}, not in models...")
                continue                
            model_task_ids.add(task_id)

        print(f"Task ids for run = {list(model_task_ids)}")

        return list(model_task_ids)

    pull_messages_operator = PubSubPullOperator(
        task_id="pull_pubsub_messages",
        project_id=DBTConfig.PIPELINE_EVENTS_PUBSUB_PROJECT_ID,
        ack_messages=True,
        messages_callback=handle_messages,
        subscription=DBTConfig.PIPELINE_EVENTS_PUBSUB_SUBSCRIPTION,
        max_messages=1000,
    )

    def get_task_ids_for_run(**context):
        task_instance = context['ti']
        tasks = task_instance.xcom_pull(task_ids='pull_pubsub_messages')
        return tasks

    # DBT Model Selector:
    dbt_model_selector = BranchPythonOperator(
        task_id="dbt_model_selector",
        python_callable=get_task_ids_for_run,
        provide_context=True
    )

    # DBT Tasks:
    run_dbt_models = DbtTaskGroup(
        group_id="run_dbt_models",
        render_config=DBTConfig.render_config(
            exclude=[
                'tag:not_updated_by_pubsub',
                'tag:updated_only_manually'
            ]
        ),
        project_config=DBTConfig.project_config(),
        execution_config=DBTConfig.execution_config(),
        profile_config=DBTConfig.profile_config(),
        default_args={
            "retries": 1,
            "trigger_rule": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
            "on_success_callback": publish_pubsub_event,
            "on_execute_callback": control_last_execution_date,
            "install_deps": True
        }
    )

    # Last Empty Task:
    last_task = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    (check_daily_dag >> pull_messages_operator >> dbt_model_selector >> run_dbt_models >> last_task)
