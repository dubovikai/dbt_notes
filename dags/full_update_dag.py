# from airflow import DAG
from cosmos import DbtDag
import datetime
from airflow.utils.trigger_rule import TriggerRule
from config import DBTConfig
from utils import (
    publish_pubsub_event,
    on_failure_callbacks_list,
    control_last_execution_date
)


full_update_dag = DbtDag(
    dag_id="full_update_dag",
    start_date=datetime.datetime(2024, 7, 4),
    schedule="@daily",
    project_config=DBTConfig.project_config(),
    profile_config=DBTConfig.profile_config(),
    execution_config=DBTConfig.execution_config(),
    max_active_runs=1,
    render_config=DBTConfig.render_config(
        exclude=[
            'tag:updated_only_manually'
        ]
    ),
    operator_args={
        "retries": 1,
        "trigger_rule": TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        "on_failure_callback": on_failure_callbacks_list,
        "on_success_callback": publish_pubsub_event,
        "on_execute_callback": control_last_execution_date,
        "install_deps": True
    },
    catchup=False,
)
