import json
from typing import Dict
import os
import types
from datetime import datetime as dt, timedelta, timezone

from sqlalchemy import func

from google.cloud import pubsub_v1
from airflow.exceptions import AirflowSkipException
from airflow.models.taskinstance import TaskInstance
from airflow.providers.slack.notifications.slack import send_slack_notification
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from config import DBTConfig


SLACK_NOTIFICATION_CHANNEL = '...'


on_failure_callbacks_list = [
    send_slack_notification(
        text="""
            :red_circle: *""" + DBTConfig.ENVIRONMENT + """* DBT Task Failed. 
            *Task*: {{ ti.task_id }}  
            *Dag*: {{ dag.dag_id }} 
            *Execution Time*: {{ logical_date.strftime("%Y-%m-%d %H:%M:%S %Z") }}
            *Log Url*: <{{ ti.log_url }}|Logs>
        """,
        channel=os.environ['SLACK_NOTIFICATION_CHANNEL'] if os.environ.get('SLACK_NOTIFICATION_CHANNEL') else SLACK_NOTIFICATION_CHANNEL,
    )
]


def publish_pubsub_event(context: Dict):
    """
    Publish PubSub message to DBT Events topic on successful table refreshing.
    For context example json see features/airflow_context_example.json
    """
    if context.get('dbt_node_config', {}).get('config', {}).get('materialized') != 'table':
        print('Publishing message to PubSub skipped; not a table')
        return

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(DBTConfig.DBT_EVENTS_PUBSUB_PROJECT_ID, DBTConfig.DBT_EVENTS_PUBSUB_TOPIC)

    attributes_dict = {
        "app-name": "APP name",
        "app-environment": DBTConfig.ENVIRONMENT.lower(),
        "sent-at": dt.now(timezone.utc).replace(microsecond=0).isoformat(),
        "app-host": f'...',
        "event": "...",
        "event-type": "...",
        "table_name": context.get('dbt_node_config', {}).get('name')
    }
    data_dict = {
        "table_name": context.get('dbt_node_config', {}).get('name')
    }

    future = publisher.publish(
        topic=topic_path,
        data=json.dumps(data_dict).encode("utf-8"),
        # message attributes:
        **attributes_dict
    )
    future.result()
    print(f'Published message to PubSub topic: {topic_path}')


def get_config_param(task_id: str, config_param: str, manifest_path='/home/airflow/gcs/dags/dbt/project_name/target'):
    model_name = str(task_id).removesuffix('_run').split('.')[-1]

    manifest_file_path = f'{manifest_path}/manifest.json'
    if not os.path.exists(manifest_path):
        print('manifest.json not found; skipped')
        return

    with open(manifest_file_path, "r") as f:
        data = json.loads(f.read())
        for node_values in data['nodes'].values():
            if node_values['name'] == model_name:
                min_period = node_values['config'].get(config_param)
                return min_period
        print(f'Config not found for model {model_name}')


def get_last_execution(task_id):
    with create_session() as session:
        last_execution = (
            session.query(func.max(TaskInstance.start_date))
            .filter_by(
                task_id=task_id,
                state=TaskInstanceState.SUCCESS
            )
            .scalar()
        )
    return last_execution


def control_last_execution_date(context: Dict):
    minimal_interval = get_config_param(context['task'].task_id, config_param='min_period_between_updates_hours')

    if not minimal_interval:
        print('There are no restrictions on the frequency of updates...')
        return    

    last_execution = get_last_execution(context['task'].task_id)

    if not last_execution:
        print('Can not get last success execution time; processed')
        return
    time_delta: timedelta = dt.now(timezone.utc) - last_execution
    time_delta = time_delta.total_seconds() / 60 / 60
    if time_delta < minimal_interval:
        def skip_task(self, context):
            raise AirflowSkipException('Skipped on update frequency restriction')
        context['task'].execute = types.MethodType(skip_task, context['task'])
        print(f'Last update was earlier than {minimal_interval} hours ago ({last_execution}); skipped')
    else:
        print(f'Last update was later than {minimal_interval} hours ago ({last_execution}); processed')
