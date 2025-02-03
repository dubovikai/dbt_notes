from typing import Dict
from cosmos import (
    ProjectConfig,
    ProfileConfig,
    RenderConfig,
    ExecutionConfig,
    ExecutionMode,
    LoadMode
)


class DBTConfig:

    ENVIRONMENT: str = 'STAGING'

    PIPELINE_EVENTS_PUBSUB_PROJECT_ID: str = "..."
    PIPELINE_EVENTS_PUBSUB_SUBSCRIPTION: str = "..."

    DBT_EVENTS_PUBSUB_PROJECT_ID: str = "..."
    DBT_EVENTS_PUBSUB_TOPIC: str = "..."

    DAGS_PATH: str = "/home/airflow/gcs/dags"

    @classmethod
    def profile_config(cls, **kwargs) -> 'ProfileConfig':
        return ProfileConfig(
            profile_name="dbt",
            target_name="dev",
            profiles_yml_filepath=f"{cls.DAGS_PATH}/dbt/project_name/profiles.yml",
            **kwargs
        )

    @classmethod
    def project_config(cls, **kwargs) -> 'ProjectConfig':
        return ProjectConfig(
            dbt_project_path=f"{cls.DAGS_PATH}/dbt/project_name",
            env_vars=cls.read_env_file(f"{cls.DAGS_PATH}/.env.composer"),
            manifest_path=f"{cls.DAGS_PATH}/dbt/project_name/target/manifest.json",
            **kwargs
        )

    @classmethod
    def execution_config(cls, **kwargs) -> 'ExecutionConfig':
        return ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            **kwargs
        )

    @classmethod
    def render_config(cls, **kwargs) -> 'RenderConfig':
        return RenderConfig(
            load_method=LoadMode.DBT_MANIFEST,
            enable_mock_profile=False,
            emit_datasets=False,
            **kwargs
        )

    @classmethod
    def read_env_file(cls, path: str) -> Dict[str, str]:
        env_vars = {}
        with open(path) as f:
            for line in f:
                if line.startswith('#') or not line.strip():
                    continue
                key, value = line.strip().split('=', 1)
                env_vars.update({key: value})
        return env_vars
