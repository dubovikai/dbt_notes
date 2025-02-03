#!/usr/bin/env bash

echo "= = = = = = = = = = = Deployment Pipeline to [$BRANCH_NAME] branch started  = = = = = = = = = ="

# load env-related variables, if any (primarily, AIRFLOW_BUCKET for the relevant env must be loaded)
echo "Target bucket detected: [$AIRFLOW_BUCKET]. Deploying updated files..."

gsutil -m rsync -r -d -x ".*__pycache__|config.py|airflow_monitoring.py|.*dbt_packages" dags $AIRFLOW_BUCKET/dags
gsutil cp $CONFIG_PATH $AIRFLOW_BUCKET/dags/config.py

echo "= = = = = = = = = = = Deployment Pipeline to [$BRANCH_NAME] branch completed  = = = = = = = = = ="
