#!/bin/bash
export BRANCH_NAME=develop
export AIRFLOW_BUCKET=...
export CONFIG_PATH=dags/config.py

source .cloudbuild/scripts/copy_files.sh
