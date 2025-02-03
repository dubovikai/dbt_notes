#!/usr/bin/env bash

# we don't need to push this container anywhere, so we can install the linter in-place
apt update && apt install -y python3.11-venv
python3 -m venv /opt/venv
/opt/venv/bin/pip install sqlfluff==3.0.7

echo "Running SQL linter tests..."
# we use explicit exit, because CloudBuild doesn't catch non-zero exit code for some mysterious reason
/opt/venv/bin/sqlfluff lint dags/dbt/project_name
