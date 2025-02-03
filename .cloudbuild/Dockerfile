FROM python:3.11.8-slim

WORKDIR /opt/project

COPY requirements.txt .
COPY dags/dbt/project_name/ .

RUN pip install -r requirements.txt

RUN dbt deps