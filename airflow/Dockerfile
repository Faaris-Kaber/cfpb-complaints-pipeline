FROM apache/airflow:3.3.2-python3.12

# Providers and the ingestion package's dependencies go in Airflow's environment.
RUN pip install --no-cache-dir \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "apache-airflow-providers-snowflake" \
    "requests>=2.32" "boto3>=1.34" "tenacity>=8.2"

# dbt gets its own virtualenv so its pins never fight Airflow's.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
       "dbt-core>=1.9,<2.0" "dbt-snowflake>=1.9,<2.0"

ENV DBT_BIN=/home/airflow/dbt-venv/bin/dbt \
    CFPB_PROJECT_ROOT=/opt/airflow/project \
    PYTHONPATH=/opt/airflow/project
