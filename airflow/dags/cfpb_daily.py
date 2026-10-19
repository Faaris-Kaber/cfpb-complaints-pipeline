"""Daily CFPB pipeline: API -> S3 -> Snowflake RAW -> dbt -> marts.

Runs on Airflow 3. Expected configuration (see docker-compose.yml / .env):
- AWS credentials for the extractor and loader (mounted ~/.aws from `aws login`)
- S3_BUCKET and SNOWFLAKE_* env vars (key-pair auth) for the loader and dbt
- optional SLACK_WEBHOOK_URL for failure alerts
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_ROOT = Path(os.environ.get("CFPB_PROJECT_ROOT", "/opt/airflow/project"))
DBT_DIR = PROJECT_ROOT / "dbt"
# dbt lives in its own virtualenv inside the image to avoid dependency clashes.
DBT_BIN = os.environ.get("DBT_BIN", "dbt")
DBT = f"cd {DBT_DIR} && {DBT_BIN} --no-use-colors"
DBT_ENV = {"DBT_PROFILES_DIR": str(DBT_DIR), "DBT_TARGET": "prod"}


def _alert_on_failure(context) -> None:
    """Post to Slack if SLACK_WEBHOOK_URL is set; otherwise just log."""
    import json
    import logging
    import urllib.request

    ti = context["task_instance"]
    msg = f":red_circle: CFPB pipeline failed: {ti.dag_id}.{ti.task_id} ({context['ds']})"
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        logging.getLogger(__name__).error(msg)
        return
    req = urllib.request.Request(
        url, data=json.dumps({"text": msg}).encode(), headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10)


default_args = {
    "owner": "analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": _alert_on_failure,
}


@dag(
    dag_id="cfpb_daily",
    description="Extract CFPB complaints to S3, load to Snowflake, build dbt models",
    schedule="0 9 * * *",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["cfpb", "elt"],
)
def cfpb_daily():
    @task(execution_timeout=timedelta(minutes=45))
    def extract_to_s3(ds: str | None = None) -> dict:
        from datetime import date

        from ingestion.cfpb_extract import run

        return run(mode="incremental", load_date=date.fromisoformat(ds))

    @task(execution_timeout=timedelta(minutes=30))
    def load_to_snowflake(ds: str | None = None) -> dict:
        # S3 partition -> Snowflake internal stage -> COPY INTO RAW.CFPB.COMPLAINTS.
        # force=True: re-runs reload the partition; staging dedupes on complaint_id.
        from ingestion.snowflake_loader import load_partition

        return load_partition(f"incremental/load_date={ds}/", force=True)

    source_freshness = BashOperator(
        task_id="dbt_source_freshness",
        bash_command=f"{DBT} deps && {DBT} source freshness",
        env=DBT_ENV,
        append_env=True,
    )

    # `dbt build` runs seeds, staging, the snapshot, then the rest of the DAG in
    # dependency order, testing each model right after it is built.
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{DBT} build",
        env=DBT_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=60),
    )

    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} docs generate",
        env=DBT_ENV,
        append_env=True,
        trigger_rule="all_done",
    )

    extract_to_s3() >> load_to_snowflake() >> source_freshness >> dbt_build >> dbt_docs


cfpb_daily()
