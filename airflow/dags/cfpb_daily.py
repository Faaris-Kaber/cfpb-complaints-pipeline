"""Daily CFPB pipeline: API -> S3 -> Snowflake RAW -> dbt -> marts.

Runs on Airflow 3. Expected configuration (see docker-compose.yml / .env):
- AWS credentials for the extractor and loader (mounted ~/.aws; the profile is
  PIPELINE_AWS_PROFILE, see README "Credentials for scheduled runs")
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
DBT_ENV = {
    "DBT_PROFILES_DIR": str(DBT_DIR),
    "DBT_TARGET": "prod",
    # Keep dbt's packages, artifacts and logs inside the container. DBT_DIR is a bind
    # mount of the host repo, and sharing these folders with a host-side dbt (or a
    # Windows filesystem) makes `dbt deps` fail midway through reinstalling packages.
    "DBT_PACKAGES_INSTALL_PATH": "/home/airflow/dbt_packages",
    "DBT_TARGET_PATH": "/home/airflow/dbt_target",
    "DBT_LOG_PATH": "/home/airflow/dbt_logs",
}


def _alert_on_failure(context) -> None:
    """Post to Slack if SLACK_WEBHOOK_URL is set; otherwise just log."""
    import json
    import logging
    import urllib.request

    # Manual runs in Airflow 3 have no logical date, so there is no `ds`; use run_id.
    ti = context["task_instance"]
    msg = f":red_circle: CFPB pipeline failed: {ti.dag_id}.{ti.task_id} ({ti.run_id})"
    url = os.environ.get("SLACK_WEBHOOK_URL")
    if not url:
        logging.getLogger(__name__).error(msg)
        return
    req = urllib.request.Request(
        url, data=json.dumps({"text": msg}).encode(), headers={"Content-Type": "application/json"}
    )
    urllib.request.urlopen(req, timeout=10)


# Credential problems that a retry 10 minutes later can't fix.
_AWS_CREDENTIAL_ERRORS = {
    "LoginRefreshRequired",
    "TokenRetrievalError",
    "NoCredentialsError",
    "PartialCredentialsError",
    "ProfileNotFound",
}
_AWS_CREDENTIAL_ERROR_CODES = {
    "ExpiredToken",
    "ExpiredTokenException",
    "InvalidAccessKeyId",
    "SignatureDoesNotMatch",
    "InvalidClientTokenId",
}


def _fail_fast_on_expired_aws_session(exc: Exception) -> None:
    """Missing or expired AWS credentials won't fix themselves on retry; fail clearly."""
    from airflow.exceptions import AirflowFailException

    # botocore ClientError carries a dict response; requests' HTTPError carries a
    # Response object, so only read the error code from a dict.
    resp = getattr(exc, "response", None)
    code = resp.get("Error", {}).get("Code") if isinstance(resp, dict) else None
    if type(exc).__name__ in _AWS_CREDENTIAL_ERRORS or code in _AWS_CREDENTIAL_ERROR_CODES:
        raise AirflowFailException(
            f"AWS credentials are missing, misconfigured or expired ({type(exc).__name__}"
            f"{': ' + code if code else ''}). Run `aws login` on the host, or check "
            "PIPELINE_AWS_PROFILE (README: credentials for scheduled runs), then clear this task."
        ) from exc


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
    def extract_to_s3(dag_run=None) -> dict:
        # Airflow 3 manual runs have no logical date (so no `ds`); run_after is
        # always set and stays fixed when a run is cleared and re-run.
        from ingestion.cfpb_extract import run
        from ingestion.config import Settings

        # Statuses keep changing for ~60 days after receipt, past the daily 30-day
        # window, so Sunday runs re-pull the last DEEP_LOOKBACK_DAYS (90) instead.
        run_day = dag_run.run_after.date()
        deep = run_day.weekday() == 6
        lookback = Settings.from_env().deep_lookback_days if deep else None
        try:
            return run(mode="incremental", load_date=run_day, lookback_days=lookback)
        except Exception as exc:
            _fail_fast_on_expired_aws_session(exc)
            raise

    @task(execution_timeout=timedelta(minutes=30))
    def load_to_snowflake(extract: dict) -> dict:
        # S3 partition -> Snowflake internal stage -> COPY INTO RAW.CFPB.COMPLAINTS.
        # The partition comes from the extract task's result, so the two always agree.
        # force=True: re-runs reload the partition; staging dedupes on complaint_id.
        from ingestion.snowflake_loader import load_partition

        try:
            return load_partition(extract["partition"], force=True)
        except Exception as exc:
            _fail_fast_on_expired_aws_session(exc)
            raise

    # Every dbt task installs packages first (a few seconds): they live in the container,
    # so any task re-run on its own after a container restart still finds them.
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
        bash_command=f"{DBT} deps && {DBT} build",
        env=DBT_ENV,
        append_env=True,
        execution_timeout=timedelta(minutes=60),
    )

    # Default all_success trigger rule: this is the DAG's only leaf task, so it must
    # not succeed when an upstream task failed, or the run would be marked green.
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command=f"{DBT} deps && {DBT} docs generate",
        env=DBT_ENV,
        append_env=True,
    )

    load_to_snowflake(extract_to_s3()) >> source_freshness >> dbt_build >> dbt_docs


cfpb_daily()
