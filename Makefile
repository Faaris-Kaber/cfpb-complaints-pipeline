# Run from the repo root. On Windows use Git Bash (make via `choco install make`)
# or copy the commands. Assumes .env is filled in and the venv is active.
include .env
export

DBT = cd dbt && DBT_PROFILES_DIR=. dbt

.PHONY: setup test lint backfill extract load-backfill dbt-deps dbt-build dbt-docs dashboard airflow

setup:
	python -m venv .venv
	.venv/Scripts/python -m pip install -e ".[dev,dbt,dashboard]" || .venv/bin/python -m pip install -e ".[dev,dbt,dashboard]"

test:
	pytest -q

lint:
	ruff check . && ruff format --check .
	sqlfluff lint dbt/models dbt/tests

backfill:            ## one-time: bulk CSV -> s3://$S3_BUCKET/raw/cfpb/backfill/
	python -m ingestion.cfpb_extract --mode backfill

extract:             ## today's incremental partition
	python -m ingestion.cfpb_extract --mode incremental

dbt-deps:
	$(DBT) deps

dbt-build: dbt-deps
	$(DBT) source freshness
	$(DBT) build

dbt-docs: dbt-deps
	$(DBT) docs generate && $(DBT) docs serve --port 8081

dashboard:
	streamlit run dashboard/app.py

airflow:
	docker compose up --build
