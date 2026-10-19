# Bank Complaints Analytics Pipeline: Spec Sheet

**Type:** Analytics engineering portfolio project
**Stack:** Python · AWS S3 · Snowflake · dbt · Airflow · GitHub Actions · Streamlit
**Status:** Built and running (Oct 2026). This document describes the system as built.

---

## 1. Goal

A production-style ELT pipeline that ingests the CFPB Consumer Complaint Database every day, models it into a tested star schema in Snowflake, and serves a dashboard comparing banks on complaint volume, mix and response behavior.

**Questions the dashboard answers:**
1. Which companies get the most complaints, overall and per product?
2. How is complaint volume trending month over month and year over year?
3. What share of each company's complaints get a timely response, and how are they resolved (relief, explanation, and so on)?
4. Which issues and states drive complaints?
5. How long do companies take to close complaints? (See the caveat in §5.4.)

### Resume claims and where they're met

| Claim | Where it's met |
|---|---|
| Incremental Python job ingesting 18M+ complaints from a public REST API into S3 | §3 |
| Snowflake + dbt staging / intermediate / mart layers → star schema | §4, §5 |
| 60+ dbt tests and freshness checks run in CI (GitHub Actions), with docs and lineage | §6, §7 |
| Daily Airflow runs feeding a Streamlit dashboard | §8, §9 |

The first full load held **18,504,768 complaints** (2026-10-19).

---

## 2. Architecture

```
CFPB API / bulk CSV ──► Python extractor ──► S3 (gzip NDJSON, partitioned by load_date)
                              │                         │
                       watermark in S3        Python loader: PUT to internal stage, COPY INTO
                                                        ▼
                                              RAW.CFPB.COMPLAINTS (VARIANT)
                                                        │  dbt
               staging (incremental) ─► snapshot ─► intermediate ─► star schema ─► reporting
                                                        │
                                                   Streamlit app

Orchestration: Airflow DAG (daily)    CI: GitHub Actions (lint, tests, dbt parse, PR dbt build, docs → Pages)
```

### Repository layout

```
CFPB/
├── ingestion/
│   ├── cfpb_client.py         # API client: day-by-day paging with search_after, retries
│   ├── cfpb_extract.py        # CLI / Airflow entry point: incremental + backfill → S3
│   ├── backfill.py            # bulk CSV download, header mapping
│   ├── s3_writer.py           # partitioned gzip NDJSON writer
│   ├── watermark.py           # incremental state in S3
│   ├── snowflake_loader.py    # S3 partition → internal stage → COPY INTO
│   └── tests/                 # pytest (API, S3 and Snowflake mocked)
├── aws/                       # bucket + pipeline-user setup script, IAM policy
├── snowflake/setup.sql        # warehouse, databases, roles, service users, stage, raw table
├── dbt/                       # models, snapshot, seeds, macros, singular tests
├── airflow/                   # cfpb_daily DAG + Docker image (Airflow 3, isolated dbt venv)
├── dashboard/app.py           # Streamlit app
├── .github/workflows/         # ci.yml, docs.yml
└── docker-compose.yml         # local Airflow
```

---

## 3. Ingestion (Python → S3)

### 3.1 Source

- **API:** `https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
- **Params that work** (verified Oct 2026): `date_received_min` and `date_received_max` (both inclusive), `size` (max 10,000; larger values return 424), `sort=created_date_asc`, `no_aggs=true`, `search_after`. `frm` is ignored and `format=json` returns 404.
- **Bulk file (backfill only):** `https://files.consumerfinance.gov/ccdb/complaints.csv.zip` (~350 MB, 15 columns, no narratives). The JSON export no longer exists.
- No auth. Requests retry with backoff.

### 3.2 Load strategy

| Mode | When | How |
|---|---|---|
| **Backfill** | One time | Download the bulk CSV zip, map its headers to API field names, write 100k-row gzip NDJSON files to `s3://<bucket>/raw/cfpb/backfill/` |
| **Incremental** | Daily | Pull `date_received` from (min(watermark, load_date) − lookback) through `load_date`, write to `s3://<bucket>/raw/cfpb/incremental/load_date=YYYY-MM-DD/part-NNNNN.json.gz`. Lookback is 30 days, or 90 days on Sundays. |
| **Custom window** | By hand | `--start/--end` writes `incremental/window=<start>_<end>/`, so it never replaces a scheduled partition |

**Why re-pull, and why two windows:** complaints change after they're filed. `company_response` moves from "In progress" to a closed status, and the share still open only drops below 0.2% about 60 days after receipt (31–37% are still open at 6–8 weeks). The daily 30-day re-pull catches most changes cheaply. The Sunday 90-day re-pull catches the rest, so no complaint's status freezes. dbt keeps the newest version (§5.1). The CFPB also publishes complaints with a lag of up to ~15 days, so recent days fill in over the following two weeks.

**Pagination:** one day at a time (~15–30k complaints/day). Within a day, sort by `created_date_asc` and page with `search_after=<sort0>_<sort1>` from the previous page's last hit. The day fails if fewer rows come back than `hits.total` reports.

### 3.3 Behavior

- Retries with exponential backoff (`tenacity`) on 429/5xx, connection errors, timeouts, and truncated or garbled response bodies. Other HTTP errors fail immediately.
- Every record gets `_extracted_at` (UTC) and `_source` (`api` or `bulk_csv`).
- The watermark (`max(date_received)` seen) lives in `s3://<bucket>/state/cfpb_watermark.json` and only moves forward, after every file is written.
- Idempotent and safe to re-run: files go to `raw/cfpb/_staging/<id>/` first and only replace the partition once the whole extract has succeeded, so a failed re-run leaves the previous partition intact. Re-running a date older than the lookback also works. S3 delete errors are checked, not ignored.
- Config: `S3_BUCKET`, `AWS_REGION`, `S3_RAW_PREFIX`, `S3_STATE_KEY`, `LOOKBACK_DAYS`, `DEEP_LOOKBACK_DAYS`, `PAGE_SIZE`, `ROWS_PER_FILE`.
- CLI: `python -m ingestion.cfpb_extract --mode incremental|backfill [--load-date D] [--lookback-days N] [--start D --end D] [--zip-path FILE]`. Its printed summary includes the `partition` to pass to the loader.

### 3.4 S3 and AWS access

- Bucket `cfpb-complaints-faaris` (us-east-2): versioning, SSE-S3 encryption, public access blocked, `raw/` moves to Infrequent Access after 90 days.
- **Interactive runs** use the developer's `aws login` session: short-lived credentials that refresh automatically for up to 12 hours.
- **Scheduled runs** need a credential that outlives that session. `aws/setup_aws.sh pipeline-user` creates the IAM user `cfpb-pipeline`, which can only list the bucket and read/write `raw/cfpb/` and `state/`. Its access key is stored in a local AWS profile that only the Airflow container uses (`PIPELINE_AWS_PROFILE`). If credentials are missing, misconfigured (unknown profile, invalid key) or expired, the Airflow tasks fail immediately with instructions instead of retrying.

---

## 4. Snowflake

### 4.1 Objects (`snowflake/setup.sql`)

| Object | Name |
|---|---|
| Warehouse | `TRANSFORM_WH` (XS, auto-suspend 60s) |
| Databases | `RAW`, `ANALYTICS` (`PROD` for scheduled runs, `<DEV>_*` and `PR_<n>_*` schemas for development and CI) |
| Roles | `LOADER` (writes RAW), `TRANSFORMER` (reads RAW, owns ANALYTICS), `REPORTER` (reads PROD) |
| Users | `CFPB_SVC` (LOADER + TRANSFORMER), `CFPB_DASHBOARD` (REPORTER), both key-pair auth only |
| File format | `JSON_GZ` |
| Stage | `RAW.CFPB.CFPB_STAGE` (internal, see §4.2) |
| Raw table | `RAW.CFPB.COMPLAINTS (record VARIANT, _file STRING, _file_row NUMBER, _loaded_at TIMESTAMP_NTZ)`. `_loaded_at` is UTC from 2026-10-19 on; the first loads before that recorded Pacific time. Incremental loads only rely on the ordering, and the switch moved new values later, so nothing was skipped. |

### 4.2 Load

The AWS account is a "new AWS experience" project. Its AWS-managed resource control policy denies `s3:*` and `sts:*` to principals outside the AWS organization, so a Snowflake storage integration can't read the bucket. Instead, `ingestion/snowflake_loader.py` runs with our own AWS credentials:

1. List the partition's files in S3 and download them in batches of 20.
2. `PUT` each batch to `@RAW.CFPB.CFPB_STAGE/<partition>/` (already gzipped, so `AUTO_COMPRESS = FALSE`).
3. Run `COPY INTO RAW.CFPB.COMPLAINTS ... PURGE = TRUE`. Daily loads use `FORCE = TRUE`; the backfill uses `FORCE = FALSE`.

```sql
copy into raw.cfpb.complaints (record, _file, _file_row, _loaded_at)
from (select $1, metadata$filename, metadata$file_row_number, sysdate()
      from @raw.cfpb.cfpb_stage/incremental/load_date=2026-10-19/)
file_format = (format_name = 'RAW.CFPB.JSON_GZ')
on_error = 'ABORT_STATEMENT' purge = true force = true;
```

The raw table is append-only. Deduplication happens in dbt.

---

## 5. dbt modeling

dbt-core with dbt-snowflake, plus `dbt_utils` and `dbt_expectations`.

### 5.1 Staging: `stg_cfpb__complaints`

- Typed columns: `complaint_id`, `date_received`, `date_sent_to_company`, `product`, `sub_product`, `issue`, `sub_issue`, `company`, `state`, `zip_code`, `zip3`, `submitted_via`, `company_response`, `company_public_response`, `tags`, `is_timely_response`, `is_consumer_disputed`, `extracted_at`, `extract_source`, `source_file`, `loaded_at`.
- Cleaning: trim, `''`/`None`/`N/A` → NULL, upper-case state (and `UNITED STATES MINOR OUTLYING ISLANDS` → `UM`), Yes/No → boolean.
- **Incremental merge** on `complaint_id`, reading only raw rows loaded since the last run. Within a batch the newest `extracted_at` wins, and a batch row never replaces a newer version already in the table (so reloading an old partition can't roll data back).
- Source freshness on `_loaded_at`: warn after 26h, error after 50h.

### 5.2 Snapshot: `snap_complaints_status`

`strategy='check'` on `company_response`, `is_timely_response`, `company_public_response`. It records the day each complaint's status changes, which makes the response-time metric possible (§5.4).

### 5.3 Intermediate

| Model | Purpose |
|---|---|
| `int_companies__normalized` | Maps raw legal-entity names to a parent brand and company type (seed `company_reference`); unmapped names keep their raw name and type `other` |
| `int_complaints__resolution_dates` | From the snapshot: the first day a complaint seen "In progress" was observed closed (`observed_closed_on`) |
| `int_complaints__enriched` | Staging + company + resolution data; `resolution_category` (relief / explanation / closed_no_relief / untimely / in_progress / unknown), flags, `days_to_forward`, `days_to_close_observed` |

### 5.4 Marts: star schema

**Fact:** `fct_complaints`, one row per complaint. Incremental merge on `complaint_id` (rows whose `loaded_at` is newer than the table's max), clustered by `date_received_key`.

| Column | Notes |
|---|---|
| `complaint_id` | PK |
| `date_received_key`, `company_key`, `product_key`, `issue_key`, `geography_key`, `channel_key` | FKs |
| `company_legal_name`, `company_response`, `company_public_response`, `resolution_category` | degenerate dimensions |
| `is_timely_response`, `is_monetary_relief`, `is_relief`, `is_in_progress`, `is_servicemember`, `is_older_american`, `is_consumer_disputed` | flags |
| `days_to_forward` | days from receipt to company |
| `observed_closed_on`, `days_to_close_observed` | from the snapshot; NULL for complaints already closed when first seen |

**Dimensions**

| Dimension | Grain | Key attributes |
|---|---|---|
| `dim_date` | day (Dec 2011 → today + 1 year) | year, quarter, month, month_start, week_start, is_weekend |
| `dim_company` | legal entity (raw company name) | legal_name, parent_company, company_type, is_bank, is_mapped. Keyed on the legal name, so remapping a parent in the seed never orphans fact rows. |
| `dim_product` | product + sub_product | product, sub_product, product_group (stable across CFPB renames) |
| `dim_issue` | issue + sub_issue | issue, sub_issue |
| `dim_geography` | state + ZIP3 | state_name, census_region, state_population_2020 |
| `dim_submission_channel` | channel | channel, is_digital |

**Reporting** (what the dashboard reads)
- `rpt_company_monthly`: parent company × month (legal entities rolled up through `dim_company`): complaints, timely %, relief %, median days to forward and to close.
- `rpt_product_trends`: product group × month with prior-month and prior-year counts (joined by calendar month) and YoY %.
- `rpt_state_monthly`: state × month: complaints and bank complaints, with population for per-capita rates.

> **Response-time caveat:** the public data has no company response date, only `date_received`, `date_sent_to_company` and a `timely` flag. The dashboard shows (a) the **timely response rate**, which covers all history, and (b) **observed days to close** from the daily snapshot, which only exists from the day the pipeline started and is labeled as such.

### 5.5 Seeds

- `company_reference.csv`: raw company name → parent brand and type (bank, credit bureau, fintech, debt collector, servicers, lenders). Covers the ~80 largest companies, 93% of complaint volume.
- `state_reference.csv`: states, DC and Puerto Rico with census region and 2020 population.

---

## 6. Data quality: 66 dbt tests

| Layer | Tests | What they check |
|---|---|---|
| Seeds | 5 | unique and not-null keys, accepted company types |
| Staging | 13 | unique/not-null `complaint_id`; required fields; accepted values for `company_response` and `submitted_via`; 2-letter state codes; date sanity; fact and staging row counts match; daily-volume anomaly check |
| Intermediate | 10 | unique keys; resolution categories; `days_to_forward` range; close date after first-seen-open date |
| Dimensions | 15 | unique and not-null surrogate keys; unique parent company; product group present |
| Fact | 16 | unique/not-null PK; not-null and `relationships` tests for all 6 FKs; accepted resolution categories; non-negative days to close |
| Reporting | 6 | unique grain per table; rates between 0 and 1; population present |
| Singular | 1 | at least 90% of complaints map to a known company |

Plus **source freshness** (warn after 26h, error after 50h). Five tests are warnings by design: two that flag known CFPB data quirks (7,050 bulk rows have the send date one day before the receive date; 241 complaints took over a year to forward), the volume-anomaly check, the company-coverage check, and the `submitted_via` values check.

The **volume-anomaly check** compares each day 15–21 days ago (old enough to be fully published) with the average for the same weekday over the previous four weeks, counting days with no rows as zero. It fails below 50%. On prod data it passes today, and it fires when one day is cut to 40% or removed.

---

## 7. CI/CD (GitHub Actions)

### `ci.yml` (pull requests and pushes to `main`)
1. `ruff check`, `ruff format --check`, `pytest`.
2. `sqlfluff lint` on `dbt/models` and `dbt/tests` (jinja templater with dbt_utils stubs, so it needs no warehouse).
3. `dbt deps && dbt parse`.
4. On pull requests, when the repo variable `SNOWFLAKE_ENABLED` is `true`: `dbt build --select state:modified+ --defer --state prod-artifacts` into a per-PR schema (`PR_<number>_*`), using the prod manifest published by `docs.yml`. Falls back to `dbt build --empty` when no prod manifest exists. `dbt source freshness` runs too. The PR schemas are dropped at the end.

### `docs.yml` (pushes to `main`)
1. `dbt docs generate --static` against prod.
2. Publish the docs and lineage graph to **GitHub Pages**.
3. Upload `manifest.json` as the `prod-manifest` artifact for slim CI.

**Secrets:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY`. Role and warehouse are set in the workflow.

---

## 8. Orchestration (Airflow)

**DAG:** `cfpb_daily`. Schedule `0 9 * * *` (UTC), `catchup=False`, `max_active_runs=1`, 2 retries 10 minutes apart.

```
extract_to_s3 ─► load_to_snowflake ─► dbt_source_freshness ─► dbt_build (incl. snapshot) ─► dbt_docs_generate
```

| Task | Implementation |
|---|---|
| `extract_to_s3` | `@task`: `ingestion.cfpb_extract.run(mode="incremental", load_date=dag_run.run_after.date())`, with `lookback_days=90` on Sundays. Airflow 3 manual runs have no logical date, so the date comes from `run_after`. |
| `load_to_snowflake` | `@task`: `ingestion.snowflake_loader.load_partition(...)` for the `partition` the extract task returned |
| `dbt_source_freshness`, `dbt_build`, `dbt_docs_generate` | `BashOperator` running `dbt deps` and then the command, from dbt's own virtualenv with `DBT_TARGET=prod`. Packages, artifacts and logs stay in the container. |

- Runs locally with Docker Compose: Airflow 3.3 in a single `airflow standalone` container, UI on `127.0.0.1:8080`, metadata kept in a named volume.
- Failure alerts post to `SLACK_WEBHOOK_URL` if set, otherwise they're logged.
- `dbt_docs_generate` is the only leaf task and uses the default `all_success` trigger rule, so any upstream failure marks the run failed.

---

## 9. Dashboard (Streamlit)

- Connects to `ANALYTICS.PROD` as `CFPB_DASHBOARD` (role `REPORTER`) through `st.connection("snowflake")`. Queries are cached for an hour.
- Reads only the star schema and `rpt_*` tables.

| Section | Visual |
|---|---|
| KPI row | Complaints in range, last complete month vs prior, timely response %, relief % |
| Trends | Monthly complaints per company (complete months only) |
| Leaderboard | Complaints, timely %, relief %, and median observed days to close once data exists |
| Products by company | Heatmap of complaint counts by product group |
| Top issues | Top 10 issues for a chosen company |
| Year over year | YoY % change by product group for the last complete month |
| Geography | Complaints per 100k residents by state |

**Filters:** month range, banks-only toggle, companies (defaults to the top 10).
**Deploy:** Streamlit Community Cloud from `dashboard/app.py`, with the Snowflake key in the app's secrets.

---

## 10. Non-functional

| Area | Target / actual |
|---|---|
| Daily runtime | About 3 minutes end to end |
| Snowflake cost | XS warehouse with 60s auto-suspend; the full prod build takes about 30 seconds |
| Idempotency | Any DAG run, including old ones, can be re-run safely |
| Security | No credentials in the repo; key-pair auth for Snowflake; least-privilege roles; the pipeline IAM user can only touch this bucket's `raw/` and `state/` |

---

## 11. Milestones

| # | Deliverable | Status |
|---|---|---|
| M1 | AWS and Snowflake setup | Done |
| M2 | Backfill | Done: 18,504,768 complaints |
| M3 | Incremental extractor | Done: watermark, daily 30-day + weekly 90-day re-pull, staged partition swaps, 24 pytest tests |
| M4 | dbt staging and intermediate | Done |
| M5 | Star schema and snapshot | Done: 66 tests, prod build green |
| M6 | CI and docs | Done: CI green; docs live on GitHub Pages |
| M7 | Airflow DAG | Scheduled and manual runs succeed; unattended daily runs need the pipeline IAM user's access key (§3.4) |
| M8 | Dashboard | Runs locally; deploying to Streamlit Community Cloud needs the owner's account |

---

## 12. Stretch goals

- Snowpipe-style auto-ingest
- Elementary for test-result monitoring
- Terraform for S3, IAM and the Snowflake objects
