# Bank Complaints Analytics Pipeline: Spec Sheet

**Type:** Analytics engineering portfolio project
**Stack:** Python · AWS S3 · Snowflake · dbt · Airflow · GitHub Actions · Streamlit
**Status:** Code scaffolded (Oct 2026); cloud setup pending

---

## 1. Goal

Build a production-style ELT pipeline that ingests the CFPB Consumer Complaint Database every day, models it into a tested star schema in Snowflake, and serves a dashboard comparing banks on complaint volume, mix, and response behavior.

**Questions the dashboard answers:**
1. Which companies get the most complaints, overall and per product?
2. How is complaint volume trending month over month and year over year?
3. What share of each company's complaints get a timely response, and how are they resolved (relief, explanation, and so on)?
4. Which issues and states drive each company's complaints?
5. How long do companies take to close complaints? (See the caveat in §5.4.)

### Resume claims and how this spec covers them

| Claim | Where it's met |
|---|---|
| Incremental Python job, 5M+ complaints, REST API → S3 | §3 |
| Snowflake + dbt staging / intermediate / mart → star schema | §4, §5 |
| 40+ dbt tests and freshness checks in CI (GitHub Actions), docs and lineage | §6, §7 |
| Daily Airflow schedule feeding a Streamlit dashboard | §8, §9 |

> The live API reported **18.35M complaints** on 2026-10-12, so "5M+" undersells it. Consider "18M+" on the resume.

---

## 2. Architecture

```
CFPB Complaints API ──► Python extractor ──► S3 (raw, partitioned NDJSON.gz)
                              │                         │
                       watermark in S3        Python loader → internal stage
                                                        │  COPY INTO
                                                        ▼
                                              RAW.CFPB.COMPLAINTS (VARIANT)
                                                        │  dbt
                         staging ─► snapshots ─► intermediate ─► marts (star schema)
                                                        │
                                                   Streamlit app

Orchestration: Airflow DAG (daily)      CI: GitHub Actions (lint, dbt build on PR, docs)
```

### Repository layout

```
CFPB/
├── ingestion/
│   ├── cfpb_extract.py        # API client, pagination, retries
│   ├── s3_writer.py           # partitioned gzip NDJSON writer
│   ├── watermark.py           # read/write state in S3
│   └── tests/                 # pytest (mocked API with responses/moto)
├── snowflake/
│   └── setup.sql              # db, schemas, warehouse, roles, stage, file format, raw table
├── dbt/
│   ├── models/{staging,intermediate,marts}/
│   ├── snapshots/
│   ├── macros/
│   ├── tests/                 # singular tests
│   └── dbt_project.yml
├── airflow/
│   └── dags/cfpb_daily.py
├── dashboard/
│   └── app.py
├── .github/workflows/{ci.yml,docs.yml}
├── docker-compose.yml         # local Airflow
├── pyproject.toml
└── README.md
```

---

## 3. Ingestion (Python → S3)

### 3.1 Source

- **API:** `https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/`
- **Params that work** (verified Oct 2026): `date_received_min` and `date_received_max` (both inclusive), `size` (max 10,000; larger values return 424), `sort=created_date_asc`, `no_aggs=true`, `search_after`. `frm` is ignored and `format=json` returns 404.
- **Bulk file (backfill only):** `https://files.consumerfinance.gov/ccdb/complaints.csv.zip` (~350 MB). The JSON export no longer exists.
- No auth. Treat it as rate-limited and back off politely.

### 3.2 Load strategy

| Mode | When | How |
|---|---|---|
| **Backfill** | One time | Download the bulk CSV zip, map its headers to API field names, and write ~100k-row gzip NDJSON files, upload to `s3://<bucket>/raw/cfpb/backfill/` |
| **Incremental** | Daily | Query the API with `date_received_min = watermark − 30 days`, paginate by date window, write to `s3://<bucket>/raw/cfpb/incremental/load_date=YYYY-MM-DD/part-NNNN.json.gz` |

**Why a 30-day lookback:** complaints change after they're filed. `company_response` moves from "In progress" to a closed status, and narratives are published later. Re-pulling a trailing window catches those updates, and dbt dedupes them downstream (§5.1).

**Pagination:** walk the range one day at a time (~15–25k complaints/day). Within a day, sort by `created_date_asc` and page with `search_after=<sort0>_<sort1>` taken from the previous page's last hit. Fail if fewer rows come back than `hits.total` reports.

### 3.3 Requirements

- Retries with exponential backoff (`tenacity`) on 429 and 5xx responses. Fail loudly after N attempts.
- Every record gets `_extracted_at` (UTC) and `_source` (`api` or `bulk`) added before writing.
- The watermark (`max(date_received)` loaded) lives in `s3://<bucket>/state/cfpb_watermark.json` and is updated **only after** every S3 write succeeds.
- Idempotent: re-running the same `load_date` overwrites that partition and creates no duplicates.
- Config comes from environment variables: `S3_BUCKET`, `AWS_REGION`, `LOOKBACK_DAYS`, `PAGE_SIZE`.
- CLI: `python -m ingestion.cfpb_extract --mode incremental|backfill [--start YYYY-MM-DD --end YYYY-MM-DD]`
- Logs: rows fetched, pages, files written, and elapsed time, in structured JSON.

### 3.4 S3

- Bucket: `cfpb-complaints-<suffix>` with versioning on and SSE-S3 encryption
- Lifecycle: move `raw/` to Infrequent Access after 90 days
- Access: the extractor and loader use the developer's `aws login` session (short-lived, auto-refreshing). No IAM users or access keys.

---

## 4. Snowflake

### 4.1 Objects (`snowflake/setup.sql`)

| Object | Name |
|---|---|
| Warehouse | `TRANSFORM_WH` (XS, auto-suspend 60s) |
| Databases | `RAW`, `ANALYTICS` (dev schemas per developer, `PROD` schemas for the scheduled run) |
| Roles | `LOADER`, `TRANSFORMER`, `REPORTER` |
| File format | `JSON_GZ` (`TYPE=JSON, COMPRESSION=GZIP, STRIP_OUTER_ARRAY=FALSE`) |
| Stage | `RAW.CFPB.CFPB_STAGE` (internal; see §4.2 for why) |
| Raw table | `RAW.CFPB.COMPLAINTS (record VARIANT, _file STRING, _file_row INT, _loaded_at TIMESTAMP_NTZ)` |

### 4.2 Load

The AWS account is a "new AWS experience" project, and its AWS-managed resource control policy denies `s3:*` and `sts:*` to principals outside the AWS organization. That rules out a Snowflake storage integration, because Snowflake's IAM user can't assume a role in the account. Instead, `ingestion/snowflake_loader.py` runs with our own AWS credentials:

1. List the partition's files in S3 and download them in batches of 20.
2. `PUT` each batch to `@RAW.CFPB.CFPB_STAGE/<partition>/` (already gzipped, so `AUTO_COMPRESS = FALSE`).
3. Run `COPY INTO RAW.CFPB.COMPLAINTS ... PURGE = TRUE`. Daily loads use `FORCE = TRUE`; the backfill uses `FORCE = FALSE`.

```sql
COPY INTO RAW.CFPB.COMPLAINTS (record, _file, _file_row, _loaded_at)
FROM (SELECT $1, METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, CURRENT_TIMESTAMP()
      FROM @RAW.CFPB.CFPB_STAGE/incremental/load_date=2026-10-18/)
FILE_FORMAT = (FORMAT_NAME = 'RAW.CFPB.JSON_GZ')
ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE FORCE = TRUE;
```

The raw table is append-only. Deduplication happens in dbt.

---

## 5. dbt modeling

Uses dbt-core with dbt-snowflake, plus the `dbt_utils` and `dbt_expectations` packages.

### 5.1 Staging: `stg_cfpb__complaints`

- Flatten the VARIANT into typed columns: `complaint_id`, `date_received`, `date_sent_to_company`, `product`, `sub_product`, `issue`, `sub_issue`, `company`, `state`, `zip_code`, `submitted_via`, `company_response`, `company_public_response`, `timely_response` (bool), `consumer_disputed`, `consumer_consent_provided`, `tags`, `has_narrative`, `_extracted_at`, `_loaded_at`
- Clean up: trim, normalize "N/A" and empty strings to NULL, uppercase state, cast Yes/No to boolean
- **Dedupe:** `qualify row_number() over (partition by complaint_id order by _extracted_at desc) = 1`
- Materialized as an **incremental** table (merge on `complaint_id`, reading only raw rows loaded since the last run), so the growing append-only raw table isn't re-deduplicated every day
- Source freshness on `RAW.CFPB.COMPLAINTS._loaded_at`: warn after 26h, error after 50h

### 5.2 Snapshot: `snap_complaints_status`

- `strategy='check'`, `check_cols=['company_response','timely_response','company_public_response']`
- Records when each complaint's status changes. This is what makes the response-time metric possible (§5.4).

### 5.3 Intermediate

| Model | Purpose |
|---|---|
| `int_complaints__enriched` | Joins the staging model to the company-name normalization seed, buckets `company_response` into a resolution category (relief / explanation / closed / in progress), and derives `days_to_forward = date_sent_to_company − date_received` |
| `int_complaints__resolution_dates` | From the snapshot, the first `dbt_valid_from` where the status left "In progress" becomes `observed_closed_at` |
| `int_companies__normalized` | Collapses legal-entity variants, such as "BANK OF AMERICA, NATIONAL ASSOCIATION", to a parent bank (seed `company_parent_map.csv`) |

### 5.4 Marts: star schema

**Fact:** `fct_complaints` (one row per complaint; incremental, `unique_key='complaint_id'`, `merge` strategy, picks up rows whose `loaded_at` is newer than the table's max)

| Column | Notes |
|---|---|
| `complaint_id` | PK |
| `date_received_key`, `company_key`, `product_key`, `issue_key`, `geography_key`, `channel_key` | FKs |
| `is_timely_response`, `is_disputed`, `has_narrative` | flags |
| `resolution_category` | relief / explanation / closed / in progress |
| `days_to_forward` | days from receipt to company |
| `days_to_close_observed` | from the snapshot; NULL for complaints closed before tracking began |

**Dimensions**

| Dimension | Grain | Key attributes |
|---|---|---|
| `dim_date` | day (`dbt_utils.date_spine`, 2011 to today+1y) | year, quarter, month, week, is_weekend |
| `dim_company` | parent company | company_name, parent_company, is_bank (seed) |
| `dim_product` | product + sub_product | product, sub_product |
| `dim_issue` | issue + sub_issue | issue, sub_issue |
| `dim_geography` | state + zip3 | state, state_name, region (seed), zip3 |
| `dim_submission_channel` | channel | web, phone, referral, and so on |

Surrogate keys come from `dbt_utils.generate_surrogate_key`.

**Reporting aggregates** (tables the dashboard reads)
- `rpt_company_monthly`: company × month: complaint count, timely %, relief %, median `days_to_close_observed`
- `rpt_product_trends`: product × month volume with YoY change

> **Response-time caveat:** the public dataset has no company response date. It only has `date_received`, `date_sent_to_company`, and a `timely` flag. The dashboard therefore shows (a) the **timely response rate**, which covers full history, and (b) the **observed days to close**, from snapshots, which only exists from the day the pipeline starts running. Label (b) clearly in the UI.

### 5.5 Seeds

- `company_parent_map.csv`
- `state_regions.csv`
- `bank_flags.csv` (marks which companies are banks vs. credit bureaus and debt collectors, so the dashboard can filter to banks)

---

## 6. Data quality: 40+ dbt tests (66 implemented)

| # | Model | Test |
|---|---|---|
| 1–2 | source `complaints` | freshness (warn and error thresholds) |
| 3–4 | stg_complaints | `complaint_id` unique, not_null |
| 5–7 | stg_complaints | not_null: `date_received`, `company`, `product` |
| 8 | stg_complaints | `accepted_values`: `submitted_via` |
| 9 | stg_complaints | `accepted_values`: `company_response` |
| 10 | stg_complaints | `state` matches `^[A-Z]{2}$` or is NULL (`dbt_expectations`) |
| 11 | stg_complaints | `date_received` ≤ current_date |
| 12 | stg_complaints | `date_sent_to_company` ≥ `date_received` (allowing a small tolerance) |
| 13–14 | int_enriched | `complaint_id` unique, not_null |
| 15 | int_enriched | `resolution_category` accepted_values |
| 16 | int_enriched | `days_to_forward` between 0 and 365 |
| 17 | int_companies | every raw company maps to a parent (no NULL `parent_company`) |
| 18–19 | fct_complaints | `complaint_id` unique, not_null |
| 20–25 | fct_complaints | `relationships` for all 6 FKs |
| 26–31 | fct_complaints | not_null on all 6 FKs |
| 32 | fct_complaints | row count equals stg row count (`dbt_utils.equal_rowcount`) |
| 33 | fct_complaints | `days_to_close_observed` ≥ 0 |
| 34–35 | dim_company | `company_key` unique, not_null |
| 36–37 | dim_product | `product_key` unique, not_null |
| 38–39 | dim_issue | `issue_key` unique, not_null |
| 40–41 | dim_geography | `geography_key` unique, not_null |
| 42–43 | dim_date | `date_key` unique, not_null |
| 44–45 | dim_submission_channel | `channel_key` unique, not_null |
| 46 | rpt_company_monthly | unique combination of (`company_key`, `month`) |
| 47 | rpt_company_monthly | `timely_pct` between 0 and 1 |
| 48 | singular | daily volume isn't more than 3σ below the 30-day average (catches a silently broken ingest) |

**Severity:** freshness and the anomaly test use `warn`. Everything else uses `error`.

---

## 7. CI/CD (GitHub Actions)

### `ci.yml` (on pull request)
1. `ruff` and `pytest` for `ingestion/`
2. `sqlfluff lint` (dbt templater) for `dbt/models`
3. `dbt deps && dbt build --select state:modified+ --defer --state prod-artifacts/`, run against a per-PR schema (`PR_<number>`)
4. `dbt source freshness`
5. Drop the PR schema at the end

### `docs.yml` (on push to `main`)
1. `dbt docs generate` against prod
2. Publish the `target/` docs and lineage graph to **GitHub Pages**
3. Upload `manifest.json` as the prod state artifact for slim CI

**Secrets:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY` (key-pair auth), `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE`

---

## 8. Orchestration (Airflow)

**DAG:** `cfpb_daily`. Schedule `0 9 * * *` (UTC), `catchup=False`, `max_active_runs=1`, 2 retries with a 10-minute delay.

```
extract_to_s3 ─► load_to_snowflake ─► dbt_source_freshness ─► dbt_build (incl. snapshot) ─► dbt_docs_generate
```

| Task | Operator |
|---|---|
| `extract_to_s3` | `PythonOperator` (calls `ingestion.cfpb_extract.run(mode="incremental", load_date=ds)`) |
| `load_to_snowflake` | `@task` calling `ingestion.snowflake_loader.load_partition` |
| `dbt_*` | Cosmos `DbtTaskGroup` or `BashOperator` |

- Runs locally with `docker-compose` (Airflow 3.3, single `airflow standalone` container, with dbt in its own virtualenv). Optionally runs on MWAA or Astronomer.
- Alerts on task failure through a Slack webhook or email.
- SLA: data current by 10:00 UTC.

---

## 9. Dashboard (Streamlit)

- Connects to `ANALYTICS.PROD` with the `REPORTER` role through `st.connection("snowflake")`, with `@st.cache_data(ttl=3600)`
- Reads only from `rpt_*` and the star schema, never from raw

| Section | Visual |
|---|---|
| KPI row | Total complaints (period), MoM change, timely response %, relief % |
| Trends | Monthly complaint volume line chart, filterable by bank and product |
| Bank leaderboard | Table: complaints, timely %, relief %, median observed days to close |
| Product × bank | Heatmap of complaint counts |
| Issues | Top 10 issues for the selected bank (bar chart) |
| Geography | US choropleth by state (complaints per 100k residents, with a population seed) |

**Filters:** date range, bank (multi-select, default top 10), product, banks-only toggle
**Deploy:** Streamlit Community Cloud, with Snowflake credentials in `secrets.toml`

---

## 10. Non-functional requirements

| Area | Target |
|---|---|
| Daily runtime | Under 15 minutes end to end |
| Snowflake cost | Under $5/month (XS warehouse, auto-suspend, incremental models) |
| Idempotency | Any DAG run can be re-run safely |
| Security | No credentials in the repo; key-pair auth; least-privilege roles and IAM |
| Reproducibility | `make setup`, `make backfill`, `make run` documented in the README |

---

## 11. Milestones

| # | Deliverable | Done when |
|---|---|---|
| M1 | AWS and Snowflake setup | Bucket exists; loader can PUT and COPY; `setup.sql` is committed |
| M2 | Backfill | Full history in `RAW.CFPB.COMPLAINTS`; row count noted |
| M3 | Incremental extractor | Watermark plus lookback works and pytest passes |
| M4 | dbt staging and intermediate layers | `dbt build` is green |
| M5 | Star schema and snapshot | Marts built; 40+ tests passing |
| M6 | CI and docs | PR checks run; docs live on GitHub Pages |
| M7 | Airflow DAG | 3 consecutive successful daily runs |
| M8 | Dashboard | Deployed publicly; README has screenshots and an architecture diagram |

---

## 12. Stretch goals

- Snowpipe auto-ingest instead of a scheduled COPY
- Elementary or re_data for test-result monitoring
- NLP topic tags on `complaint_what_happened` narratives (Snowflake Cortex)
- Terraform for S3, IAM, and the Snowflake objects
