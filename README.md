# Bank Complaints Analytics Pipeline

End-to-end ELT pipeline over the [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)
(**18.5M complaints**). Python extracts to **S3**, **Snowflake** loads it raw, **dbt**
models it into a tested star schema, **Airflow** runs it daily, and a **Streamlit**
dashboard compares banks on complaint volume, timeliness and resolution.

![Dashboard overview: KPI cards, monthly complaints by bank, company scorecard](docs/images/dashboard_overview.png)

```
CFPB API / bulk CSV ──► Python extractor ──► S3 (gzip NDJSON, partitioned by load_date)
                                                   │ Python loader: PUT to internal stage, COPY INTO
                                                   ▼
                                     Snowflake RAW.CFPB.COMPLAINTS (VARIANT)
                                                   │ dbt
              staging (incremental) ─► snapshot ─► intermediate ─► star schema ─► reporting
                                                   │
                                              Streamlit app
      Airflow: daily DAG          GitHub Actions: lint, tests, dbt parse/build, docs → Pages
```

- **dbt docs and lineage:** https://faaris-kaber.github.io/cfpb-complaints-pipeline/
- **Full design:** [SPEC.md](SPEC.md)

## Results

| | |
|---|---|
| Complaints loaded | 18,514,327 as of Oct 19, 2026 (Dec 2011 onward), plus ~600k re-pulled daily (~1.8M on Sundays) to catch status changes |
| Daily pipeline | ~3 minutes end to end: API → S3 → Snowflake → `dbt build` |
| dbt | 14 models, 1 snapshot, 2 seeds, **67 data tests** + source freshness; prod build ~30s on an XS warehouse |
| Python | 24 pytest tests (API, S3 and Snowflake mocked); ruff + sqlfluff in CI |

A few things the data shows (2025, banks only):
- Capital One received the most complaints (34k), ahead of JPMorgan Chase (24k) and Citibank (20k).
- Citibank closed **52%** of complaints with relief, against 14–29% for its peers.
- In January 2025, complaints against Capital One jumped 4× (1,887 → 7,601) and against Navy Federal almost 10× (873 → 8,302), then fell back the next month.

## Dashboard

The Streamlit app reads the marts in `ANALYTICS.PROD` with a read-only role. Every
chart has a **Data** menu that shows the numbers behind it.

- **Filters:** period (12M, 24M, YTD, All, or a custom month range), banks or all
  companies, and up to 8 companies. A company keeps its color while it stays selected.
- **KPI cards:** complaints, the latest complete month against the one before,
  timely-response rate and relief rate, each with a 12-month sparkline.
- **Overview:** monthly complaints per company (complete months only) and a scorecard
  with timely, relief and in-progress rates.
- **Products & issues:** product mix per company (share or count), the top 10 issues,
  and year-over-year change by product for the latest complete month.
- **Geography:** complaints per 100,000 residents by state (2020 census).
- Light and dark themes, set in [`.streamlit/config.toml`](.streamlit/config.toml).

| Products & issues | Geography |
|---|---|
| ![Product mix heatmap, top issues and year-over-year change](docs/images/dashboard_products.png) | ![Complaints per 100k residents by state](docs/images/dashboard_geography.png) |
| **Company picker** | **Dark theme** |
| ![Company picker with top-5/top-8 shortcuts](docs/images/dashboard_picker.png) | ![Overview in the dark theme](docs/images/dashboard_dark.png) |

## dbt lineage

![dbt lineage](docs/images/dbt_lineage.png)

**Fact:** `fct_complaints` (one row per complaint, incremental merge).
**Dimensions:** `dim_date`, `dim_company` (one row per legal entity, with its parent brand),
`dim_product` (with a `product_group` stable across CFPB renames), `dim_issue`,
`dim_geography` (state + ZIP3, with population), `dim_submission_channel`.
**Reporting:** `rpt_company_monthly` and `rpt_state_monthly` feed the dashboard.
`rpt_product_trends` (market-wide product trends with month-over-month and year-over-year
change) is for analysis only; the dashboard's year-over-year chart follows the company
filter, so it reads `fct_complaints` directly.

**About "response time":** the public data has no close date, only
`date_received`, `date_sent_to_company` and a timely-response flag. The pipeline
snapshots each complaint's status daily (`snap_complaints_status`) and records
when it leaves "In progress". That gives an observed days-to-close measure, which
exists only from the day the pipeline started. The timely-response rate covers
all history.

## Repo layout

| Path | What |
|---|---|
| `ingestion/` | API client (search_after paging, retries), bulk CSV backfill, S3 writer, watermark, S3 → Snowflake loader; `tests/` |
| `aws/` | Bucket setup and the scoped IAM user for scheduled runs |
| `snowflake/` | One-time setup: warehouse, databases, roles, service users, internal stage, raw table |
| `dbt/` | Staging → intermediate → marts, snapshot, seeds, macros, singular tests |
| `airflow/` | `cfpb_daily` DAG and Docker image (Airflow 3 + isolated dbt virtualenv) |
| `dashboard/` | Streamlit app reading the marts |
| `.streamlit/` | Dashboard theme (light and dark) |
| `.github/workflows/` | CI (lint, tests, dbt parse, PR dbt build) and docs deployment |

## Getting started

### 1. Local setup (no cloud accounts needed)

```bash
py -3.13 -m venv .venv
source .venv/Scripts/activate     # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev,dbt,dashboard]"
pytest -q                         # API, S3 and Snowflake are mocked
sqlfluff lint dbt/models dbt/tests
cd dbt && DBT_PROFILES_DIR=. SNOWFLAKE_ACCOUNT=x SNOWFLAKE_USER=x dbt deps && dbt parse
```

### 2. Cloud setup

1. **AWS:** sign in with `aws login`, then run `BUCKET=<name> REGION=<region> ./aws/setup_aws.sh bucket`.
2. **Snowflake:** generate key pairs into `secrets/` (`cfpb_svc.p8` and `cfpb_dashboard.p8`; the folder is git-ignored), paste the public keys into `snowflake/setup.sql`, and run it as ACCOUNTADMIN.
3. Copy `.env.example` to `.env` and fill it in. Copy `dashboard/.streamlit/secrets.toml.example` to `secrets.toml` for the dashboard.

### 3. First load

With `.env` loaded into your shell:

```bash
python -m ingestion.cfpb_extract --mode backfill                  # ~350 MB CSV -> S3
python -m ingestion.snowflake_loader --path backfill/ --no-force  # S3 -> Snowflake RAW
python -m ingestion.cfpb_extract --mode incremental --lookback-days 90  # recent statuses from the API
python -m ingestion.snowflake_loader --path <partition printed by the extract>
#   e.g. incremental/load_date=2026-10-19/  (partitions are named by the UTC date)
cd dbt && DBT_PROFILES_DIR=. dbt deps && DBT_PROFILES_DIR=. dbt build --target prod
```

### 4. Run daily

```bash
docker compose up --build -d     # Airflow UI at http://127.0.0.1:8080
streamlit run dashboard/app.py   # dashboard at http://localhost:8501
```

Start the dashboard from the repo root so it picks up the theme in `.streamlit/config.toml`.

The Airflow admin password is generated on first start. Read it with
`docker compose exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated`.

### Credentials for scheduled runs

By default the Airflow container uses your `aws login` session. That's fine for
manual runs, but the session ends after at most 12 hours, so the 09:00 UTC
scheduled run would fail the next day. For unattended runs:

1. `BUCKET=<name> ./aws/setup_aws.sh pipeline-user` creates the IAM user `cfpb-pipeline`. It can only list the bucket and read/write `raw/cfpb/` and `state/`.
2. In the AWS console, create an access key for `cfpb-pipeline` (IAM → Users → cfpb-pipeline → Security credentials), then run `aws configure --profile cfpb-pipeline`.
3. Set `PIPELINE_AWS_PROFILE=cfpb-pipeline` in `.env` and run `docker compose up -d`.

If credentials are missing, misconfigured (for example an unknown profile) or expired, the extract and load tasks fail straight away with instructions instead of retrying.

### 5. Deploy the dashboard (optional)

On [Streamlit Community Cloud](https://share.streamlit.io), create an app from this
repo with main file `dashboard/app.py`. In the app's **Secrets**, paste the
`[connections.snowflake]` block from `secrets.toml.example`, replacing
`private_key_file` with `private_key = "<base64 body>"`. The connector expects the key's base64 body only: all the lines of `cfpb_dashboard.p8` between `-----BEGIN PRIVATE KEY-----` and `-----END PRIVATE KEY-----`, joined into one line. The full PEM file, header lines included, fails with "Incorrect padding".

## Extraction notes

Verified against the live API in Oct 2026:

- `frm` (offset) is ignored, so pages use `search_after=<sort0>_<sort1>` from the previous page's last hit.
- `size` is capped at 10,000 (anything larger returns HTTP 424). `format=json` returns 404.
- `date_received_min` and `date_received_max` are inclusive. The extractor walks one day at a time.
- The bulk export is only published as `complaints.csv.zip`. Its headers are mapped to API field names, so both sources land in the same shape.
- Complaint statuses keep changing for about 60 days after receipt ("In progress" → "Closed"). Daily runs re-pull the last 30 days and Sunday runs re-pull 90, so no complaint's status freezes early. Staging keeps the newest version of each complaint.

## Why Snowflake doesn't read S3 directly

The usual pattern is a Snowflake storage integration that assumes an IAM role to
read the bucket. This AWS account is a "new AWS experience" project, and its
AWS-managed resource control policy denies `s3:*` and `sts:*` to any principal
outside the AWS organization. Snowflake's IAM user is outside it, so the role
can't be assumed, and the policy can't be edited.

So the load step runs on our side instead. `ingestion/snowflake_loader.py` uses
our own AWS credentials to read each S3 partition, `PUT`s the files to a Snowflake
internal stage, and runs `COPY INTO ... PURGE = TRUE`. S3 remains the raw landing
zone and system of record, and no AWS keys are stored in Snowflake.
