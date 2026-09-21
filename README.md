# Bank Complaints Analytics Pipeline

End-to-end ELT pipeline over the [CFPB Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/)
(18M+ complaints): Python extracts to **S3**, **Snowflake** loads it raw, **dbt**
models it into a tested star schema, **Airflow** runs it daily, and a **Streamlit**
dashboard compares banks on complaint volume, timeliness and resolution.

```
CFPB API / bulk CSV ──► Python extractor ──► S3 (gzip NDJSON, partitioned by load_date)
                                                   │ COPY INTO
                                                   ▼
                                     Snowflake RAW.CFPB.COMPLAINTS (VARIANT)
                                                   │ dbt
              staging (incremental) ─► snapshot ─► intermediate ─► star schema ─► reporting
                                                   │
                                              Streamlit app
      Airflow: daily DAG          GitHub Actions: lint, tests, dbt parse/build, docs → Pages
```

See [SPEC.md](SPEC.md) for the full design.

## Repo layout

| Path | What |
|---|---|
| `ingestion/` | API client (search_after pagination, retries), bulk CSV backfill, S3 writer, watermark, CLI; `tests/` with mocked API + S3 |
| `snowflake/` | One-time setup (warehouse, roles, storage integration, stage, raw table) and the COPY template |
| `dbt/` | Models (staging → intermediate → marts), snapshot, seeds, macros, 66 tests |
| `airflow/` | `cfpb_daily` DAG and Docker image (Airflow 3 + isolated dbt venv) |
| `dashboard/` | Streamlit app reading the marts |
| `.github/workflows/` | CI and docs deployment |

## Data model

**Fact:** `fct_complaints` (one row per complaint, incremental merge).
**Dimensions:** `dim_date`, `dim_company` (legal entities rolled up to parent),
`dim_product` (with a `product_group` stable across CFPB renames), `dim_issue`,
`dim_geography` (state + ZIP3, with population), `dim_submission_channel`.
**Reporting:** `rpt_company_monthly`, `rpt_product_trends`, `rpt_state_monthly`.

**About "response time":** the public data has no close date, only
`date_received`, `date_sent_to_company` and a timely-response flag. The pipeline
snapshots each complaint's status daily (`snap_complaints_status`) and records
when it leaves "In progress". That gives an observed days-to-close measure,
which exists only from the day the pipeline starts running. The timely-response
rate covers all history.

## Getting started

### 1. Local setup (no cloud accounts needed)

```bash
py -3.13 -m venv .venv
.venv/Scripts/activate            # macOS/Linux: source .venv/bin/activate
pip install -e ".[dev,dbt,dashboard]"
pytest -q                         # ingestion tests (API and S3 are mocked)
sqlfluff lint dbt/models dbt/tests
cd dbt && DBT_PROFILES_DIR=. SNOWFLAKE_ACCOUNT=x SNOWFLAKE_USER=x dbt deps && dbt parse
```

### 2. Cloud setup

1. **AWS:** create the bucket, an IAM user or role for the extractor (write to `raw/` and `state/`), and an IAM role Snowflake can assume (read `raw/cfpb/`).
2. **Snowflake:** generate a key pair and run `snowflake/setup.sql`, then copy the integration's IAM user ARN and external ID into the AWS role's trust policy.
3. Copy `.env.example` to `.env` and fill it in.

### 3. First load

```bash
python -m ingestion.cfpb_extract --mode backfill        # ~350 MB CSV -> S3
# In Snowflake, run snowflake/copy_into_raw.sql with path = 'backfill/' and force = FALSE
python -m ingestion.cfpb_extract --mode incremental     # last 30 days from the API
cd dbt && dbt deps && dbt seed && dbt build
```

### 4. Run daily

```bash
docker compose up --build    # Airflow UI at http://localhost:8080
streamlit run dashboard/app.py
```

## Extraction notes

These were verified against the live API in Oct 2026:

- `frm` (offset) is ignored, so pages use `search_after=<sort0>_<sort1>` from the previous page's last hit.
- `size` is capped at 10,000 (anything larger returns HTTP 424). `format=json` returns 404.
- `date_received_min` and `date_received_max` are inclusive. The extractor walks one day at a time.
- The bulk export is only published as `complaints.csv.zip`. Its headers are mapped to API field names, so both sources land in the same shape.
- Each daily run re-pulls the last 30 days, so status changes such as "In progress" → "Closed" get captured. Staging dedupes on `complaint_id`.
