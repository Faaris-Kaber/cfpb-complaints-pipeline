-- =============================================================================
-- CFPB pipeline: one-time Snowflake setup
-- Run as ACCOUNTADMIN (or SYSADMIN + SECURITYADMIN). Replace the <PASTE ...> keys.
-- =============================================================================

-- ---------- Warehouse ----------
USE ROLE SYSADMIN;
CREATE WAREHOUSE IF NOT EXISTS TRANSFORM_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

-- ---------- Databases & schemas ----------
CREATE DATABASE IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS RAW.CFPB;
CREATE DATABASE IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS ANALYTICS.PROD;

-- ---------- Roles ----------
USE ROLE SECURITYADMIN;
CREATE ROLE IF NOT EXISTS LOADER;       -- COPY INTO raw tables
CREATE ROLE IF NOT EXISTS TRANSFORMER;  -- dbt
CREATE ROLE IF NOT EXISTS REPORTER;     -- Streamlit, read-only on marts
GRANT ROLE LOADER TO ROLE SYSADMIN;
GRANT ROLE TRANSFORMER TO ROLE SYSADMIN;
GRANT ROLE REPORTER TO ROLE SYSADMIN;

GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE LOADER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE TRANSFORMER;
GRANT USAGE ON WAREHOUSE TRANSFORM_WH TO ROLE REPORTER;

-- LOADER: write raw
GRANT USAGE ON DATABASE RAW TO ROLE LOADER;
GRANT ALL ON SCHEMA RAW.CFPB TO ROLE LOADER;
GRANT ALL ON FUTURE TABLES IN SCHEMA RAW.CFPB TO ROLE LOADER;

-- TRANSFORMER: read raw, own ANALYTICS (creates dev/PR schemas)
GRANT USAGE ON DATABASE RAW TO ROLE TRANSFORMER;
GRANT USAGE ON SCHEMA RAW.CFPB TO ROLE TRANSFORMER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RAW.CFPB TO ROLE TRANSFORMER;
GRANT ALL ON DATABASE ANALYTICS TO ROLE TRANSFORMER;
GRANT ALL ON SCHEMA ANALYTICS.PROD TO ROLE TRANSFORMER;

-- REPORTER: read prod marts
GRANT USAGE ON DATABASE ANALYTICS TO ROLE REPORTER;
GRANT USAGE ON SCHEMA ANALYTICS.PROD TO ROLE REPORTER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ANALYTICS.PROD TO ROLE REPORTER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA ANALYTICS.PROD TO ROLE REPORTER;

-- ---------- Service users (key-pair auth; no passwords) ----------
-- Generate keys locally:
--   (save them as secrets/cfpb_svc.p8/.pub and secrets/cfpb_dashboard.p8/.pub; secrets/ is git-ignored)
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
CREATE USER IF NOT EXISTS CFPB_SVC
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = TRANSFORM_WH
  RSA_PUBLIC_KEY = '<PASTE rsa_key.pub BODY WITHOUT HEADER/FOOTER>';
GRANT ROLE LOADER TO USER CFPB_SVC;
GRANT ROLE TRANSFORMER TO USER CFPB_SVC;

CREATE USER IF NOT EXISTS CFPB_DASHBOARD
  DEFAULT_ROLE = REPORTER
  DEFAULT_WAREHOUSE = TRANSFORM_WH
  RSA_PUBLIC_KEY = '<PASTE dashboard public key>';
GRANT ROLE REPORTER TO USER CFPB_DASHBOARD;

-- ---------- Why there is no S3 storage integration ----------
-- The AWS account is a "new AWS experience" project. Its AWS-managed resource
-- control policy denies s3:* and sts:* to principals outside the AWS
-- organization, so Snowflake cannot assume an IAM role to read the bucket.
-- Instead ingestion/snowflake_loader.py (running with our own AWS credentials)
-- downloads each S3 partition, PUTs it to the internal stage below, and COPYs it.

-- ---------- File format, stage, raw table ----------
USE ROLE LOADER;
USE WAREHOUSE TRANSFORM_WH;
USE SCHEMA RAW.CFPB;

CREATE FILE FORMAT IF NOT EXISTS JSON_GZ
  TYPE = JSON
  COMPRESSION = GZIP
  STRIP_OUTER_ARRAY = FALSE;

-- Internal stage: the loader PUTs files here and COPY ... PURGE = TRUE removes them.
CREATE STAGE IF NOT EXISTS CFPB_STAGE
  FILE_FORMAT = JSON_GZ;

CREATE TABLE IF NOT EXISTS COMPLAINTS (
  record      VARIANT,
  _file       STRING,
  _file_row   NUMBER,
  _loaded_at  TIMESTAMP_NTZ
);
