"""Load an S3 partition into Snowflake RAW.CFPB.COMPLAINTS via an internal stage.

Why an internal stage instead of an S3 storage integration: the AWS account is a
"new AWS experience" project, whose AWS-managed resource control policy denies
s3:* and sts:* to principals outside the AWS organization. Snowflake's IAM user
is outside it, so Snowflake cannot read the bucket directly. This loader runs
with our own AWS credentials instead: it downloads the partition from S3, PUTs
the files to a Snowflake internal stage, and COPYs them into the raw table.

Usage:
    python -m ingestion.snowflake_loader --path incremental/load_date=2026-10-18/
    python -m ingestion.snowflake_loader --path backfill/ --no-force
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
import time
from pathlib import Path

import boto3

from ingestion.config import Settings

log = logging.getLogger(__name__)

STAGE = "RAW.CFPB.CFPB_STAGE"
TABLE = "RAW.CFPB.COMPLAINTS"
FILE_FORMAT = "RAW.CFPB.JSON_GZ"


def connect():
    """Snowflake connection as the LOADER role, using key-pair auth from env vars."""
    import snowflake.connector

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role=os.environ.get("SNOWFLAKE_LOADER_ROLE", "LOADER"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "TRANSFORM_WH"),
        database="RAW",
        schema="CFPB",
    )


def copy_sql(stage_path: str, force: bool) -> str:
    return f"""
        copy into {TABLE} (record, _file, _file_row, _loaded_at)
        from (
            select
                $1,
                metadata$filename,
                metadata$file_row_number,
                current_timestamp()::timestamp_ntz
            from @{STAGE}/{stage_path}
        )
        file_format = (format_name = '{FILE_FORMAT}')
        on_error = 'ABORT_STATEMENT'
        purge = true
        force = {"true" if force else "false"}
    """


def load_partition(
    path: str,
    force: bool = True,
    settings: Settings | None = None,
    s3=None,
    conn=None,
    batch_size: int = 20,
) -> dict:
    """Load s3://<bucket>/<raw_prefix>/<path> into the raw table.

    `path` is relative to the raw prefix, e.g. "incremental/load_date=2026-10-18/"
    or "backfill/". Files are moved in batches so a large backfill never needs
    more than `batch_size` files on local disk at once.
    """
    settings = settings or Settings.from_env()
    s3 = s3 or boto3.client("s3", region_name=settings.aws_region)
    own_conn = conn is None
    conn = conn or connect()
    path = path.strip("/") + "/"
    prefix = f"{settings.raw_prefix}/{path}"
    t0 = time.monotonic()

    keys = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=settings.s3_bucket, Prefix=prefix
    ):
        keys += [o["Key"] for o in page.get("Contents", []) if o["Key"].endswith(".json.gz")]
    keys.sort()
    if not keys:
        raise FileNotFoundError(f"no .json.gz files under s3://{settings.s3_bucket}/{prefix}")

    rows_loaded = 0
    files_loaded = 0
    cur = conn.cursor()
    try:
        for i in range(0, len(keys), batch_size):
            batch = keys[i : i + batch_size]
            with tempfile.TemporaryDirectory() as tmp:
                tmp_path = Path(tmp)
                for key in batch:
                    s3.download_file(settings.s3_bucket, key, str(tmp_path / Path(key).name))
                local_glob = tmp_path.as_posix() + "/*.json.gz"
                cur.execute(
                    f"put 'file://{local_glob}' @{STAGE}/{path} "
                    "auto_compress = false source_compression = gzip parallel = 8 overwrite = true"
                )
            cur.execute(copy_sql(path, force))
            for row in cur.fetchall():
                # COPY result columns: file, status, rows_parsed, rows_loaded, ...
                # With nothing to load it returns one row with a single message column.
                if len(row) < 4:
                    log.info("COPY: %s", row[0])
                    continue
                status, loaded = row[1], row[3]
                if status not in ("LOADED", "LOAD_SKIPPED"):
                    raise RuntimeError(f"COPY failed for {row[0]}: {row}")
                rows_loaded += loaded or 0
                files_loaded += status == "LOADED"
            log.info(
                "loaded batch %d-%d of %d files (rows so far: %d)",
                i + 1,
                i + len(batch),
                len(keys),
                rows_loaded,
            )
    finally:
        cur.close()
        if own_conn:
            conn.close()

    summary = {
        "path": path,
        "files": len(keys),
        "files_loaded": files_loaded,
        "rows_loaded": rows_loaded,
        "elapsed_s": round(time.monotonic() - t0, 1),
    }
    log.info("load summary %s", summary)
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--path", required=True, help="partition under the raw prefix, e.g. backfill/")
    p.add_argument(
        "--no-force",
        action="store_true",
        help="skip files Snowflake has already loaded (use for the one-time backfill)",
    )
    p.add_argument("--batch-size", type=int, default=20)
    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    load_partition(args.path, force=not args.no_force, batch_size=args.batch_size)


if __name__ == "__main__":
    main()
