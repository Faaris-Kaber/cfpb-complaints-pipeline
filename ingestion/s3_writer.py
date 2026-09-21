"""Write records to S3 as gzip-compressed newline-delimited JSON."""

from __future__ import annotations

import gzip
import io
import json
import logging
from collections.abc import Iterable

log = logging.getLogger(__name__)


def clear_prefix(s3, bucket: str, prefix: str) -> int:
    """Delete every object under `prefix`, so a re-run replaces the partition."""
    deleted = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        keys = [{"Key": o["Key"]} for o in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=bucket, Delete={"Objects": keys})
            deleted += len(keys)
    return deleted


def write_ndjson_gz(
    s3,
    records: Iterable[dict],
    bucket: str,
    prefix: str,
    rows_per_file: int = 100_000,
) -> tuple[list[str], int]:
    """Write records under `prefix` as part-00000.json.gz, part-00001.json.gz, ...

    Returns (keys written, total rows).
    """
    prefix = prefix.rstrip("/")
    keys: list[str] = []
    total = 0
    buf: list[dict] = []

    def flush() -> None:
        key = f"{prefix}/part-{len(keys):05d}.json.gz"
        raw = io.BytesIO()
        with gzip.GzipFile(fileobj=raw, mode="wb") as gz:
            for rec in buf:
                gz.write(json.dumps(rec, separators=(",", ":")).encode())
                gz.write(b"\n")
        s3.put_object(Bucket=bucket, Key=key, Body=raw.getvalue())
        keys.append(key)
        log.info("wrote s3://%s/%s rows=%d", bucket, key, len(buf))
        buf.clear()

    for rec in records:
        buf.append(rec)
        total += 1
        if len(buf) >= rows_per_file:
            flush()
    if buf:
        flush()
    return keys, total
