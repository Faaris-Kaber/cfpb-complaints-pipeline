"""Write records to S3 as gzip-compressed newline-delimited JSON."""

from __future__ import annotations

import gzip
import io
import json
import logging
from collections.abc import Iterable

log = logging.getLogger(__name__)


def list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys += [o["Key"] for o in page.get("Contents", [])]
    return keys


def delete_keys(s3, bucket: str, keys: list[str]) -> int:
    """Delete keys in batches of 1000. delete_objects returns HTTP 200 even when some
    keys fail, so check its Errors list instead of assuming success."""
    deleted = 0
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i : i + 1000]]
        resp = s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        errors = resp.get("Errors")
        if errors:
            raise RuntimeError(f"S3 delete failed for {len(errors)} keys: {errors[:3]}")
        deleted += len(resp.get("Deleted", batch))
    return deleted


def publish_partition(s3, bucket: str, staged_keys: list[str], dest_prefix: str) -> list[str]:
    """Move fully written files from a staging prefix into `dest_prefix`.

    Copies each staged file over its final key first, then deletes final keys that are
    no longer part of the partition, then the staged copies. If anything fails before
    this point the existing partition is untouched.
    """
    dest_prefix = dest_prefix.rstrip("/")
    final_keys = []
    for key in staged_keys:
        final = f"{dest_prefix}/{key.rsplit('/', 1)[-1]}"
        s3.copy_object(Bucket=bucket, Key=final, CopySource={"Bucket": bucket, "Key": key})
        final_keys.append(final)
    stale = [k for k in list_keys(s3, bucket, dest_prefix + "/") if k not in set(final_keys)]
    if stale:
        delete_keys(s3, bucket, stale)
        log.info("removed %d stale objects from %s/", len(stale), dest_prefix)
    delete_keys(s3, bucket, staged_keys)
    return final_keys


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
