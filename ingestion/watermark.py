"""Incremental-load state, stored as a small JSON object in S3."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

from botocore.exceptions import ClientError


def read_watermark(s3, bucket: str, key: str) -> date | None:
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    state = json.loads(obj["Body"].read())
    return date.fromisoformat(state["max_date_received"])


def write_watermark(s3, bucket: str, key: str, max_date_received: date, rows: int) -> None:
    state = {
        "max_date_received": max_date_received.isoformat(),
        "rows_last_run": rows,
        "updated_at": datetime.now(UTC).isoformat(),
    }
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(state).encode(),
        ContentType="application/json",
    )
