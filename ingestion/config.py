"""Runtime settings, read from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/"
BULK_CSV_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"

# The API rejects page sizes above 10,000 with HTTP 424.
MAX_PAGE_SIZE = 10_000


@dataclass(frozen=True)
class Settings:
    s3_bucket: str
    aws_region: str = "us-east-1"
    raw_prefix: str = "raw/cfpb"
    state_key: str = "state/cfpb_watermark.json"
    lookback_days: int = 30
    page_size: int = MAX_PAGE_SIZE
    rows_per_file: int = 100_000

    @classmethod
    def from_env(cls) -> Settings:
        bucket = os.environ.get("S3_BUCKET")
        if not bucket:
            raise RuntimeError("S3_BUCKET is not set (see .env.example)")
        page_size = int(os.environ.get("PAGE_SIZE", MAX_PAGE_SIZE))
        return cls(
            s3_bucket=bucket,
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
            raw_prefix=os.environ.get("S3_RAW_PREFIX", "raw/cfpb").strip("/"),
            state_key=os.environ.get("S3_STATE_KEY", "state/cfpb_watermark.json"),
            lookback_days=int(os.environ.get("LOOKBACK_DAYS", 30)),
            page_size=min(page_size, MAX_PAGE_SIZE),
            rows_per_file=int(os.environ.get("ROWS_PER_FILE", 100_000)),
        )
