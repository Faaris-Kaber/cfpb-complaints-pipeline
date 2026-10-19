"""Entry point: extract CFPB complaints to S3.

Usage:
    python -m ingestion.cfpb_extract --mode incremental [--load-date YYYY-MM-DD]
    python -m ingestion.cfpb_extract --mode incremental --lookback-days 90
    python -m ingestion.cfpb_extract --mode incremental --start 2026-09-01 --end 2026-09-30
    python -m ingestion.cfpb_extract --mode backfill [--zip-path complaints.csv.zip]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
import uuid
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3

from ingestion.backfill import bulk_records
from ingestion.cfpb_client import CFPBClient
from ingestion.config import Settings
from ingestion.s3_writer import delete_keys, list_keys, publish_partition, write_ndjson_gz
from ingestion.watermark import read_watermark, write_watermark

log = logging.getLogger("cfpb_extract")


class _MaxDateTracker:
    """Pass records through while remembering the latest date_received seen."""

    def __init__(self) -> None:
        self.max_date: date | None = None

    def __call__(self, records: Iterable[dict]) -> Iterator[dict]:
        for rec in records:
            raw = rec.get("date_received")
            if raw:
                d = date.fromisoformat(raw[:10])
                if self.max_date is None or d > self.max_date:
                    self.max_date = d
            yield rec


def _tag(records: Iterable[dict], source: str) -> Iterator[dict]:
    extracted_at = datetime.now(UTC).isoformat()
    for rec in records:
        yield {**rec, "_extracted_at": extracted_at, "_source": source}


def incremental_window(
    watermark: date | None, load_date: date, lookback_days: int
) -> tuple[date, date]:
    """Re-pull `lookback_days` before the watermark so status changes are captured.

    Anchored on the earlier of the watermark and load_date, so re-running an old
    load_date (older than the lookback) still yields a valid window ending on it.
    """
    anchor = min(watermark, load_date) if watermark else load_date
    start = anchor - timedelta(days=lookback_days)
    return start, load_date


def run(
    mode: str = "incremental",
    load_date: date | None = None,
    start: date | None = None,
    end: date | None = None,
    zip_path: Path | None = None,
    lookback_days: int | None = None,
    settings: Settings | None = None,
    s3=None,
    client: CFPBClient | None = None,
) -> dict:
    """Extract to S3 and return a summary whose `partition` is the path the loader takes.

    Incremental runs write `incremental/load_date=<load_date>/`. A custom --start/--end
    window writes `incremental/window=<start>_<end>/` so it never replaces a scheduled
    partition. Files are written to a staging prefix first and only replace the
    partition once the whole extract has succeeded.
    """
    settings = settings or Settings.from_env()
    s3 = s3 or boto3.client("s3", region_name=settings.aws_region)
    load_date = load_date or datetime.now(UTC).date()
    t0 = time.monotonic()
    tracker = _MaxDateTracker()

    if mode == "backfill":
        partition = "backfill"
        records = _tag(bulk_records(zip_path), "bulk_csv")
        window = None
    elif mode == "incremental":
        client = client or CFPBClient(page_size=settings.page_size)
        if (start is None) != (end is None):
            raise ValueError("pass both --start and --end, or neither")
        if start is None:
            wm = read_watermark(s3, settings.s3_bucket, settings.state_key)
            start, end = incremental_window(wm, load_date, lookback_days or settings.lookback_days)
            partition = f"incremental/load_date={load_date.isoformat()}"
        else:
            partition = f"incremental/window={start.isoformat()}_{end.isoformat()}"
        if end < start:
            raise ValueError(f"window end {end} is before start {start}")
        records = _tag(client.fetch_range(start, end), "api")
        window = (start.isoformat(), end.isoformat())
    else:
        raise ValueError(f"unknown mode {mode!r}")

    prefix = f"{settings.raw_prefix}/{partition}"
    staging = f"{settings.raw_prefix}/_staging/{uuid.uuid4().hex}"
    try:
        staged, rows = write_ndjson_gz(
            s3, tracker(records), settings.s3_bucket, staging, settings.rows_per_file
        )
    except BaseException:
        # Leave the existing partition alone; just drop the half-written staging files.
        delete_keys(s3, settings.s3_bucket, list_keys(s3, settings.s3_bucket, staging + "/"))
        raise
    keys = publish_partition(s3, settings.s3_bucket, staged, prefix)

    # Advance the watermark only after the partition is in place.
    if tracker.max_date is not None:
        prev = read_watermark(s3, settings.s3_bucket, settings.state_key)
        new_wm = max(d for d in (prev, tracker.max_date) if d is not None)
        write_watermark(s3, settings.s3_bucket, settings.state_key, new_wm, rows)

    summary = {
        "mode": mode,
        "load_date": load_date.isoformat(),
        "partition": partition + "/",
        "window": window,
        "prefix": f"s3://{settings.s3_bucket}/{prefix}/",
        "files": len(keys),
        "rows": rows,
        "max_date_received": tracker.max_date.isoformat() if tracker.max_date else None,
        "elapsed_s": round(time.monotonic() - t0, 1),
    }
    log.info(json.dumps(summary))
    return summary


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("--mode", choices=["incremental", "backfill"], default="incremental")
    p.add_argument("--load-date", type=date.fromisoformat)
    p.add_argument("--start", type=date.fromisoformat, help="override window start")
    p.add_argument("--end", type=date.fromisoformat, help="override window end")
    p.add_argument("--zip-path", type=Path, help="local bulk zip (skip download)")
    p.add_argument(
        "--lookback-days",
        type=int,
        help="override the re-pull window (default LOOKBACK_DAYS; the weekly deep run uses 90)",
    )
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(
        mode=args.mode,
        load_date=args.load_date,
        start=args.start,
        end=args.end,
        zip_path=args.zip_path,
        lookback_days=args.lookback_days,
    )


if __name__ == "__main__":
    main()
