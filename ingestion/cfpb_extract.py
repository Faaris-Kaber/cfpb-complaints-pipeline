"""Entry point: extract CFPB complaints to S3.

Usage:
    python -m ingestion.cfpb_extract --mode incremental [--load-date YYYY-MM-DD]
    python -m ingestion.cfpb_extract --mode incremental --start 2026-09-01 --end 2026-09-30
    python -m ingestion.cfpb_extract --mode backfill [--zip-path complaints.csv.zip]
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import boto3

from ingestion.backfill import bulk_records
from ingestion.cfpb_client import CFPBClient
from ingestion.config import Settings
from ingestion.s3_writer import clear_prefix, write_ndjson_gz
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
    """Re-pull `lookback_days` before the watermark so status changes are captured."""
    anchor = watermark or load_date
    start = anchor - timedelta(days=lookback_days)
    return start, load_date


def run(
    mode: str = "incremental",
    load_date: date | None = None,
    start: date | None = None,
    end: date | None = None,
    zip_path: Path | None = None,
    settings: Settings | None = None,
    s3=None,
    client: CFPBClient | None = None,
) -> dict:
    settings = settings or Settings.from_env()
    s3 = s3 or boto3.client("s3", region_name=settings.aws_region)
    load_date = load_date or datetime.now(UTC).date()
    t0 = time.monotonic()
    tracker = _MaxDateTracker()

    if mode == "backfill":
        prefix = f"{settings.raw_prefix}/backfill"
        records = _tag(bulk_records(zip_path), "bulk_csv")
        window = None
    elif mode == "incremental":
        client = client or CFPBClient(page_size=settings.page_size)
        if start is None:
            wm = read_watermark(s3, settings.s3_bucket, settings.state_key)
            start, end = incremental_window(wm, load_date, settings.lookback_days)
        end = end or load_date
        prefix = f"{settings.raw_prefix}/incremental/load_date={load_date.isoformat()}"
        records = _tag(client.fetch_range(start, end), "api")
        window = (start.isoformat(), end.isoformat())
    else:
        raise ValueError(f"unknown mode {mode!r}")

    cleared = clear_prefix(s3, settings.s3_bucket, prefix + "/")
    if cleared:
        log.info("cleared %d existing objects under %s/ (re-run)", cleared, prefix)

    keys, rows = write_ndjson_gz(
        s3, tracker(records), settings.s3_bucket, prefix, settings.rows_per_file
    )

    # Advance the watermark only after every file is written.
    if tracker.max_date is not None:
        prev = read_watermark(s3, settings.s3_bucket, settings.state_key)
        new_wm = max(d for d in (prev, tracker.max_date) if d is not None)
        write_watermark(s3, settings.s3_bucket, settings.state_key, new_wm, rows)

    summary = {
        "mode": mode,
        "load_date": load_date.isoformat(),
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
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    run(
        mode=args.mode,
        load_date=args.load_date,
        start=args.start,
        end=args.end,
        zip_path=args.zip_path,
    )


if __name__ == "__main__":
    main()
