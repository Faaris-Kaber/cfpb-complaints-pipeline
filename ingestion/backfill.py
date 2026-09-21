"""One-time historical load from the CFPB bulk CSV export.

The bulk JSON export no longer exists; only complaints.csv.zip is published.
CSV headers are mapped to the API's field names so both sources land in the
same raw shape.
"""

from __future__ import annotations

import csv
import io
import logging
import shutil
import tempfile
import zipfile
from collections.abc import Iterator
from pathlib import Path

import requests

from ingestion.config import BULK_CSV_URL

log = logging.getLogger(__name__)

CSV_TO_API_FIELD = {
    "Date received": "date_received",
    "Product": "product",
    "Sub-product": "sub_product",
    "Issue": "issue",
    "Sub-issue": "sub_issue",
    "Consumer complaint narrative": "complaint_what_happened",
    "Company public response": "company_public_response",
    "Company": "company",
    "State": "state",
    "ZIP code": "zip_code",
    "Tags": "tags",
    "Consumer consent provided?": "consumer_consent_provided",
    "Submitted via": "submitted_via",
    "Date sent to company": "date_sent_to_company",
    "Company response to consumer": "company_response",
    "Timely response?": "timely",
    "Consumer disputed?": "consumer_disputed",
    "Complaint ID": "complaint_id",
}


def download_bulk_zip(dest: Path, url: str = BULK_CSV_URL) -> Path:
    """Stream the bulk zip (~350 MB) to disk."""
    log.info("downloading %s -> %s", url, dest)
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        with open(dest, "wb") as f:
            shutil.copyfileobj(resp.raw, f, length=1024 * 1024)
    return dest


def iter_csv_records(zip_path: Path) -> Iterator[dict]:
    """Yield one dict per complaint, with API-style keys. Empty strings -> None."""
    with zipfile.ZipFile(zip_path) as zf:
        name = next(n for n in zf.namelist() if n.endswith(".csv"))
        with zf.open(name) as raw:
            reader = csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8", newline=""))
            unknown = set(reader.fieldnames or []) - CSV_TO_API_FIELD.keys()
            if unknown:
                log.warning("unmapped CSV columns (kept as-is): %s", sorted(unknown))
            for row in reader:
                yield {CSV_TO_API_FIELD.get(k, k): (v if v != "" else None) for k, v in row.items()}


def bulk_records(zip_path: Path | None = None) -> Iterator[dict]:
    """Download (unless a local zip is given) and iterate the bulk export."""
    if zip_path is not None:
        yield from iter_csv_records(zip_path)
        return
    with tempfile.TemporaryDirectory() as tmp:
        path = download_bulk_zip(Path(tmp) / "complaints.csv.zip")
        yield from iter_csv_records(path)
