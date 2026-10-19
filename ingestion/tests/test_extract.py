import gzip
import io
import json
import zipfile
from datetime import date

import pytest
import responses

from ingestion.backfill import iter_csv_records
from ingestion.cfpb_extract import incremental_window, run
from ingestion.config import API_URL
from ingestion.s3_writer import write_ndjson_gz
from ingestion.tests.conftest import BUCKET, make_hit, make_page
from ingestion.watermark import read_watermark, write_watermark


def _read_all(s3, prefix: str) -> list[dict]:
    rows = []
    for obj in s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix).get("Contents", []):
        body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
        rows += [json.loads(line) for line in gzip.decompress(body).splitlines()]
    return rows


def test_incremental_window_uses_watermark_minus_lookback():
    assert incremental_window(date(2026, 10, 10), date(2026, 10, 12), 30) == (
        date(2026, 9, 10),
        date(2026, 10, 12),
    )
    # Re-running a load_date older than the lookback still gives a valid window.
    assert incremental_window(date(2026, 10, 10), date(2026, 8, 1), 30) == (
        date(2026, 7, 2),
        date(2026, 8, 1),
    )
    # No watermark yet: fall back to load_date - lookback.
    assert incremental_window(None, date(2026, 10, 12), 2) == (
        date(2026, 10, 10),
        date(2026, 10, 12),
    )


def test_writer_splits_files(s3):
    keys, total = write_ndjson_gz(s3, ({"i": i} for i in range(7)), BUCKET, "p", rows_per_file=3)
    assert total == 7
    assert keys == ["p/part-00000.json.gz", "p/part-00001.json.gz", "p/part-00002.json.gz"]
    assert [r["i"] for r in _read_all(s3, "p/")] == list(range(7))


def test_watermark_round_trip(s3):
    assert read_watermark(s3, BUCKET, "state/wm.json") is None
    write_watermark(s3, BUCKET, "state/wm.json", date(2026, 10, 11), rows=5)
    assert read_watermark(s3, BUCKET, "state/wm.json") == date(2026, 10, 11)


@responses.activate
def test_incremental_run_writes_partition_and_advances_watermark(s3, settings):
    write_watermark(s3, BUCKET, settings.state_key, date(2026, 10, 10), rows=0)

    def page_for(request):
        day = request.params["date_received_min"]
        hits = [make_hit(int(day.replace("-", "")), day)]
        return 200, {}, json.dumps(make_page(hits, total=1))

    responses.add_callback(responses.GET, API_URL, callback=page_for)

    summary = run(mode="incremental", load_date=date(2026, 10, 12), settings=settings, s3=s3)

    assert summary["window"] == ("2026-10-08", "2026-10-12")
    assert summary["rows"] == 5
    rows = _read_all(s3, "raw/cfpb/incremental/load_date=2026-10-12/")
    assert len(rows) == 5
    assert all(r["_source"] == "api" and r["_extracted_at"] for r in rows)
    assert read_watermark(s3, BUCKET, settings.state_key) == date(2026, 10, 12)


@responses.activate
def test_rerun_replaces_partition(s3, settings):
    # settings.lookback_days=2 and no watermark: the window is 3 days, one hit per day.
    responses.get(API_URL, json=make_page([make_hit(1, "2026-10-12")], total=1))
    part = "raw/cfpb/incremental/load_date=2026-10-12/"
    s3.put_object(Bucket=BUCKET, Key=part + "part-00009.json.gz", Body=b"stale")
    for _ in range(2):
        summary = run(mode="incremental", load_date=date(2026, 10, 12), settings=settings, s3=s3)
    assert summary["partition"] == "incremental/load_date=2026-10-12/"
    keys = [o["Key"] for o in s3.list_objects_v2(Bucket=BUCKET, Prefix=part)["Contents"]]
    assert part + "part-00009.json.gz" not in keys  # stale file from an older run removed
    assert len(_read_all(s3, part)) == 3
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/cfpb/_staging/")["KeyCount"] == 0


@responses.activate
def test_failed_fetch_keeps_existing_partition(s3, settings):
    part = "raw/cfpb/incremental/load_date=2026-10-12/"
    s3.put_object(Bucket=BUCKET, Key=part + "part-00000.json.gz", Body=b"previous run")
    responses.get(API_URL, status=400)  # not retryable: fails on the first day
    with pytest.raises(Exception):  # noqa: B017
        run(mode="incremental", load_date=date(2026, 10, 12), settings=settings, s3=s3)
    obj = s3.get_object(Bucket=BUCKET, Key=part + "part-00000.json.gz")["Body"].read()
    assert obj == b"previous run"
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix="raw/cfpb/_staging/")["KeyCount"] == 0


@responses.activate
def test_custom_window_gets_its_own_partition(s3, settings):
    responses.get(API_URL, json=make_page([make_hit(1, "2026-09-01")], total=1))
    daily = "raw/cfpb/incremental/load_date=2026-10-12/"
    s3.put_object(Bucket=BUCKET, Key=daily + "part-00000.json.gz", Body=b"scheduled")
    summary = run(
        mode="incremental",
        load_date=date(2026, 10, 12),
        start=date(2026, 9, 1),
        end=date(2026, 9, 1),
        settings=settings,
        s3=s3,
    )
    assert summary["partition"] == "incremental/window=2026-09-01_2026-09-01/"
    assert s3.get_object(Bucket=BUCKET, Key=daily + "part-00000.json.gz")["Body"].read() == (
        b"scheduled"
    )


def test_end_without_start_is_rejected(s3, settings):
    with pytest.raises(ValueError, match="both"):
        run(mode="incremental", end=date(2026, 10, 1), settings=settings, s3=s3)


@responses.activate
def test_lookback_override_widens_window(s3, settings):
    write_watermark(s3, BUCKET, settings.state_key, date(2026, 10, 12), rows=0)
    responses.get(API_URL, json=make_page([], total=0))
    summary = run(
        mode="incremental", load_date=date(2026, 10, 12), lookback_days=90, settings=settings, s3=s3
    )
    assert summary["window"] == ("2026-07-14", "2026-10-12")


def test_csv_headers_map_to_api_fields(tmp_path):
    csv_text = (
        "Date received,Product,Sub-product,Issue,Sub-issue,Company public response,Company,"
        "State,ZIP code,Tags,Submitted via,Date sent to company,Company response to consumer,"
        "Timely response?,Complaint ID\n"
        '2024-05-17,Mortgage,,Trouble,,,"BANK, N.A.",LA,70129,,Web,2024-05-18,'
        "Closed with explanation,Yes,9022199\n"
    )
    zp = tmp_path / "complaints.csv.zip"
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("complaints.csv", csv_text)

    (rec,) = list(iter_csv_records(zp))
    assert rec["complaint_id"] == "9022199"
    assert rec["company"] == "BANK, N.A."
    assert rec["timely"] == "Yes"
    assert rec["sub_product"] is None
    assert rec["company_response"] == "Closed with explanation"


def test_backfill_run_uses_local_zip(s3, settings, tmp_path):
    zp = tmp_path / "c.zip"
    buf = io.StringIO("Complaint ID,Date received\n1,2024-01-01\n2,2024-01-02\n")
    with zipfile.ZipFile(zp, "w") as zf:
        zf.writestr("complaints.csv", buf.getvalue())

    summary = run(mode="backfill", zip_path=zp, settings=settings, s3=s3)
    assert summary["rows"] == 2
    rows = _read_all(s3, "raw/cfpb/backfill/")
    assert {r["_source"] for r in rows} == {"bulk_csv"}
    assert read_watermark(s3, BUCKET, settings.state_key) == date(2024, 1, 2)


def test_invalid_window_does_not_delete_existing_partition(s3, settings):
    prefix = "raw/cfpb/incremental/load_date=2026-10-12/"
    s3.put_object(Bucket=BUCKET, Key=prefix + "part-00000.json.gz", Body=b"keep")
    with pytest.raises(ValueError):
        run(
            mode="incremental",
            load_date=date(2026, 10, 12),
            start=date(2026, 10, 12),
            end=date(2026, 10, 1),
            settings=settings,
            s3=s3,
        )
    assert s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)["KeyCount"] == 1
