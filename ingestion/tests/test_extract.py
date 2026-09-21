import gzip
import io
import json
import zipfile
from datetime import date

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
    responses.get(API_URL, json=make_page([make_hit(1, "2026-10-12")], total=1))
    for _ in range(2):
        run(
            mode="incremental",
            load_date=date(2026, 10, 12),
            start=date(2026, 10, 12),
            end=date(2026, 10, 12),
            settings=settings,
            s3=s3,
        )
    assert len(_read_all(s3, "raw/cfpb/incremental/load_date=2026-10-12/")) == 1


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
