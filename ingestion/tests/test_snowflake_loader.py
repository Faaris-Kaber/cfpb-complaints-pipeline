import pytest

from ingestion.snowflake_loader import load_partition
from ingestion.tests.conftest import BUCKET


class FakeCursor:
    def __init__(self, copy_rows):
        self.sql: list[str] = []
        self.copy_rows = copy_rows
        self._last: list = []

    def execute(self, sql):
        self.sql.append(" ".join(sql.split()))
        self._last = self.copy_rows.pop(0) if sql.strip().lower().startswith("copy") else []
        return self

    def fetchall(self):
        return self._last

    def close(self):
        pass


class FakeConn:
    def __init__(self, copy_rows):
        self.cur = FakeCursor(copy_rows)

    def cursor(self):
        return self.cur


def _put_files(s3, prefix, n):
    for i in range(n):
        s3.put_object(Bucket=BUCKET, Key=f"{prefix}part-{i:05d}.json.gz", Body=b"x")


def test_puts_each_batch_then_copies(s3, settings):
    _put_files(s3, "raw/cfpb/incremental/load_date=2026-10-18/", 3)
    copy_rows = [
        [("f0", "LOADED", 10, 10), ("f1", "LOADED", 5, 5)],
        [("f2", "LOADED", 7, 7)],
    ]
    conn = FakeConn(copy_rows)

    summary = load_partition(
        "incremental/load_date=2026-10-18", settings=settings, s3=s3, conn=conn, batch_size=2
    )

    assert summary["files"] == 3
    assert summary["rows_loaded"] == 22
    puts = [q for q in conn.cur.sql if q.startswith("put")]
    copies = [q for q in conn.cur.sql if q.startswith("copy")]
    assert len(puts) == 2 and len(copies) == 2
    assert "@RAW.CFPB.CFPB_STAGE/incremental/load_date=2026-10-18/" in puts[0]
    assert "purge = true" in copies[0] and "force = true" in copies[0]


def test_no_force_for_backfill(s3, settings):
    _put_files(s3, "raw/cfpb/backfill/", 1)
    conn = FakeConn([[("Copy executed with 0 files processed.",)]])
    summary = load_partition("backfill/", force=False, settings=settings, s3=s3, conn=conn)
    assert summary["rows_loaded"] == 0
    assert "force = false" in [q for q in conn.cur.sql if q.startswith("copy")][0]


def test_copy_error_raises(s3, settings):
    _put_files(s3, "raw/cfpb/backfill/", 1)
    conn = FakeConn([[("f0", "LOAD_FAILED", 10, 0)]])
    with pytest.raises(RuntimeError, match="COPY failed"):
        load_partition("backfill/", settings=settings, s3=s3, conn=conn)


def test_empty_partition_raises(s3, settings):
    with pytest.raises(FileNotFoundError):
        load_partition("backfill/", settings=settings, s3=s3, conn=FakeConn([]))
