import boto3
import pytest
from moto import mock_aws

from ingestion.config import Settings

BUCKET = "test-cfpb-bucket"


@pytest.fixture
def settings() -> Settings:
    return Settings(s3_bucket=BUCKET, page_size=2, rows_per_file=3, lookback_days=2)


@pytest.fixture
def s3(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def make_hit(complaint_id: int, day: str) -> dict:
    return {
        "_source": {
            "complaint_id": str(complaint_id),
            "date_received": f"{day}T12:00:00.000Z",
            "company": "BANK OF EXAMPLE, N.A.",
            "product": "Checking or savings account",
            "timely": "Yes",
        },
        "sort": [1_700_000_000_000 + complaint_id, str(complaint_id)],
    }


def make_page(hits: list[dict], total: int) -> dict:
    return {"hits": {"total": {"value": total, "relation": "eq"}, "hits": hits}, "_meta": {}}
