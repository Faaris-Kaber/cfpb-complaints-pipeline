from datetime import date

import pytest
import requests
import responses
from responses import matchers

from ingestion.cfpb_client import CFPBAPIError, CFPBClient
from ingestion.config import API_URL
from ingestion.tests.conftest import make_hit, make_page


def _no_sleep(client: CFPBClient) -> None:
    client._get.retry.sleep = lambda _s: None


@responses.activate
def test_paginates_with_search_after():
    day = "2026-10-05"
    page1 = make_page([make_hit(1, day), make_hit(2, day)], total=3)
    page2 = make_page([make_hit(3, day)], total=3)

    responses.get(
        API_URL,
        json=page2,
        match=[
            matchers.query_param_matcher({"search_after": "1700000000002_2"}, strict_match=False)
        ],
    )
    responses.get(API_URL, json=page1)

    client = CFPBClient(page_size=2)
    rows = list(client.fetch_range(date(2026, 10, 5), date(2026, 10, 5)))

    assert [r["complaint_id"] for r in rows] == ["1", "2", "3"]
    first_qs = responses.calls[0].request.params
    assert first_qs["date_received_min"] == day
    assert first_qs["date_received_max"] == day
    assert first_qs["sort"] == "created_date_asc"
    assert "search_after" not in first_qs


@responses.activate
def test_walks_each_day_in_range():
    responses.get(API_URL, json=make_page([], total=0))
    client = CFPBClient(page_size=2)
    list(client.fetch_range(date(2026, 10, 1), date(2026, 10, 3)))
    days = [c.request.params["date_received_min"] for c in responses.calls]
    assert days == ["2026-10-01", "2026-10-02", "2026-10-03"]


@responses.activate
def test_raises_when_rows_missing():
    responses.get(API_URL, json=make_page([make_hit(1, "2026-10-05")], total=5))
    client = CFPBClient(page_size=2)
    with pytest.raises(CFPBAPIError, match="fetched 1 rows"):
        list(client.fetch_range(date(2026, 10, 5), date(2026, 10, 5)))


@responses.activate
def test_retries_on_429_then_succeeds():
    responses.get(API_URL, status=429)
    responses.get(API_URL, json=make_page([make_hit(1, "2026-10-05")], total=1))
    client = CFPBClient(page_size=2)
    _no_sleep(client)
    rows = list(client.fetch_range(date(2026, 10, 5), date(2026, 10, 5)))
    assert len(rows) == 1
    assert len(responses.calls) == 2


@responses.activate
def test_retries_truncated_response():
    responses.get(API_URL, body=requests.exceptions.ChunkedEncodingError("connection broken"))
    responses.get(API_URL, json=make_page([make_hit(1, "2026-10-05")], total=1))
    client = CFPBClient(page_size=2)
    _no_sleep(client)
    assert len(list(client.fetch_range(date(2026, 10, 5), date(2026, 10, 5)))) == 1


@responses.activate
def test_does_not_retry_on_404():
    responses.get(API_URL, status=404, json={"detail": "Not found."})
    client = CFPBClient(page_size=2)
    _no_sleep(client)
    with pytest.raises(Exception):  # noqa: B017
        list(client.fetch_range(date(2026, 10, 5), date(2026, 10, 5)))
    assert len(responses.calls) == 1


def test_rejects_inverted_range():
    with pytest.raises(ValueError):
        list(CFPBClient().fetch_range(date(2026, 10, 5), date(2026, 10, 1)))


def test_page_size_capped_at_api_limit():
    assert CFPBClient(page_size=50_000).page_size == 10_000
