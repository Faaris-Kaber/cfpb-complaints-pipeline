"""Client for the CFPB Consumer Complaint Database search API.

Pagination notes (verified against the live API):
- `frm` is ignored, so offset paging does not work. We use `search_after`.
- `search_after` takes the previous page's last `sort` array joined with "_",
  e.g. sort=[1791158442000, "29090577"] -> search_after=1791158442000_29090577.
- `date_received_min` / `date_received_max` are both inclusive (YYYY-MM-DD).
- `size` above 10,000 returns HTTP 424.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from ingestion.config import API_URL, MAX_PAGE_SIZE

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class CFPBAPIError(RuntimeError):
    pass


def _is_retryable(exc: BaseException) -> bool:
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        return exc.response.status_code in RETRYABLE_STATUS
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class CFPBClient:
    def __init__(
        self,
        base_url: str = API_URL,
        page_size: int = MAX_PAGE_SIZE,
        timeout: float = 120,
        session: requests.Session | None = None,
    ) -> None:
        self.base_url = base_url
        self.page_size = min(page_size, MAX_PAGE_SIZE)
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.setdefault("User-Agent", "cfpb-analytics-pipeline/0.1")

    @retry(
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(6),
        reraise=True,
    )
    def _get(self, params: dict) -> dict:
        resp = self.session.get(self.base_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_range(self, start: date, end: date) -> Iterator[dict]:
        """Yield complaint `_source` dicts with date_received in [start, end].

        Walks one day at a time so each request stays small, and pages within
        a day using search_after on (created_date, complaint_id).
        """
        if end < start:
            raise ValueError(f"end {end} is before start {start}")
        day = start
        while day <= end:
            yield from self._fetch_day(day)
            day += timedelta(days=1)

    def _fetch_day(self, day: date) -> Iterator[dict]:
        params: dict = {
            "no_aggs": "true",
            "date_received_min": day.isoformat(),
            "date_received_max": day.isoformat(),
            "sort": "created_date_asc",
            "size": self.page_size,
        }
        expected: int | None = None
        seen = 0
        while True:
            payload = self._get(params)
            meta = payload.get("_meta") or {}
            if meta.get("is_data_stale") or meta.get("has_data_issue"):
                log.warning("CFPB API reports stale data or a data issue: %s", meta)

            hits = (payload.get("hits") or {}).get("hits")
            if hits is None:
                raise CFPBAPIError(f"Unexpected response shape for {day}: {list(payload)}")
            if expected is None:
                expected = payload["hits"]["total"]["value"]

            for hit in hits:
                yield hit["_source"]
            seen += len(hits)

            if len(hits) < self.page_size:
                break
            last_sort = hits[-1]["sort"]
            params["search_after"] = "_".join(str(v) for v in last_sort)

        log.info("day=%s fetched=%d expected=%s", day, seen, expected)
        # Complaints filed while we page can push `seen` above `expected`; that
        # is fine. Fewer rows than reported means a page was dropped.
        if expected is not None and seen < expected:
            raise CFPBAPIError(f"{day}: fetched {seen} rows but API reported {expected}")
