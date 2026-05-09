"""AirBnB availability scraper — fetches calendar availability for individual
listings via the AirBnB calendar API.

For each AirBnB listing the scraper uses a Playwright browser session
(which carries real session cookies/headers from browsing the site first)
to call:

    GET https://www.airbnb.com/api/v2/calendar_months
        ?listing_id={id}&month={m}&year={y}&count={months}&_format=with_conditions

This produces one row per ``(facility_id, calendar_date)`` pair, indicating
whether each date is available and the minimum-stay requirement.

This scraper produces the ``airbnb_availability`` dataset — fully independent
from the accommodation-search and facility datasets.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from typing import Any

from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_AIRBNB_HOME_URL  = "https://www.airbnb.com/"
_CALENDAR_API_URL = (
    "https://www.airbnb.com/api/v2/calendar_months"
    "?listing_id={listing_id}&month={month}&year={year}"
    "&count={count}&_format=with_conditions"
)
_PAGE_TIMEOUT_MS = 30_000


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class AirbnbAvailabilityRecord:
    """One calendar-date availability record for a single AirBnB listing."""

    facility_id: str
    date: str        # ISO-8601 date string (YYYY-MM-DD)
    is_available: bool
    min_nights: int | None
    scraped_at: str  # ISO-8601 datetime

    def as_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# JSON parser helpers
# ---------------------------------------------------------------------------

def _parse_calendar_response(
    facility_id: str,
    scraped_at: str,
    response_json: dict,
) -> list[AirbnbAvailabilityRecord]:
    """Extract per-day availability records from the calendar API response."""
    records: list[AirbnbAvailabilityRecord] = []

    calendar_months = response_json.get("calendar_months", [])
    for month_obj in calendar_months:
        for day_obj in month_obj.get("days", []):
            date_str = day_obj.get("date")
            if not date_str:
                continue

            available = bool(day_obj.get("available", False))

            min_nights_raw = day_obj.get("min_nights")
            try:
                min_nights: int | None = int(min_nights_raw) if min_nights_raw is not None else None
            except (TypeError, ValueError):
                min_nights = None

            records.append(
                AirbnbAvailabilityRecord(
                    facility_id  = facility_id,
                    date         = str(date_str),
                    is_available = available,
                    min_nights   = min_nights,
                    scraped_at   = scraped_at,
                )
            )

    return records


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class AirbnbAvailabilityScraper:
    """Fetches calendar availability for AirBnB listings via the internal API.

    The scraper opens a Playwright browser, visits the AirBnB homepage to
    acquire session cookies, then uses ``page.request.get()`` to call the
    calendar API — which automatically includes the necessary cookies and
    headers.

    Parameters
    ----------
    months_ahead:
        Number of calendar months to fetch per listing (default: 6, max: 12).
    request_delay_s:
        Polite delay (seconds) between API calls.
    headless:
        Run the browser in headless mode (default ``True``).
    """

    def __init__(
        self,
        months_ahead: int = 6,
        request_delay_s: float = 2.0,
        headless: bool = True,
    ) -> None:
        self.months_ahead      = max(1, min(months_ahead, 12))
        self.request_delay_s   = request_delay_s
        self.headless          = headless
        self._pw               = None
        self._browser          = None
        self._page             = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "AirbnbAvailabilityScraper":
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        ctx           = self._browser.new_context(
            locale             = "en-US",
            extra_http_headers = {"Accept-Language": "en-US,en;q=0.9"},
        )
        self._page = ctx.new_page()
        self._warm_up_session()
        logger.info(
            "AirbnbAvailabilityScraper browser started (headless=%s, months=%d).",
            self.headless,
            self.months_ahead,
        )
        return self

    def __exit__(self, *_: object) -> None:
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        logger.info("AirbnbAvailabilityScraper browser closed.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(
        self,
        facility_id: str,
        scraped_at: str,
    ) -> list[AirbnbAvailabilityRecord]:
        """Fetch calendar availability for one listing.

        Parameters
        ----------
        facility_id:
            AirBnB listing ID.
        scraped_at:
            ISO-8601 datetime string (shared across a pipeline run).

        Returns
        -------
        list[AirbnbAvailabilityRecord]
            One record per calendar date in the requested range.
        """
        assert self._page is not None, "Use AirbnbAvailabilityScraper as a context manager."

        today   = datetime.now(tz=timezone.utc).date()
        month   = today.month
        year    = today.year

        url = _CALENDAR_API_URL.format(
            listing_id = facility_id,
            month      = month,
            year       = year,
            count      = self.months_ahead,
        )

        try:
            response = self._page.request.get(
                url,
                headers={"Accept": "application/json"},
                timeout=_PAGE_TIMEOUT_MS,
            )
        except PlaywrightTimeout as exc:
            logger.error("Calendar API timeout for facility %s: %s", facility_id, exc)
            return []

        if response.status != 200:
            logger.warning(
                "Calendar API returned HTTP %d for facility %s.",
                response.status,
                facility_id,
            )
            return []

        try:
            data = response.json()
        except Exception as exc:
            logger.error("JSON parse error from calendar API for facility %s: %s", facility_id, exc)
            return []

        records = _parse_calendar_response(facility_id, scraped_at, data)
        logger.info("Facility %s: %d calendar day(s) fetched.", facility_id, len(records))
        return records

    def scrape_as_dicts(
        self,
        facility_ids: list[str],
        scraped_at: str,
    ) -> list[dict]:
        """Fetch availability for multiple listings and return as dicts.

        Parameters
        ----------
        facility_ids:
            List of AirBnB listing IDs.
        scraped_at:
            ISO-8601 datetime string.
        """
        results: list[dict] = []
        for i, fid in enumerate(facility_ids):
            try:
                records = self.scrape(fid, scraped_at)
                results.extend(r.as_dict() for r in records)
            except Exception as exc:
                logger.error("Failed to fetch availability for facility %s: %s", fid, exc)

            if i < len(facility_ids) - 1:
                time.sleep(self.request_delay_s)

        return results

    # ------------------------------------------------------------------
    # Session warm-up
    # ------------------------------------------------------------------

    def _warm_up_session(self) -> None:
        """Visit the AirBnB homepage to acquire session cookies."""
        try:
            self._page.goto(_AIRBNB_HOME_URL, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT_MS)
            time.sleep(2)
            logger.debug("Session warmed up via %s.", _AIRBNB_HOME_URL)
        except PlaywrightTimeout:
            logger.warning("Timeout warming up session — API calls may fail.")
