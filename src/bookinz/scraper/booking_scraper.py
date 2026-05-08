"""booking.com scraper — extracts raw accommodation stats for a given area."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Iterator
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class AccommodationRecord:
    """Raw accommodation record — one row in the bronze layer."""

    # Identifiers
    facility_id: str  # booking.com internal property ID (data-hotelid)
    name: str
    url: str

    # Search context
    search_area: str
    checkin_date: str   # ISO-8601
    checkout_date: str  # ISO-8601
    scraped_at: str     # ISO-8601 datetime

    # Stats
    price_per_night: float | None         # in the local currency shown
    currency: str | None
    rating: float | None                  # 0–10 guest review score
    rating_category: str | None           # e.g. "Superb", "Very Good"
    num_reviews: int | None
    distance_from_center_km: float | None
    num_rooms_available: int | None       # remaining rooms shown by booking.com
    is_available: bool = True

    # Raw HTML fragment (kept for re-parsing in silver layer)
    raw_html_snippet: str = field(default="", repr=False)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.booking.com/searchresults.html"
_PROPERTY_URL = "https://www.booking.com/hotel/gb/{facility_id}.html"

_DEFAULT_HEADERS = {
    "Accept-Language": "en-GB,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Connection": "keep-alive",
}


def _build_session() -> requests.Session:
    session = requests.Session()
    ua = UserAgent()
    session.headers.update({**_DEFAULT_HEADERS, "User-Agent": ua.random})
    return session


# ---------------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------------

def _safe_float(text: str | None) -> float | None:
    if not text:
        return None
    cleaned = text.strip().replace(",", ".").replace("\xa0", "")
    # keep only numeric chars and the decimal separator
    numeric = "".join(c for c in cleaned if c.isdigit() or c == ".")
    try:
        return float(numeric) if numeric else None
    except ValueError:
        return None


def _safe_int(text: str | None) -> int | None:
    if not text:
        return None
    cleaned = "".join(c for c in text if c.isdigit())
    return int(cleaned) if cleaned else None


def _extract_facility_id(card: BeautifulSoup) -> str:
    return (
        card.get("data-hotelid")
        or card.get("data-property-card-id")
        or card.get("id", "unknown")
    )


def _extract_name(card: BeautifulSoup) -> str:
    for selector in [
        {"data-testid": "title"},
        {"class": "sr-hotel__name"},
    ]:
        el = card.find(attrs=selector)
        if el:
            return el.get_text(strip=True)
    return "N/A"


def _extract_url(card: BeautifulSoup) -> str:
    anchor = card.find("a", {"data-testid": "title-link"}) or card.find(
        "a", href=lambda h: h and "/hotel/" in h
    )
    if anchor:
        href = anchor.get("href", "")
        return href if href.startswith("http") else f"https://www.booking.com{href}"
    return ""


def _extract_price(card: BeautifulSoup) -> tuple[float | None, str | None]:
    price_el = card.find(attrs={"data-testid": "price-and-discounted-price"}) or card.find(
        attrs={"class": "bui-price-display__value"}
    )
    if not price_el:
        return None, None
    text = price_el.get_text(strip=True)
    # Currency symbol is typically the first non-digit character(s)
    currency = "".join(c for c in text if not c.isdigit() and c not in ".,\xa0 ").strip() or None
    return _safe_float(text), currency


def _extract_rating(card: BeautifulSoup) -> tuple[float | None, str | None]:
    score_el = card.find(attrs={"data-testid": "review-score"})
    if not score_el:
        score_el = card.find(attrs={"class": "bui-review-score__badge"})
    if score_el:
        score_text = score_el.get_text(strip=True)
        score = _safe_float(score_text[:4])
        # Category (e.g. "Superb")
        cat_el = card.find(attrs={"data-testid": "review-score-word"}) or card.find(
            attrs={"class": "bui-review-score__title"}
        )
        category = cat_el.get_text(strip=True) if cat_el else None
        return score, category
    return None, None


def _extract_num_reviews(card: BeautifulSoup) -> int | None:
    el = card.find(attrs={"data-testid": "review-score-count"}) or card.find(
        attrs={"class": "bui-review-score__text"}
    )
    return _safe_int(el.get_text() if el else None)


def _extract_distance(card: BeautifulSoup) -> float | None:
    """Returns distance from city centre in km."""
    el = card.find(attrs={"data-testid": "distance"}) or card.find(
        string=lambda t: t and ("km from centre" in t or "km from center" in t)
    )
    if el:
        text = el if isinstance(el, str) else el.get_text()
        return _safe_float(text.split("km")[0])
    return None


def _extract_rooms_available(card: BeautifulSoup) -> int | None:
    """Parses 'Only X rooms left' urgency messages."""
    for el in card.find_all(string=True):
        text = el.strip().lower()
        if "room" in text and ("left" in text or "remaining" in text or "only" in text):
            return _safe_int(el)
    return None


def _parse_card(
    card: BeautifulSoup,
    search_area: str,
    checkin_date: str,
    checkout_date: str,
    scraped_at: str,
) -> AccommodationRecord:
    price, currency = _extract_price(card)
    rating, rating_category = _extract_rating(card)
    return AccommodationRecord(
        facility_id=_extract_facility_id(card),
        name=_extract_name(card),
        url=_extract_url(card),
        search_area=search_area,
        checkin_date=checkin_date,
        checkout_date=checkout_date,
        scraped_at=scraped_at,
        price_per_night=price,
        currency=currency,
        rating=rating,
        rating_category=rating_category,
        num_reviews=_extract_num_reviews(card),
        distance_from_center_km=_extract_distance(card),
        num_rooms_available=_extract_rooms_available(card),
        is_available=price is not None,
        raw_html_snippet=str(card)[:4096],
    )


# ---------------------------------------------------------------------------
# Main scraper
# ---------------------------------------------------------------------------

class BookingComScraper:
    """Scrapes accommodation listings from booking.com.

    Parameters
    ----------
    search_area:
        City or region name as it appears in booking.com search (e.g. "Amsterdam").
    checkin_date:
        ISO-8601 check-in date (``YYYY-MM-DD``).
    checkout_date:
        ISO-8601 check-out date (``YYYY-MM-DD``).
    num_adults:
        Number of adult guests (default 2).
    max_pages:
        Maximum result pages to scrape (each page has ~25 listings).
    request_delay_s:
        Polite delay between HTTP requests (seconds).
    """

    _RESULTS_PER_PAGE = 25

    def __init__(
        self,
        search_area: str,
        checkin_date: str,
        checkout_date: str,
        num_adults: int = 2,
        max_pages: int = 5,
        request_delay_s: float = 2.0,
    ) -> None:
        self.search_area = search_area
        self.checkin_date = checkin_date
        self.checkout_date = checkout_date
        self.num_adults = num_adults
        self.max_pages = max_pages
        self.request_delay_s = request_delay_s
        self._session = _build_session()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_url(self, offset: int = 0) -> str:
        checkin = date.fromisoformat(self.checkin_date)
        checkout = date.fromisoformat(self.checkout_date)
        params = {
            "ss": self.search_area,
            "checkin_year": checkin.year,
            "checkin_month": checkin.month,
            "checkin_monthday": checkin.day,
            "checkout_year": checkout.year,
            "checkout_month": checkout.month,
            "checkout_monthday": checkout.day,
            "group_adults": self.num_adults,
            "no_rooms": 1,
            "offset": offset,
            "lang": "en-gb",
        }
        return f"{_BASE_URL}?{urlencode(params)}"

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_page(self, url: str) -> str:
        logger.debug("GET %s", url)
        response = self._session.get(url, timeout=15)
        response.raise_for_status()
        return response.text

    @staticmethod
    def _extract_cards(html: str) -> list[BeautifulSoup]:
        soup = BeautifulSoup(html, "lxml")
        # booking.com property cards
        cards = soup.find_all("div", {"data-testid": "property-card"})
        if not cards:
            # fallback to older markup
            cards = soup.find_all("div", {"data-hotelid": True})
        return cards

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, scraped_at: str) -> list[AccommodationRecord]:
        """Scrape all pages and return a flat list of :class:`AccommodationRecord`.

        Parameters
        ----------
        scraped_at:
            ISO-8601 datetime string (e.g. ``"2024-01-15T08:00:00"``).
        """
        records: list[AccommodationRecord] = []

        for page in range(self.max_pages):
            offset = page * self._RESULTS_PER_PAGE
            url = self._build_url(offset)
            logger.info(
                "Scraping page %d/%d for '%s' (offset=%d)",
                page + 1,
                self.max_pages,
                self.search_area,
                offset,
            )
            try:
                html = self._fetch_page(url)
            except requests.RequestException as exc:
                logger.error("Failed to fetch page %d: %s", page + 1, exc)
                break

            cards = self._extract_cards(html)
            if not cards:
                logger.info("No property cards found on page %d — stopping.", page + 1)
                break

            for card in cards:
                try:
                    record = _parse_card(
                        card,
                        self.search_area,
                        self.checkin_date,
                        self.checkout_date,
                        scraped_at,
                    )
                    records.append(record)
                except (AttributeError, ValueError, KeyError, TypeError) as exc:
                    logger.warning("Error parsing card: %s", exc)

            logger.info("Page %d: collected %d records so far.", page + 1, len(records))

            if len(cards) < self._RESULTS_PER_PAGE:
                logger.info("Last page reached (fewer than %d results).", self._RESULTS_PER_PAGE)
                break

            time.sleep(self.request_delay_s)

        return records

    def scrape_as_dicts(self, scraped_at: str) -> list[dict]:
        """Same as :meth:`scrape` but returns plain dicts (ready for pandas)."""
        return [asdict(r) for r in self.scrape(scraped_at)]
