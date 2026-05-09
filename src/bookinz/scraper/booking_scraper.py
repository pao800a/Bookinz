"""booking.com scraper — extracts raw accommodation stats for a given area."""

from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Iterator
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
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
    num_adults: int     # number of adult guests searched for

    # Stats
    total_price: float | None             # total price for the stay shown by booking.com
    currency: str | None
    rating: float | None                  # 0–10 guest review score
    rating_category: str | None           # e.g. "Superb", "Very Good"
    num_reviews: int | None
    distance_from_center_km: float | None
    num_rooms_available: int | None       # remaining rooms shown by booking.com
    neighbourhood: str | None = None          # area within the city (e.g. "Trionfale, Rome")
    accommodation_type: str | None = None     # e.g. "Entire apartment", "Hotel room"
    tags: str | None = None                   # pipe-separated amenity/badge labels
    is_available: bool = True

    # Raw HTML fragment (kept for re-parsing in silver layer)
    raw_html_snippet: str = field(default="", repr=False)


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

_BASE_URL     = "https://www.booking.com/searchresults.html"
_HOMEPAGE_URL = "https://www.booking.com/"

# Extra HTTP headers injected on every Playwright request.
_DEFAULT_HEADERS = {
    "Accept-Language": "en-GB,en;q=0.9",
}


# ---------------------------------------------------------------------------
# Parser helpers
# ---------------------------------------------------------------------------

def _safe_float(text: str | None) -> float | None:
    if not text:
        return None
    # Strip whitespace and non-breaking spaces, then keep only digits, dots, commas
    cleaned = text.strip().replace("\xa0", "").replace(" ", "")
    numeric = "".join(c for c in cleaned if c.isdigit() or c in ",.")
    if not numeric:
        return None

    has_dot   = "." in numeric
    has_comma = "," in numeric

    try:
        if has_dot and has_comma:
            # Whichever separator appears last is the decimal separator.
            # "1.234,56" → dot=thousands, comma=decimal → 1234.56
            # "1,234.56" → comma=thousands, dot=decimal  → 1234.56
            if numeric.rindex(".") > numeric.rindex(","):
                result = numeric.replace(",", "")           # remove thousands commas
            else:
                result = numeric.replace(".", "").replace(",", ".")  # remove thousands dots
        elif has_dot and not has_comma:
            parts = numeric.split(".")
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                # Trailing 3-digit groups that are all zeros → decimal separator,
                # not thousands.  "758.000" → 758.0  (round number with .000 suffix)
                # Non-zero trailing groups → thousands separator.
                # "1.101" → 1101.0
                if all(p == "0" * len(p) for p in parts[1:]):
                    result = parts[0]                  # "758.000" → "758"
                else:
                    result = numeric.replace(".", "")  # "1.101"   → "1101"
            else:
                result = numeric                       # "8.5"     → "8.5"
        elif has_comma and not has_dot:
            parts = numeric.split(",")
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                # Trailing 3-digit groups after lone comma → thousands separator.
                # "1,101" → 1101.0,  "1,000" → 1000.0
                result = numeric.replace(",", "")
            else:
                # Comma is the decimal separator: "8,5" → 8.5,  "8,50" → 8.50
                result = numeric.replace(",", ".")
        else:
            result = numeric

        return float(result) if result else None
    except ValueError:
        return None


def _safe_int(text: str | None) -> int | None:
    if not text:
        return None
    cleaned = "".join(c for c in text if c.isdigit())
    return int(cleaned) if cleaned else None


def _extract_facility_id(card: BeautifulSoup) -> str:
    # Try explicit data attributes first
    fid = (
        card.get("data-hotelid")
        or card.get("data-property-card-id")
    )
    if fid:
        return fid
    # Fall back to extracting the hotel slug from the title-link URL
    # e.g. /hotel/pt/hotel-name.en-gb.html  →  "hotel-name"
    anchor = card.find("a", {"data-testid": "title-link"}) or card.find(
        "a", href=lambda h: h and "/hotel/" in h
    )
    if anchor:
        href = anchor.get("href", "")
        # split on '/' and drop empty segments (handles both relative and absolute URLs)
        segments = [s for s in href.split("/") if s]
        hotel_idx = next((i for i, s in enumerate(segments) if s == "hotel"), None)
        if hotel_idx is not None and hotel_idx + 2 < len(segments):
            # segment after /hotel/<country-code>/ is the slug; strip query string + language suffix
            slug = segments[hotel_idx + 2].split("?")[0]  # remove query string first
            return slug.split(".")[0]                     # then strip language suffix
    return card.get("id", "unknown")


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
    if not score_el:
        return None, None
    # Numeric score is in the aria-hidden="true" element (e.g. "8.5").
    # The surrounding "Scored 8.5" text would give "Scor" if we just do [:4].
    numeric_el = score_el.find(attrs={"aria-hidden": "true"})
    if numeric_el:
        score = _safe_float(numeric_el.get_text(strip=True))
    else:
        m = re.search(r"\b(\d+(?:[.,]\d+)?)\b", score_el.get_text())
        score = _safe_float(m.group(1)) if m else None
    # Category (e.g. "Very good") is the first span inside aria-hidden="false" div.
    visible_el = score_el.find(attrs={"aria-hidden": "false"})
    if visible_el:
        spans = visible_el.find_all("span")
        category = spans[0].get_text(strip=True) if spans else None
    else:
        cat_el = card.find(attrs={"data-testid": "review-score-word"}) or card.find(
            attrs={"class": "bui-review-score__title"}
        )
        category = cat_el.get_text(strip=True) if cat_el else None
    return score, category


def _extract_num_reviews(card: BeautifulSoup) -> int | None:
    score_el = card.find(attrs={"data-testid": "review-score"})
    if score_el:
        # Reviews count is the second span inside the aria-hidden="false" div,
        # e.g. "\u00b7\u00a026,485 reviews" → 26485.
        visible_el = score_el.find(attrs={"aria-hidden": "false"})
        if visible_el:
            spans = visible_el.find_all("span")
            if len(spans) >= 2:
                return _safe_int(spans[1].get_text())
    # Legacy fallback
    el = card.find(attrs={"data-testid": "review-score-count"}) or card.find(
        attrs={"class": "bui-review-score__text"}
    )
    return _safe_int(el.get_text() if el else None)


def _extract_neighbourhood(card: BeautifulSoup) -> str | None:
    """Returns the neighbourhood/area portion of the location element.

    Booking.com format: ``"Neighbourhood, City \u2022 X km from centre"``
    Targets the inner text span to avoid SVG content from the pin icon,
    then strips the distance suffix via regex.
    """
    loc_el = card.find(attrs={"data-testid": "location"})
    if not loc_el:
        return None
    # Real HTML wraps the human-readable text in <span class="beb5ef4fb4">
    inner = loc_el.find("span", class_="beb5ef4fb4")
    text = (inner or loc_el).get_text(strip=True)
    # Strip the "X km from centre" suffix and any preceding separator
    m = re.match(r"^(.+?)\s*[\u2022\u00b7\u2027\-\|]\s*[\d.,]+\s*km", text, re.IGNORECASE)
    if m:
        return m.group(1).strip() or None
    # Fallback: remove trailing km info if no separator found
    stripped = re.sub(r"\s*[\d.,]+\s*km.*$", "", text, flags=re.IGNORECASE).strip()
    return stripped or None


def _extract_accommodation_type(card: BeautifulSoup) -> str | None:
    """Returns the accommodation type from the unit-configuration element.

    E.g. ``"Entire apartment \u2013 80 m\u00b2: 3 beds \u2026"`` → ``"Entire apartment"``.
    Prefers the ``<b>`` child element (contains only the type + separator, no
    sibling span noise), then falls back to full element text.
    """
    el = card.find(attrs={"data-testid": "unit-configuration"})
    if not el:
        return None
    # Real HTML: <b>Entire apartment \u2013 80 m\u00b2: </b><span>3 beds</span>...
    bold = el.find("b")
    text = (bold or el).get_text(strip=True)
    # Split on en-dash, em-dash, or colon — whichever comes first
    for sep in ("\u2013", "\u2014", " - ", ":"):
        if sep in text:
            part = text.split(sep)[0].strip()
            return part or None
    return text[:80] or None


def _extract_tags(card: BeautifulSoup) -> str | None:
    """Returns pipe-separated amenity/badge labels from the card.

    Combines text from ``data-testid="badges"`` with the amenity labels
    that follow each ``data-testid="icon-with-text-icon"`` span (skipping the
    location-pin icon whose parent has ``data-testid="location"``).
    """
    tags: list[str] = []
    # Text badges (e.g. "New to Booking.com", "Genius")
    badges_el = card.find(attrs={"data-testid": "badges"})
    if badges_el:
        badge_text = badges_el.get_text(strip=True)
        if badge_text:
            tags.append(badge_text)
    # Amenity icons (e.g. "Swimming pool", "Free airport taxi")
    for icon_span in card.find_all("span", attrs={"data-testid": "icon-with-text-icon"}):
        # The location element also uses this icon — skip it
        parent = icon_span.parent
        if parent and parent.get("data-testid") == "location":
            continue
        sibling = icon_span.find_next_sibling("span")
        if sibling:
            text = sibling.get_text(strip=True)
            if text and text not in tags:
                tags.append(text)
    return "|".join(tags) if tags else None


def _extract_is_available(card: BeautifulSoup, price: float | None) -> bool:
    """Returns True if the card represents a bookable, available property.

    A card is considered unavailable when:
    - No price is shown (``price is None``).
    - The card text contains known unavailability signals from booking.com.
    """
    if price is None:
        return False
    card_text = card.get_text(separator=" ", strip=True).lower()
    unavailability_phrases = (
        "sold out",
        "no availability",
        "not available for your dates",
        "see availability",
        "unavailable",
    )
    return not any(phrase in card_text for phrase in unavailability_phrases)


def _extract_distance(card: BeautifulSoup) -> float | None:
    """Returns distance from city centre in km."""
    # Modern booking.com embeds distance in a 'location' element:
    # e.g. "07. Erzsébetváros, Budapest • 2.3 km from centre"
    loc_el = (
        card.find(attrs={"data-testid": "location"})
        or card.find(attrs={"data-testid": "distance"})
    )
    if loc_el:
        text = loc_el.get_text(strip=True)
    else:
        nav = card.find(string=lambda t: t and ("km from centre" in t or "km from center" in t))
        text = str(nav) if nav else None
    if text:
        m = re.search(r"([\d.,]+)\s*km", text, re.IGNORECASE)
        if m:
            return _safe_float(m.group(1))
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
    num_adults: int = 2,
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
        num_adults=num_adults,
        total_price=price,
        currency=currency,
        rating=rating,
        rating_category=rating_category,
        num_reviews=_extract_num_reviews(card),
        distance_from_center_km=_extract_distance(card),
        num_rooms_available=_extract_rooms_available(card),
        neighbourhood=_extract_neighbourhood(card),
        accommodation_type=_extract_accommodation_type(card),
        tags=_extract_tags(card),
        is_available=_extract_is_available(card, price),
        raw_html_snippet=str(card)[:8192],
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

        ua = UserAgent()
        self._pw = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=ua.random,
            locale="en-GB",
            extra_http_headers=_DEFAULT_HEADERS,
        )
        self._page = self._context.new_page()
        self._warm_up()

    def _warm_up(self) -> None:
        """Navigate to the booking.com homepage to acquire session cookies."""
        logger.debug("Warming up browser session via homepage: %s", _HOMEPAGE_URL)
        try:
            self._page.goto(_HOMEPAGE_URL, wait_until="domcontentloaded", timeout=30_000)
            cookies = [c["name"] for c in self._context.cookies()]
            logger.debug("Homepage warm-up OK — cookies=%s", cookies)
        except PlaywrightTimeout:
            logger.warning("Homepage warm-up timed out (non-fatal).")
        finally:
            time.sleep(self.request_delay_s)

    def __del__(self) -> None:
        """Close the browser when the scraper is garbage-collected."""
        try:
            self._browser.close()
            self._pw.stop()
        except Exception:  # noqa: BLE001
            pass

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
        logger.debug("Navigating to %s", url)
        try:
            response = self._page.goto(url, wait_until="networkidle", timeout=30_000)
        except PlaywrightTimeout:
            logger.warning("networkidle timeout — falling back to domcontentloaded")
            response = self._page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        status = response.status if response else 0
        logger.debug(
            "Response: status=%d, url=%s, size=%d bytes",
            status,
            self._page.url,
            len(self._page.content()),
        )
        html = self._page.content()
        return html

    @staticmethod
    def _extract_cards(html: str) -> list[BeautifulSoup]:
        soup = BeautifulSoup(html, "lxml")
        # booking.com property cards
        cards = soup.find_all("div", {"data-testid": "property-card"})
        if not cards:
            # fallback to older markup
            cards = soup.find_all("div", {"data-hotelid": True})
        return cards

    def _scroll_to_load_all_cards(self) -> None:
        """Scroll the page slowly to trigger lazy-loaded property cards."""
        try:
            for step in range(1, 7):
                self._page.evaluate(
                    f"window.scrollTo(0, document.body.scrollHeight * {step} / 6)"
                )
                time.sleep(0.4)
            # Pause at the bottom for any trailing lazy content, then scroll back up
            time.sleep(0.8)
            self._page.evaluate("window.scrollTo(0, 0)")
            time.sleep(0.3)
        except Exception:  # noqa: BLE001
            pass

    def _navigate_to_next_page(self) -> bool:
        """Click the 'Next page' button on the current results page.

        Returns True if the click succeeded and the next page loaded,
        False if no active next-page control was found (last page).
        """
        selectors = [
            "[data-testid='pagination-next']",
            "button[aria-label='Next page']",
            "a[aria-label='Next page']",
            ".bui-pagination__item--action-next a",
            "li.bui-pagination__next-arrow a",
        ]
        for selector in selectors:
            try:
                el = self._page.query_selector(selector)
                if el is None or not el.is_visible():
                    continue
                # A disabled button signals the last page
                if (
                    el.get_attribute("aria-disabled") == "true"
                    or el.get_attribute("disabled") is not None
                ):
                    logger.debug("Next-page button is disabled — last page reached.")
                    return False
                el.click()
                try:
                    self._page.wait_for_load_state("networkidle", timeout=30_000)
                except PlaywrightTimeout:
                    self._page.wait_for_load_state("domcontentloaded", timeout=15_000)
                logger.debug("Navigated to next page via button click.")
                return True
            except PlaywrightTimeout:
                raise
            except Exception as exc:  # noqa: BLE001
                logger.debug("Next-page selector %r failed: %s", selector, exc)
        logger.debug("No next-page button found on current page.")
        return False

    def _click_load_more(self) -> bool:
        """Scroll to the bottom of the page and click 'Load more results'.

        Booking.com appends new cards to the existing DOM rather than
        navigating to a new URL.  Returns True if the button was found
        and clicked, False if no such button exists (results exhausted).
        """
        try:
            self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            time.sleep(1.0)
            btn = None
            for text_variant in ["Load more results", "Show more results", "More results"]:
                btn = self._page.query_selector(f"button:has-text('{text_variant}')")
                if btn and btn.is_visible():
                    break
                btn = None
            if btn is None:
                logger.debug("No 'Load more results' button found.")
                return False
            btn.scroll_into_view_if_needed()
            btn.click()
            try:
                self._page.wait_for_load_state("networkidle", timeout=20_000)
            except PlaywrightTimeout:
                self._page.wait_for_load_state("domcontentloaded", timeout=10_000)
            logger.debug("Clicked 'Load more results' button.")
            return True
        except Exception as exc:  # noqa: BLE001
            logger.debug("_click_load_more failed: %s", exc)
            return False

    @staticmethod
    def _log_no_cards_diagnostics(html: str) -> None:
        """Log page title and a short HTML snippet to aid bot-detection diagnosis."""
        soup = BeautifulSoup(html, "lxml")
        title_tag = soup.find("title")
        page_title = title_tag.get_text(strip=True) if title_tag else "(no <title> tag)"
        logger.debug("Page title: %s", page_title)
        # Log the first 1 500 chars of rendered text (strips tags) so the log
        # file captures enough context without becoming unreadable.
        body_text = soup.get_text(separator=" ", strip=True)[:1500]
        logger.debug("Page text snippet: %s", body_text)

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
        seen_keys: set[str] = set()   # dedup keys already collected

        for page in range(self.max_pages):
            # Page 1: navigate via the search URL.
            # Pages 2+: click the "Next page" button instead of building an
            # offset URL — booking.com ignores the offset parameter on direct
            # requests and returns page 1 again, which trips the dedup stop.
            if page == 0:
                url = self._build_url(0)
                logger.info(
                    "Scraping page 1/%d for '%s'",
                    self.max_pages,
                    self.search_area,
                )
                try:
                    html = self._fetch_page(url)
                except requests.RequestException as exc:
                    logger.error("Failed to fetch page %d: %s", page + 1, exc)
                    break
                self._scroll_to_load_all_cards()
                html = self._page.content()
            else:
                logger.info(
                    "Scraping page %d/%d for '%s'",
                    page + 1,
                    self.max_pages,
                    self.search_area,
                )
                time.sleep(self.request_delay_s)
                # Try "Load more results" first (booking.com's preferred pagination);
                # fall back to traditional "Next page" button.
                if not self._click_load_more():
                    if not self._navigate_to_next_page():
                        logger.info(
                            "No more pages after %d batches — stopping.", page
                        )
                        break
                self._scroll_to_load_all_cards()
                html = self._page.content()

            cards = self._extract_cards(html)
            if not cards:
                logger.info("No property cards found on page %d — stopping.", page + 1)
                self._log_no_cards_diagnostics(html)
                break

            new_records: list[AccommodationRecord] = []
            for card in cards:
                try:
                    record = _parse_card(
                        card,
                        self.search_area,
                        self.checkin_date,
                        self.checkout_date,
                        scraped_at,
                        self.num_adults,
                    )
                    new_records.append(record)
                except (AttributeError, ValueError, KeyError, TypeError) as exc:
                    logger.warning("Error parsing card: %s", exc)

            # Build a dedup key per record: prefer facility_id when available,
            # fall back to hotel name (always present) so dedup works even when
            # booking.com omits the data-hotelid attribute and URL extraction fails.
            def _dedup_key(r: AccommodationRecord) -> str:
                return r.facility_id if r.facility_id != "unknown" else r.name

            # Duplicate-page detection: if every hotel on this page was already
            # collected, booking.com has looped back to a previous page — stop.
            page_keys = {_dedup_key(r) for r in new_records}
            if page_keys and page_keys.issubset(seen_keys):
                logger.info(
                    "Page %d contains only already-seen hotels — stopping.",
                    page + 1,
                )
                break

            for record in new_records:
                key = _dedup_key(record)
                if key not in seen_keys:
                    records.append(record)
                    seen_keys.add(key)

            logger.info("Page %d: collected %d records so far.", page + 1, len(records))

            if len(cards) < self._RESULTS_PER_PAGE:
                logger.info("Last page reached (fewer than %d results).", self._RESULTS_PER_PAGE)
                break

        return records

    def scrape_as_dicts(self, scraped_at: str) -> list[dict]:
        """Same as :meth:`scrape` but returns plain dicts (ready for pandas)."""
        return [asdict(r) for r in self.scrape(scraped_at)]
