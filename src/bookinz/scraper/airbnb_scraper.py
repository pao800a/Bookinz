"""AirBnB scraper — extracts raw accommodation stats for a given area.

The scraper loads AirBnB search-results pages via Playwright (headless Chromium),
parses listing cards with BeautifulSoup, and extracts geolocation data from the
``__NEXT_DATA__`` JSON blob embedded in each page.

AirBnB search URL pattern::

    https://www.airbnb.com/s/<location>/homes
        ?checkin=YYYY-MM-DD&checkout=YYYY-MM-DD&adults=N&page=N

Pagination uses explicit page-number parameters (no infinite scroll).
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from datetime import date
from typing import Iterator
from urllib.parse import quote

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
class AirbnbAccommodationRecord:
    """Raw AirBnB accommodation record — one row in the AirBnB bronze layer."""

    # Identifiers
    facility_id: str        # AirBnB listing ID (from /rooms/<id>)
    name: str
    url: str

    # Search context
    search_area: str
    checkin_date: str       # ISO-8601
    checkout_date: str      # ISO-8601
    scraped_at: str         # ISO-8601 datetime
    num_adults: int

    # Property details
    description: str | None         # raw subtitle line 1
    accommodation_type: str | None  # e.g. "Entire apartment", "Private room"
    neighbourhood: str | None       # city district / area name
    num_bedrooms: int | None
    num_beds: int | None
    host_type: str | None           # "Professional host" / "Private host"

    # Pricing
    total_price: float | None
    currency: str | None
    price_is_per_night: bool        # True if only per-night price was captured

    # Quality signals
    rating: float | None            # 0–5 scale (as shown by AirBnB)
    num_reviews: int | None

    # Badges
    is_superhost: bool
    is_free_cancellation: bool
    tags: str | None                # pipe-separated remaining badge labels

    # Location (from __NEXT_DATA__ JSON map data)
    latitude: float | None
    longitude: float | None

    # Availability
    is_available: bool

    # Raw HTML (for re-parsing)
    raw_html_snippet: str = field(default="", repr=False)

    def as_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_SEARCH_URL   = "https://www.airbnb.com/s/{location}/homes"
_HOMEPAGE_URL = "https://www.airbnb.com/"

_DEFAULT_HEADERS = {
    "Accept-Language": "en-US,en;q=0.9",
}

_UNAVAILABILITY_PHRASES = (
    "sold out",
    "not available",
    "unavailable",
    "no longer available",
)


# ---------------------------------------------------------------------------
# Numeric helpers
# ---------------------------------------------------------------------------

def _safe_float(text: str | None) -> float | None:
    if not text:
        return None
    cleaned = text.strip().replace("\xa0", "").replace("\u202f", "").replace(" ", "")
    numeric = "".join(c for c in cleaned if c.isdigit() or c in ",.")
    if not numeric:
        return None
    has_dot   = "." in numeric
    has_comma = "," in numeric
    try:
        if has_dot and has_comma:
            if numeric.rindex(".") > numeric.rindex(","):
                result = numeric.replace(",", "")
            else:
                result = numeric.replace(".", "").replace(",", ".")
        elif has_dot and not has_comma:
            parts = numeric.split(".")
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                if all(p == "0" * len(p) for p in parts[1:]):
                    result = parts[0]
                else:
                    result = numeric.replace(".", "")
            else:
                result = numeric
        elif has_comma and not has_dot:
            parts = numeric.split(",")
            if len(parts) >= 2 and all(len(p) == 3 for p in parts[1:]):
                result = numeric.replace(",", "")
            else:
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


# ---------------------------------------------------------------------------
# Card-level parser helpers
# ---------------------------------------------------------------------------

def _extract_facility_id_and_url(card: BeautifulSoup) -> tuple[str, str]:
    """Return (facility_id, absolute_url) from the first /rooms/<id> anchor."""
    for anchor in card.find_all("a", href=True):
        href = anchor["href"]
        m = re.search(r"/rooms/(\w+)", href)
        if m:
            fid = m.group(1)
            url = href if href.startswith("http") else f"https://www.airbnb.com{href.split('?')[0]}"
            return fid, url
    return "unknown", ""


def _extract_name(card: BeautifulSoup) -> str:
    # Preferred: explicit test-id
    for testid in ["listing-card-title", "listing-card-name"]:
        el = card.find(attrs={"data-testid": testid})
        if el:
            return el.get_text(strip=True)
    # Fallback: id starting with "title_"
    el = card.find(id=re.compile(r"^title_"))
    if el:
        return el.get_text(strip=True)
    # Fallback: first non-empty <div> that looks like a title (short text, no price)
    for div in card.find_all("div"):
        t = div.get_text(strip=True)
        if 3 < len(t) < 120 and "$" not in t and "€" not in t and "£" not in t:
            return t
    return "N/A"


def _extract_subtitle_lines(card: BeautifulSoup) -> list[str]:
    """Return the two or three subtitle lines shown below the listing title."""
    lines: list[str] = []
    for testid in ["listing-card-subtitle", "listing-card-description"]:
        els = card.find_all(attrs={"data-testid": testid})
        for el in els:
            t = el.get_text(strip=True)
            if t:
                lines.append(t)
    # If test-ids yield nothing, try aria-level-2 spans or secondary text spans
    if not lines:
        for span in card.find_all("span"):
            aria = span.get("aria-hidden", "")
            if aria == "true":
                continue
            t = span.get_text(strip=True)
            if t and len(t) > 5 and t not in lines:
                lines.append(t)
    return lines[:4]


def _parse_accommodation_and_neighbourhood(line: str) -> tuple[str | None, str | None]:
    """Parse a subtitle like 'Entire apartment in Trastevere, Rome'.

    Returns (accommodation_type, neighbourhood).
    """
    # AirBnB format: "<type> in <neighbourhood>, <city>"
    m = re.match(r"^(.+?)\s+in\s+(.+)$", line, re.IGNORECASE)
    if m:
        acc_type     = m.group(1).strip()
        rest         = m.group(2).strip()
        # neighbourhood = everything before the last ", <city>" segment
        neighbourhood = rest.split(",")[0].strip() if "," in rest else rest
        return acc_type or None, neighbourhood or None
    # Fallback: just accommodation type
    if len(line) < 60:
        return line.strip() or None, None
    return None, None


def _extract_bedrooms_beds(subtitle_lines: list[str]) -> tuple[int | None, int | None]:
    """Regex-parse '2 bedrooms · 4 beds · 2 baths' from any subtitle line."""
    text = " ".join(subtitle_lines)
    bedrooms = _safe_int(m.group(1)) if (m := re.search(r"(\d+)\s+bedroom", text, re.IGNORECASE)) else None
    # Match "N beds" but NOT "N bedrooms"
    beds = _safe_int(m.group(1)) if (m := re.search(r"(\d+)\s+bed(?!room)", text, re.IGNORECASE)) else None
    return bedrooms, beds


def _extract_price(card: BeautifulSoup) -> tuple[float | None, str | None, bool]:
    """Return (total_price, currency, price_is_per_night).

    AirBnB may show the total price or a per-night price depending on session
    preferences.  We detect which via the surrounding aria-label text.
    """
    # Strategy 1: aria-label containing "total" or "per night"
    for el in card.find_all(True, {"aria-label": True}):
        label = el.get("aria-label", "").lower()
        if "total" in label or "per night" in label or "a night" in label:
            m = re.search(r"([$€£¥₹A-Z]{1,3})\s*([\d,.]+)", el.get_text(strip=True))
            if m:
                currency = m.group(1).strip()
                price = _safe_float(m.group(2))
                per_night = "per night" in label or "a night" in label
                if price is not None:
                    return price, _normalise_currency(currency), per_night

    # Strategy 2: price spans — prefer "_totalPrice" over "_priceBreakdown"
    for pattern in [r"_totalPrice", r"_priceItem", r"_price"]:
        for el in card.find_all(True, {"class": re.compile(pattern, re.IGNORECASE)}):
            text = el.get_text(strip=True)
            m = re.search(r"([$€£¥₹A-Z]{1,3})[\s\xa0]*([\d,.]+)", text)
            if m:
                currency = m.group(1).strip()
                price = _safe_float(m.group(2))
                if price is not None:
                    return price, _normalise_currency(currency), False

    # Strategy 3: generic price text anywhere in the card
    full_text = card.get_text(separator=" ", strip=True)
    # Match "€ 1,234 total" or "€ 176 per night"
    m = re.search(
        r"([$€£¥₹]|A\$|C\$|NZ\$|CHF|SEK|NOK|DKK)\s*([0-9][0-9,\.\s]*[0-9])"
        r"\s*(per night|a night|total|/night)?",
        full_text,
        re.IGNORECASE,
    )
    if m:
        currency  = m.group(1).strip()
        price     = _safe_float(m.group(2))
        per_night = bool(m.group(3) and ("night" in m.group(3).lower()))
        if price is not None:
            return price, _normalise_currency(currency), per_night

    return None, None, False


def _normalise_currency(symbol: str) -> str:
    """Convert common currency symbols to their ISO code."""
    _MAP = {
        "$":   "USD",
        "€":   "EUR",
        "£":   "GBP",
        "¥":   "JPY",
        "₹":   "INR",
        "A$":  "AUD",
        "C$":  "CAD",
        "NZ$": "NZD",
    }
    return _MAP.get(symbol, symbol)


def _extract_rating_and_reviews(card: BeautifulSoup) -> tuple[float | None, int | None]:
    """Parse rating (0–5) and review count from the card.

    AirBnB embeds these in aria-labels like:
    ``"4.86 out of 5 average rating, 127 reviews"``
    or in a visible ``<span>4.86 (127)</span>``-style element.
    """
    # Strategy 1: aria-label
    for el in card.find_all(True, {"aria-label": True}):
        label = el.get("aria-label", "")
        m = re.search(
            r"([\d.]+)\s+out\s+of\s+5.*?([\d,]+)\s+review",
            label,
            re.IGNORECASE,
        )
        if m:
            return _safe_float(m.group(1)), _safe_int(m.group(2))

    # Strategy 2: visible text matching "4.86 (127)" or "4.86 · 127 reviews"
    text = card.get_text(separator=" ", strip=True)
    m = re.search(
        r"\b(4\.\d{1,2}|[0-4]\.\d{1,2}|5\.0)\s*[·(,]\s*([\d,]+)\s*(?:reviews?|ratings?|\))",
        text,
        re.IGNORECASE,
    )
    if m:
        return _safe_float(m.group(1)), _safe_int(m.group(2))

    # Strategy 3: any standalone rating number near a star symbol
    m = re.search(r"★\s*([\d.]+)", text)
    if m:
        return _safe_float(m.group(1)), None

    return None, None


def _extract_is_superhost(card: BeautifulSoup) -> bool:
    # Explicit test-id badge
    badge = card.find(attrs={"data-testid": "badge-superhost"})
    if badge:
        return True
    # Text-based scan
    text = card.get_text(separator=" ", strip=True).lower()
    return "superhost" in text


def _extract_is_free_cancellation(card: BeautifulSoup) -> bool:
    text = card.get_text(separator=" ", strip=True).lower()
    return "free cancellation" in text


def _extract_host_type(card: BeautifulSoup) -> str | None:
    text = card.get_text(separator=" ", strip=True)
    m = re.search(r"(Professional host|Private host)", text, re.IGNORECASE)
    return m.group(1) if m else None


def _extract_tags(card: BeautifulSoup, is_superhost: bool, is_free_cancellation: bool) -> str | None:
    """Collect badge/pill labels not already captured as dedicated booleans."""
    tags: list[str] = []
    skip_lower = set()
    if is_superhost:
        skip_lower.add("superhost")
    if is_free_cancellation:
        skip_lower.add("free cancellation")

    # Common badge test-ids and class patterns
    for testid in ["badge", "amenity-badge", "pill", "badge-container"]:
        for el in card.find_all(attrs={"data-testid": re.compile(testid, re.IGNORECASE)}):
            t = el.get_text(strip=True)
            if t and t.lower() not in skip_lower and t not in tags:
                tags.append(t)

    # Span elements with aria roles indicating badges/pills
    for el in card.find_all("span", attrs={"aria-label": True}):
        t = el.get("aria-label", "").strip()
        if t and t.lower() not in skip_lower and t not in tags and len(t) < 80:
            tags.append(t)

    return "|".join(tags) if tags else None


def _extract_is_available(card: BeautifulSoup, price: float | None) -> bool:
    if price is None:
        return False
    card_text = card.get_text(separator=" ", strip=True).lower()
    return not any(phrase in card_text for phrase in _UNAVAILABILITY_PHRASES)


# ---------------------------------------------------------------------------
# Coordinate extraction from __NEXT_DATA__
# ---------------------------------------------------------------------------

def _scan_for_coordinates(obj: object, results: dict[str, dict]) -> None:
    """Recursively scan a JSON-decoded object for listing coordinates.

    Looks for dicts that contain **both** a numeric ``latitude``/``longitude``
    and an ``id`` field.  Tolerates structural changes in AirBnB's JSON layout.
    """
    if isinstance(obj, dict):
        has_lat = "latitude"  in obj and isinstance(obj.get("latitude"), (int, float))
        has_lon = "longitude" in obj and isinstance(obj.get("longitude"), (int, float))
        if has_lat and has_lon:
            fid = str(obj.get("id", ""))
            if fid and fid not in results:
                results[fid] = {
                    "latitude":  float(obj["latitude"]),
                    "longitude": float(obj["longitude"]),
                }
        for v in obj.values():
            _scan_for_coordinates(v, results)
    elif isinstance(obj, list):
        for item in obj:
            _scan_for_coordinates(item, results)


def _extract_coordinates_from_page(html: str) -> dict[str, tuple[float, float]]:
    """Return ``{facility_id: (latitude, longitude)}`` from ``__NEXT_DATA__``.

    Falls back to an empty dict on parse errors.
    """
    soup = BeautifulSoup(html, "lxml")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return {}
    try:
        data = json.loads(script.string)
    except json.JSONDecodeError:
        logger.debug("Could not parse __NEXT_DATA__ JSON.")
        return {}

    raw: dict[str, dict] = {}
    _scan_for_coordinates(data, raw)
    return {fid: (v["latitude"], v["longitude"]) for fid, v in raw.items()}


# ---------------------------------------------------------------------------
# Card assembler
# ---------------------------------------------------------------------------

def _parse_card(
    card: BeautifulSoup,
    coordinates_map: dict[str, tuple[float, float]],
    search_area: str,
    checkin_date: str,
    checkout_date: str,
    scraped_at: str,
    num_adults: int = 2,
) -> AirbnbAccommodationRecord:
    """Assemble one :class:`AirbnbAccommodationRecord` from a listing card."""
    facility_id, url = _extract_facility_id_and_url(card)
    name              = _extract_name(card)
    subtitle_lines    = _extract_subtitle_lines(card)

    description = subtitle_lines[0] if subtitle_lines else None
    acc_type, neighbourhood = _parse_accommodation_and_neighbourhood(
        subtitle_lines[0]
    ) if subtitle_lines else (None, None)

    num_bedrooms, num_beds = _extract_bedrooms_beds(subtitle_lines)
    price, currency, price_is_per_night = _extract_price(card)
    rating, num_reviews     = _extract_rating_and_reviews(card)
    is_superhost            = _extract_is_superhost(card)
    is_free_cancellation    = _extract_is_free_cancellation(card)
    host_type               = _extract_host_type(card)
    tags                    = _extract_tags(card, is_superhost, is_free_cancellation)
    is_available            = _extract_is_available(card, price)

    lat, lon = coordinates_map.get(facility_id, (None, None))

    return AirbnbAccommodationRecord(
        facility_id        = facility_id,
        name               = name,
        url                = url,
        search_area        = search_area,
        checkin_date       = checkin_date,
        checkout_date      = checkout_date,
        scraped_at         = scraped_at,
        num_adults         = num_adults,
        description        = description,
        accommodation_type = acc_type,
        neighbourhood      = neighbourhood,
        num_bedrooms       = num_bedrooms,
        num_beds           = num_beds,
        host_type          = host_type,
        total_price        = price,
        currency           = currency,
        price_is_per_night = price_is_per_night,
        rating             = rating,
        num_reviews        = num_reviews,
        is_superhost       = is_superhost,
        is_free_cancellation = is_free_cancellation,
        tags               = tags,
        latitude           = lat,
        longitude          = lon,
        is_available       = is_available,
        raw_html_snippet   = str(card)[:8192],
    )


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class AirbnbScraper:
    """Scrapes accommodation listings from AirBnB.

    Parameters
    ----------
    search_area:
        City or region name (e.g. ``"Tirana, Albania"``).
    checkin_date:
        ISO-8601 check-in date.
    checkout_date:
        ISO-8601 check-out date.
    num_adults:
        Number of adult guests (default 2).
    max_pages:
        Maximum result pages to scrape.
    request_delay_s:
        Polite delay between page requests (seconds).
    """

    def __init__(
        self,
        search_area: str,
        checkin_date: str,
        checkout_date: str,
        num_adults: int = 2,
        max_pages: int = 5,
        request_delay_s: float = 2.0,
    ) -> None:
        self.search_area      = search_area
        self.checkin_date     = checkin_date
        self.checkout_date    = checkout_date
        self.num_adults       = num_adults
        self.max_pages        = max_pages
        self.request_delay_s  = request_delay_s

        ua = UserAgent()
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=True)
        self._context = self._browser.new_context(
            user_agent=ua.random,
            locale="en-US",
            extra_http_headers=_DEFAULT_HEADERS,
        )
        self._page = self._context.new_page()
        self._warm_up()

    def _warm_up(self) -> None:
        """Load the AirBnB homepage to acquire session cookies."""
        logger.debug("Warming up browser session via homepage: %s", _HOMEPAGE_URL)
        try:
            self._page.goto(_HOMEPAGE_URL, wait_until="domcontentloaded", timeout=30_000)
            logger.debug("Homepage warm-up OK.")
        except PlaywrightTimeout:
            logger.warning("Homepage warm-up timed out (non-fatal).")
        finally:
            time.sleep(self.request_delay_s)

    def __del__(self) -> None:
        try:
            self._browser.close()
            self._pw.stop()
        except Exception:  # noqa: BLE001
            pass

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # Selectors tried (in order) to detect that AirBnB has rendered cards.
    _CARD_SELECTORS: list[str] = [
        "[data-testid='card-container']",
        "[itemprop='itemListElement']",
        "a[href*='/rooms/']",
    ]

    # ------------------------------------------------------------------

    def _build_url(self, page: int = 1) -> str:
        """Build the AirBnB search URL for the given page number (1-based)."""
        encoded = quote(self.search_area, safe="")
        base = _SEARCH_URL.format(location=encoded)
        return (
            f"{base}"
            f"?checkin={self.checkin_date}"
            f"&checkout={self.checkout_date}"
            f"&adults={self.num_adults}"
            f"&page={page}"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_page(self, url: str) -> str:
        logger.debug("Navigating to %s", url)
        # Phase 1: navigate — domcontentloaded is fast and reliable; we wait
        # for actual card content separately in phase 2.
        try:
            response = self._page.goto(url, wait_until="domcontentloaded", timeout=60_000)
        except PlaywrightTimeout:
            logger.warning("domcontentloaded timed out for %s", url)
            response = None

        status = response.status if response else 0
        logger.debug("Response: status=%d, url=%s", status, self._page.url)

        # Phase 2: wait for listing cards to appear (up to 30 s).
        # AirBnB renders cards via React after the initial HTML is delivered,
        # so we must wait for at least one card selector before reading content.
        found_selector = False
        for sel in self._CARD_SELECTORS:
            try:
                self._page.wait_for_selector(sel, timeout=30_000)
                logger.debug("Listing cards detected via selector: %s", sel)
                found_selector = True
                break
            except PlaywrightTimeout:
                logger.debug("Selector not found within timeout: %s", sel)

        if not found_selector:
            logger.warning(
                "No listing cards appeared after waiting. "
                "AirBnB may be blocking the request or the page structure changed."
            )
        else:
            # Give React a moment to finish rendering the full card list
            # (the first /rooms/ link appears before all ~20 cards are in the DOM).
            self._page.wait_for_timeout(3_000)

        return self._page.content()

    @staticmethod
    def _extract_cards(html: str) -> list[BeautifulSoup]:
        """Return all listing card elements from a rendered page."""
        soup = BeautifulSoup(html, "lxml")
        # Strategy 1: preferred test-id on article or div
        for tag in ("article", "div"):
            cards = soup.find_all(tag, {"data-testid": "card-container"})
            if cards:
                return cards
        # Strategy 2: itemprop schema
        cards = soup.find_all(attrs={"itemprop": "itemListElement"})
        if cards:
            return cards
        # Strategy 3: any element that directly wraps an /rooms/ anchor
        containers: list[BeautifulSoup] = []
        seen_ids: set[str] = set()
        for a in soup.find_all("a", href=re.compile(r"/rooms/\d+")):
            parent = a.parent
            # Walk up to find a reasonable card container (not the full page)
            for _ in range(5):
                if parent and parent.name in ("article", "section", "div", "li"):
                    fid_m = re.search(r"/rooms/(\d+)", a.get("href", ""))
                    fid   = fid_m.group(1) if fid_m else None
                    if fid and fid not in seen_ids:
                        containers.append(parent)
                        seen_ids.add(fid)
                    break
                parent = parent.parent if parent else None
        return containers

    def _get_next_page_url(self) -> str | None:
        """Return the absolute URL from the 'Next' pagination anchor, or None.

        AirBnB uses cursor-based pagination.  Constructing ``?page=N`` URLs
        returns the same listings on every page.  The reliable approach is to
        read the ``href`` attribute of the actual 'Next' anchor, which already
        contains the correct cursor token.
        """
        _NEXT_ANCHOR_SELECTORS = [
            "a[aria-label='Next']",
            "nav a[aria-label*='next' i]",
        ]
        # Scroll to bottom so the pagination bar is rendered.
        try:
            self._page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            self._page.wait_for_timeout(500)
        except Exception:  # noqa: BLE001
            pass

        for sel in _NEXT_ANCHOR_SELECTORS:
            try:
                el = self._page.query_selector(sel)
                if el and el.is_visible():
                    href = el.get_attribute("href")
                    if href:
                        if href.startswith("/"):
                            href = "https://www.airbnb.com" + href
                        logger.debug("Next-page URL found: %s", href[:120])
                        return href
            except Exception:  # noqa: BLE001
                pass
        return None

    @staticmethod
    def _log_diagnostics(html: str) -> None:
        soup = BeautifulSoup(html, "lxml")
        title_el = soup.find("title")
        title = title_el.get_text(strip=True) if title_el else "(no title)"
        logger.debug("Page title: %s", title)
        logger.debug("Page text snippet: %s", soup.get_text(separator=" ", strip=True)[:1000])

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, scraped_at: str) -> list[AirbnbAccommodationRecord]:
        """Scrape all configured pages and return the full list of records.

        Parameters
        ----------
        scraped_at:
            ISO-8601 datetime string stamped on every record.
        """
        records:   list[AirbnbAccommodationRecord] = []
        seen_keys: set[str] = set()

        for page_num in range(1, self.max_pages + 1):
            if page_num == 1:
                url = self._build_url(1)
                logger.info(
                    "Scraping page %d/%d for '%s' — %s",
                    page_num, self.max_pages, self.search_area, url,
                )
                try:
                    html = self._fetch_page(url)
                except (requests.RequestException, PlaywrightTimeout) as exc:
                    logger.error("Failed to fetch page 1: %s", exc)
                    break
            else:
                time.sleep(self.request_delay_s)
                next_url = self._get_next_page_url()
                if not next_url:
                    logger.info("No next-page URL after page %d — stopping.", page_num - 1)
                    break
                logger.info(
                    "Scraping page %d/%d for '%s'",
                    page_num, self.max_pages, self.search_area,
                )
                try:
                    html = self._fetch_page(next_url)
                except (requests.RequestException, PlaywrightTimeout) as exc:
                    logger.error("Failed to fetch page %d: %s", page_num, exc)
                    break

            # Extract per-page coordinate map from __NEXT_DATA__ JSON
            coords_map = _extract_coordinates_from_page(html)
            logger.debug("Coordinates found on page %d: %d entries", page_num, len(coords_map))

            cards = self._extract_cards(html)
            if not cards:
                logger.info("No listing cards found on page %d — stopping.", page_num)
                self._log_diagnostics(html)
                break

            new_records: list[AirbnbAccommodationRecord] = []
            for card in cards:
                try:
                    record = _parse_card(
                        card, coords_map,
                        self.search_area,
                        self.checkin_date,
                        self.checkout_date,
                        scraped_at,
                        self.num_adults,
                    )
                    new_records.append(record)
                except (AttributeError, ValueError, KeyError, TypeError) as exc:
                    logger.warning("Error parsing card: %s", exc)

            # Dedup key: facility_id when known, else name
            def _key(r: AirbnbAccommodationRecord) -> str:
                return r.facility_id if r.facility_id != "unknown" else r.name

            page_keys = {_key(r) for r in new_records}
            if page_keys and page_keys.issubset(seen_keys):
                logger.info("Page %d duplicates a previous page — stopping.", page_num)
                break

            for r in new_records:
                k = _key(r)
                if k not in seen_keys:
                    records.append(r)
                    seen_keys.add(k)

            logger.info(
                "Page %d: %d new records (total so far: %d)",
                page_num, len(new_records), len(records),
            )

            # Fewer than 8 cards on the page strongly suggests we've hit the last page.
            if len(cards) < 8:
                logger.info("Fewer than 8 cards on page %d — likely last page.", page_num)
                break

        logger.info("Scrape complete: %d total records for '%s'.", len(records), self.search_area)
        return records

    def scrape_as_dicts(self, scraped_at: str) -> list[dict]:
        """Return :meth:`scrape` results serialised as plain dicts."""
        return [r.as_dict() for r in self.scrape(scraped_at)]
