"""AirBnB facility scraper — extracts detailed facility/property info for individual listings.

For each AirBnB listing URL the scraper:
1. Loads the detail page via Playwright (headless Chromium).
2. Extracts the ``__NEXT_DATA__`` JSON blob embedded in the page.
3. Parses structured facility info (name, type, capacity, amenities, ratings, host
   info, policies, location) from well-known section IDs inside that blob.

This scraper produces the ``airbnb_facilities`` dataset (one row per listing per
scrape day) and is intentionally **independent** from the accommodation-search
scraper and dataset.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import asdict, dataclass, field
from typing import Any

from fake_useragent import UserAgent
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NEXT_DATA_SELECTOR = "script#__NEXT_DATA__"
_PAGE_TIMEOUT_MS    = 60_000
_AIRBNB_HOME_URL    = "https://www.airbnb.com/"

# AirBnB section IDs (Niobe framework, 2025+)
# OVERVIEW_DEFAULT was replaced by AVAILABILITY_CALENDAR_DEFAULT in the Niobe format.
_SEC_OVERVIEW     = "AVAILABILITY_CALENDAR_DEFAULT"
_SEC_DESCRIPTION  = "DESCRIPTION_DEFAULT"
_SEC_HIGHLIGHTS   = "HIGHLIGHTS_DEFAULT"
_SEC_REVIEWS      = "REVIEWS_DEFAULT"
_SEC_HOST         = "MEET_YOUR_HOST"          # was HOST_PROFILE_DEFAULT
_SEC_LOCATION     = "LOCATION_DEFAULT"
_SEC_POLICIES     = "POLICIES_DEFAULT"
_SEC_AMENITIES    = "AMENITIES_DEFAULT"

# Amenity keyword mapping: field_name → list of keywords to search in title text
_AMENITY_KEYWORDS: dict[str, list[str]] = {
    "amenity_wifi":               ["wifi", "wi-fi", "wireless internet"],
    "amenity_kitchen":            ["kitchen"],
    "amenity_washing_machine":    ["washing machine", "washer"],
    "amenity_dryer":              ["dryer"],
    "amenity_free_parking":       ["free parking", "free dedicated parking", "free carport"],
    "amenity_air_conditioning":   ["air conditioning", "air-conditioning", "ac"],
    "amenity_heating":            ["heating", "central heating"],
    "amenity_tv":                 ["tv", "television", "hdtv"],
    "amenity_dedicated_workspace":["dedicated workspace", "work desk"],
    "amenity_pool":               ["pool", "swimming pool"],
    "amenity_hot_tub":            ["hot tub", "jacuzzi", "bathtub"],
    "amenity_gym":                ["gym", "fitness", "exercise equipment"],
    "amenity_bbq_grill":          ["bbq", "grill", "barbecue"],
    "amenity_breakfast":          ["breakfast"],
    "amenity_pets_allowed":       ["pets allowed", "pet-friendly"],
    "amenity_smoking_allowed":    ["smoking allowed"],
}


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class AirbnbFacilityRecord:
    """One facility record — one AirBnB listing at one point in time."""

    # Identifiers
    facility_id: str
    url: str
    scraped_at: str                  # ISO-8601 datetime

    # Basic info
    name: str | None
    accommodation_type: str | None   # e.g. "Entire apartment"
    description: str | None

    # Capacity
    num_guests: int | None
    num_bedrooms: int | None
    num_beds: int | None
    num_bathrooms: float | None

    # Quality label
    label: str | None                # e.g. "Guest favourite"
    is_guest_favourite: bool

    # Ratings
    rating: float | None             # 0-5
    num_reviews: int | None
    rating_cleanliness: float | None
    rating_accuracy: float | None
    rating_checkin: float | None
    rating_communication: float | None
    rating_location: float | None
    abnb_price_quality_score: float | None  # AirBnB "value" sub-rating

    # Host
    host_name: str | None
    host_id: str | None
    is_superhost: bool
    host_since_year: int | None
    host_response_rate: str | None
    host_response_time: str | None

    # Location
    latitude: float | None
    longitude: float | None

    # Policies
    min_nights: int | None
    max_nights: int | None
    check_in_time: str | None
    check_out_time: str | None
    cancellation_policy: str | None
    house_rules: str | None

    # Amenities (boolean flags)
    amenity_wifi: bool
    amenity_kitchen: bool
    amenity_washing_machine: bool
    amenity_dryer: bool
    amenity_free_parking: bool
    amenity_air_conditioning: bool
    amenity_heating: bool
    amenity_tv: bool
    amenity_dedicated_workspace: bool
    amenity_pool: bool
    amenity_hot_tub: bool
    amenity_gym: bool
    amenity_bbq_grill: bool
    amenity_breakfast: bool
    amenity_pets_allowed: bool
    amenity_smoking_allowed: bool

    # Raw amenity text
    amenities_raw: str | None        # pipe-separated amenity titles

    # Raw JSON (first 8 KB for debugging)
    raw_json_snippet: str = field(default="", repr=False)

    def as_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Page-data helpers (Niobe + legacy __NEXT_DATA__ fallback)
# ---------------------------------------------------------------------------

def _get_niobe_sections(html: str) -> dict[str, dict]:
    """Return ``{sectionId: section_dict}`` from the page HTML.

    Tries the Niobe ``data-deferred-state-0`` script first (AirBnB 2025+),
    then falls back to the legacy ``__NEXT_DATA__`` Next.js blob.
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "lxml")

    # Primary path: Niobe
    tag = soup.find("script", id="data-deferred-state-0")
    if tag and tag.string:
        try:
            raw       = json.loads(tag.string)
            niobe_map = dict(raw.get("niobeClientData", []))
            pdp_key   = next((k for k in niobe_map if k.startswith("StaysPdpSections")), None)
            if pdp_key:
                sections_list = (
                    niobe_map[pdp_key]
                    ["data"]["presentation"]["stayProductDetailPage"]["sections"]["sections"]
                )
                return {s["sectionId"]: s.get("section") or {} for s in sections_list}
        except (KeyError, TypeError, json.JSONDecodeError):
            pass

    # Fallback: legacy __NEXT_DATA__
    tag = soup.find("script", id="__NEXT_DATA__")
    if tag and tag.string:
        try:
            data = json.loads(tag.string)
            raw_list = _all_sections_legacy(data)
            return {s["sectionId"]: s.get("sectionData", s)
                    for s in raw_list if isinstance(s, dict) and s.get("sectionId")}
        except (json.JSONDecodeError, AttributeError):
            pass

    return {}


def _all_sections_legacy(data: dict) -> list:
    """Walk a legacy __NEXT_DATA__ blob and collect section objects."""
    try:
        sections = data["props"]["pageProps"]["sections"]
        if isinstance(sections, list):
            return sections
    except (KeyError, TypeError):
        pass
    try:
        pdp = data["props"]["pageProps"]["pdpSections"]
        return [item.get("sectionData", item) for item in pdp.get("sections", [])]
    except (KeyError, TypeError):
        pass
    return []


def _deep_find(obj: Any, key: str) -> Any:
    """Recursively search for the first occurrence of *key* in nested dicts/lists."""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            result = _deep_find(v, key)
            if result is not None:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find(item, key)
            if result is not None:
                return result
    return None


# ---------------------------------------------------------------------------
# Field-level extractors
# ---------------------------------------------------------------------------

def _extract_overview(sec: dict) -> dict:
    out: dict[str, Any] = {
        "name": None,
        "accommodation_type": None,
        "num_guests": None,
        "num_bedrooms": None,
        "num_beds": None,
        "num_bathrooms": None,
    }
    if not sec:
        return out

    # Niobe: AVAILABILITY_CALENDAR_DEFAULT has listingTitle, maxGuestCapacity,
    # and descriptionItems like ["Entire condo", "3 beds", "1 bath"].
    if "listingTitle" in sec:
        out["name"]       = sec.get("listingTitle")
        out["num_guests"] = sec.get("maxGuestCapacity")
        for item in sec.get("descriptionItems") or []:
            t = (item.get("title") or "").strip() if isinstance(item, dict) else str(item)
            t_lower = t.lower()
            nums = re.findall(r"[\d.]+", t)
            n    = float(nums[0]) if nums else None
            if "bedroom" in t_lower:
                out["num_bedrooms"] = int(n) if n is not None else None
            elif "bed" in t_lower:
                out["num_beds"] = int(n) if n is not None else None
            elif "bath" in t_lower:
                out["num_bathrooms"] = n
            elif not out["accommodation_type"] and t:
                out["accommodation_type"] = t
        return out

    # Legacy / deep-search fallback
    for key in ("name", "title"):
        if key in sec and sec[key]:
            out["name"] = str(sec[key])
            break
    for key in ("roomTypeCategory", "roomType", "subtitle"):
        val = _deep_find(sec, key)
        if val and isinstance(val, str) and len(val) < 80:
            out["accommodation_type"] = val
            break
    capacity_source = _deep_find(sec, "overviewItems") or _deep_find(sec, "detailItems") or []
    for item in capacity_source:
        if not isinstance(item, dict):
            continue
        title   = str(item.get("title", "")).lower()
        val_str = str(item.get("value", item.get("title", ""))).lower()
        digits  = re.findall(r"\d+\.?\d*", val_str or title)
        num     = float(digits[0]) if digits else None
        if any(k in title for k in ("guest", "person", "people")):
            out["num_guests"] = int(num) if num is not None else None
        elif "bedroom" in title:
            out["num_bedrooms"] = int(num) if num is not None else None
        elif "bed" in title:
            out["num_beds"] = int(num) if num is not None else None
        elif "bath" in title:
            out["num_bathrooms"] = num
    return out


def _extract_description(sec: dict) -> str | None:
    if not sec:
        return None
    # Niobe: DESCRIPTION_DEFAULT has htmlDescription.htmlText
    html_desc = sec.get("htmlDescription")
    if isinstance(html_desc, dict) and html_desc.get("htmlText"):
        cleaned = re.sub(r"<br\s*/?>", "\n", html_desc["htmlText"])
        return re.sub(r"<[^>]+>", " ", cleaned).strip()
    # Legacy / fallback
    for key in ("htmlDescription", "description", "summary"):
        val = _deep_find(sec, key)
        if val and isinstance(val, str):
            return re.sub(r"<[^>]+>", " ", val).strip()
    return None


def _extract_highlights(sec: dict) -> tuple[str | None, bool]:
    """Return (label_text, is_guest_favourite)."""
    if not sec:
        return None, False
    # Niobe: HIGHLIGHTS_DEFAULT has highlights list with type and title
    highlights = sec.get("highlights")
    if isinstance(highlights, list) and highlights:
        for h in highlights:
            h_type = h.get("type", "")
            if "GUEST_FAVORITE" in h_type or "GUEST_FAVOURITE" in h_type:
                return h.get("title"), True
        return highlights[0].get("title"), False
    # Legacy: headline or title
    for key in ("headline", "title", "label"):
        val = _deep_find(sec, key)
        if val and isinstance(val, str) and len(val) < 120:
            is_gf = any(
                phrase in val.lower()
                for phrase in ("guest favourite", "guest favorite", "amato dagli ospiti")
            )
            return val, is_gf
    return None, False


def _extract_reviews(sec: dict) -> dict:
    out: dict[str, Any] = {
        "rating": None,
        "num_reviews": None,
        "is_guest_favourite": False,
        "rating_cleanliness": None,
        "rating_accuracy": None,
        "rating_checkin": None,
        "rating_communication": None,
        "rating_location": None,
        "abnb_price_quality_score": None,
    }
    if not sec:
        return out

    # Niobe: REVIEWS_DEFAULT has overallRating, overallCount, isGuestFavorite,
    # and ratings list of {categoryType, localizedRating}.
    if "overallRating" in sec:
        try:
            out["rating"] = float(sec["overallRating"])
        except (TypeError, ValueError):
            pass
        try:
            out["num_reviews"] = int(sec.get("overallCount", 0)) or None
        except (TypeError, ValueError):
            pass
        out["is_guest_favourite"] = bool(sec.get("isGuestFavorite"))
        _cat_map = {
            "CLEANLINESS":   "rating_cleanliness",
            "ACCURACY":      "rating_accuracy",
            "CHECKIN":       "rating_checkin",
            "COMMUNICATION": "rating_communication",
            "LOCATION":      "rating_location",
            "VALUE":         "abnb_price_quality_score",
        }
        for rating in sec.get("ratings") or []:
            cat   = rating.get("categoryType", "")
            field = _cat_map.get(cat)
            if field:
                try:
                    out[field] = float(rating.get("localizedRating", 0))
                except (TypeError, ValueError):
                    pass
        return out

    # Legacy fallback
    for key in ("overallRating", "avgRating", "rating"):
        val = _deep_find(sec, key)
        if val is not None:
            try:
                out["rating"] = float(val)
            except (TypeError, ValueError):
                pass
            break
    for key in ("numberOfReviews", "reviewsCount", "numReviews", "reviewCount"):
        val = _deep_find(sec, key)
        if val is not None:
            try:
                out["num_reviews"] = int(val)
            except (TypeError, ValueError):
                pass
            break
    sub_ratings = _deep_find(sec, "reviewCategories") or _deep_find(sec, "ratingBreakdown") or []
    _sub_key_map = {
        "cleanliness": "rating_cleanliness",
        "accuracy":    "rating_accuracy",
        "check-in":    "rating_checkin",
        "checkin":     "rating_checkin",
        "communication": "rating_communication",
        "location":    "rating_location",
        "value":       "abnb_price_quality_score",
    }
    for item in sub_ratings:
        if not isinstance(item, dict):
            continue
        cat   = str(item.get("category", item.get("title", ""))).lower()
        r_val = item.get("rating", item.get("value"))
        if r_val is None:
            continue
        try:
            r_float = float(r_val)
        except (TypeError, ValueError):
            continue
        for substr, field_name in _sub_key_map.items():
            if substr in cat:
                out[field_name] = r_float
                break
    return out


def _extract_host(sec: dict) -> dict:
    out: dict[str, Any] = {
        "host_name": None,
        "host_id": None,
        "is_superhost": False,
        "host_since_year": None,
        "host_response_rate": None,
        "host_response_time": None,
    }
    if not sec:
        return out

    # Niobe: MEET_YOUR_HOST has cardData with name, userId, isSuperhost, timeAsHost, stats
    card = sec.get("cardData") or {}
    if card:
        out["host_name"]    = card.get("name")
        out["host_id"]      = card.get("userId")
        out["is_superhost"] = bool(card.get("isSuperhost"))
        time_as_host = card.get("timeAsHost") or {}
        years = time_as_host.get("years")
        if years is not None:
            from datetime import datetime as _dt
            out["host_since_year"] = _dt.now().year - int(years)
        for stat in card.get("stats") or []:
            stat_type = stat.get("type", "")
            if stat_type == "RESPONSE_RATE":
                out["host_response_rate"] = str(stat.get("value", ""))
            elif stat_type == "RESPONSE_TIME":
                out["host_response_time"] = str(stat.get("value", ""))
        if out["host_name"]:
            return out

    # Legacy fallback
    for key in ("hostName", "name"):
        val = _deep_find(sec, key)
        if val and isinstance(val, str) and len(val) < 120:
            out["host_name"] = val
            break
    for key in ("hostId", "userId", "id"):
        val = _deep_find(sec, key)
        if val is not None:
            out["host_id"] = str(val)
            break
    for key in ("isSuperhost", "superhost"):
        val = _deep_find(sec, key)
        if val is not None:
            out["is_superhost"] = bool(val)
            break
    for key in ("memberSince", "hostSince", "yearHosting"):
        val = _deep_find(sec, key)
        if val:
            m = re.search(r"\b(20\d{2}|19\d{2})\b", str(val))
            if m:
                out["host_since_year"] = int(m.group())
                break
    for key in ("responseRate", "hostResponseRate"):
        val = _deep_find(sec, key)
        if val is not None:
            out["host_response_rate"] = str(val)
            break
    for key in ("responseTime", "hostResponseTime"):
        val = _deep_find(sec, key)
        if val is not None:
            out["host_response_time"] = str(val)
            break
    return out


def _extract_location(sec: dict) -> tuple[float | None, float | None]:
    """Return (latitude, longitude) from LOCATION_DEFAULT section."""
    if not sec:
        return None, None
    # Niobe: LOCATION_DEFAULT has direct lat / lng fields
    lat = sec.get("lat") or _deep_find(sec, "lat") or _deep_find(sec, "latitude")
    lng = sec.get("lng") or _deep_find(sec, "lng") or _deep_find(sec, "longitude")
    if lat is not None and lng is not None:
        try:
            return float(lat), float(lng)
        except (TypeError, ValueError):
            pass
    return None, None


def _extract_policies(sec: dict) -> dict:
    out: dict[str, Any] = {
        "min_nights": None,
        "max_nights": None,
        "check_in_time": None,
        "check_out_time": None,
        "cancellation_policy": None,
        "house_rules": None,
    }
    if not sec:
        return out

    # Niobe: POLICIES_DEFAULT has houseRules list.
    # Check-in/out times and max guests are embedded as rule titles.
    if "houseRules" in sec:
        rules = sec.get("houseRules") or []
        rule_texts = [r.get("title", "") for r in rules if isinstance(r, dict) and r.get("title")]
        for text in rule_texts:
            t_lower = text.lower()
            if "check-in" in t_lower or "check in" in t_lower:
                m = re.search(r"(?:after|from)\s+(.+)", text, re.IGNORECASE)
                if m:
                    out["check_in_time"] = m.group(1).strip()
            elif "checkout" in t_lower or "check-out" in t_lower or "check out" in t_lower:
                m = re.search(r"(?:before|until)\s+(.+)", text, re.IGNORECASE)
                if m:
                    out["check_out_time"] = m.group(1).strip()
        if rule_texts:
            out["house_rules"] = " | ".join(rule_texts)
        cancel = sec.get("cancellationPolicyForDisplay")
        if cancel and isinstance(cancel, str):
            out["cancellation_policy"] = cancel
        return out

    # Legacy deep-search fallback
    for key in ("minNights", "minNight"):
        val = _deep_find(sec, key)
        if val is not None:
            try:
                out["min_nights"] = int(val)
            except (TypeError, ValueError):
                pass
            break
    for key in ("maxNights", "maxNight"):
        val = _deep_find(sec, key)
        if val is not None:
            try:
                out["max_nights"] = int(val)
            except (TypeError, ValueError):
                pass
            break
    for key in ("checkInTime", "checkinTime"):
        val = _deep_find(sec, key)
        if val is not None:
            out["check_in_time"] = str(val)
            break
    for key in ("checkOutTime", "checkoutTime"):
        val = _deep_find(sec, key)
        if val is not None:
            out["check_out_time"] = str(val)
            break
    for key in ("cancellationPolicyName", "cancellationPolicy", "cancellationType"):
        val = _deep_find(sec, key)
        if val and isinstance(val, str):
            out["cancellation_policy"] = val
            break
    rules = _deep_find(sec, "houseRules") or _deep_find(sec, "additionalRules") or []
    if isinstance(rules, list):
        rule_texts = [r.get("title", r) if isinstance(r, dict) else str(r) for r in rules if r]
        if rule_texts:
            out["house_rules"] = " | ".join(str(r) for r in rule_texts if r)
    elif isinstance(rules, str):
        out["house_rules"] = rules
    return out


def _extract_amenities(sec: dict) -> dict:
    """Return boolean amenity flags and raw pipe-separated list."""
    out: dict[str, Any] = {k: False for k in _AMENITY_KEYWORDS}
    out["amenities_raw"] = None

    if not sec:
        return out

    # Collect all amenity title strings
    raw_titles: list[str] = []
    amenity_groups = (
        _deep_find(sec, "seeAllAmenitiesGroups")
        or _deep_find(sec, "amenityGroups")
        or _deep_find(sec, "amenities")
        or []
    )
    if isinstance(amenity_groups, list):
        for group in amenity_groups:
            if not isinstance(group, dict):
                continue
            items = group.get("amenities", group.get("items", []))
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, dict):
                        title = item.get("title", item.get("name", ""))
                    else:
                        title = str(item)
                    if title:
                        raw_titles.append(title)
    elif isinstance(amenity_groups, dict):
        # Sometimes it's already a flat dict keyed by amenity name
        raw_titles.extend(amenity_groups.keys())

    # Also look for flat list at top level
    flat = _deep_find(sec, "amenityListItems") or []
    if isinstance(flat, list):
        for item in flat:
            t = item.get("title", "") if isinstance(item, dict) else str(item)
            if t:
                raw_titles.append(t)

    if raw_titles:
        out["amenities_raw"] = " | ".join(raw_titles)
        lower_titles = [t.lower() for t in raw_titles]
        for field_name, keywords in _AMENITY_KEYWORDS.items():
            out[field_name] = any(
                any(kw in title for kw in keywords)
                for title in lower_titles
            )

    return out


# ---------------------------------------------------------------------------
# Main scraper class
# ---------------------------------------------------------------------------

class AirbnbFacilityScraper:
    """Scrapes detailed facility info from AirBnB listing detail pages.

    Parameters
    ----------
    request_delay_s:
        Polite delay (seconds) between page loads.
    headless:
        Run Playwright browser in headless mode (default ``True``).
    """

    def __init__(
        self,
        request_delay_s: float = 3.0,
        headless: bool = True,
    ) -> None:
        self.request_delay_s = request_delay_s
        self.headless        = headless
        self._pw             = None
        self._browser        = None
        self._page           = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self) -> "AirbnbFacilityScraper":
        ua            = UserAgent()
        self._pw      = sync_playwright().start()
        self._browser = self._pw.chromium.launch(headless=self.headless)
        ctx           = self._browser.new_context(
            user_agent         = ua.random,
            locale             = "en-US",
            extra_http_headers = {"Accept-Language": "en-US,en;q=0.9"},
        )
        self._page = ctx.new_page()
        # Block resources the scraper never uses (images, fonts, CSS, media).
        # AirBnB listing pages are asset-heavy; skipping them cuts load time by ~60%.
        _BLOCKED_TYPES = {"image", "media", "font", "stylesheet"}
        self._page.route(
            "**/*",
            lambda route, req: (
                route.abort() if req.resource_type in _BLOCKED_TYPES else route.continue_()
            ),
        )
        self._warm_up_session()
        logger.info("AirbnbFacilityScraper browser started (headless=%s).", self.headless)
        return self

    def __exit__(self, *_: object) -> None:
        if self._browser:
            self._browser.close()
        if self._pw:
            self._pw.stop()
        logger.info("AirbnbFacilityScraper browser closed.")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scrape(self, facility_id: str, url: str, scraped_at: str) -> AirbnbFacilityRecord:
        """Scrape one listing and return an :class:`AirbnbFacilityRecord`.

        Parameters
        ----------
        facility_id:
            AirBnB listing ID (used as partition key in storage).
        url:
            Full AirBnB listing URL (``https://www.airbnb.com/rooms/<id>``).
        scraped_at:
            ISO-8601 datetime string (shared across a pipeline run).
        """
        html = self._fetch_page(url)
        return self._parse(facility_id, url, scraped_at, html)

    def scrape_as_dicts(
        self,
        listings: list[tuple[str, str]],
        scraped_at: str,
    ) -> list[dict]:
        """Scrape multiple listings and return a list of dicts.

        Parameters
        ----------
        listings:
            List of ``(facility_id, url)`` tuples.
        scraped_at:
            ISO-8601 datetime string.
        """
        results: list[dict] = []
        for i, (fid, url) in enumerate(listings):
            try:
                record = self.scrape(fid, url, scraped_at)
                results.append(record.as_dict())
                logger.info("[%d/%d] Scraped facility %s", i + 1, len(listings), fid)
            except Exception as exc:
                logger.error("Failed to scrape facility %s (%s): %s", fid, url, exc)

            if i < len(listings) - 1:
                time.sleep(self.request_delay_s)

        return results

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        reraise=True,
    )
    def _fetch_page(self, url: str) -> str:
        """Fetch *url* and return the raw server-rendered HTML.

        Uses response interception: returns as soon as the HTTP response body
        is received, without waiting for any JS execution.  The Niobe
        ``data-deferred-state-0`` blob is server-side rendered, so the raw
        response body is all we need.
        """
        assert self._page is not None, "Use AirbnbFacilityScraper as a context manager."
        try:
            with self._page.expect_response(
                lambda r: r.request.resource_type == "document" and r.status == 200,
                timeout=_PAGE_TIMEOUT_MS,
            ) as resp_info:
                self._page.goto(url, wait_until="commit", timeout=_PAGE_TIMEOUT_MS)
            return resp_info.value.text()
        except PlaywrightTimeout as exc:
            logger.error("Page load timeout for %s: %s", url, exc)
            raise

    def _warm_up_session(self) -> None:
        """Visit the AirBnB homepage to acquire session cookies before scraping."""
        try:
            self._page.goto(_AIRBNB_HOME_URL, wait_until="domcontentloaded", timeout=_PAGE_TIMEOUT_MS)
            time.sleep(1)
            logger.debug("Session warmed up via %s.", _AIRBNB_HOME_URL)
        except PlaywrightTimeout:
            logger.warning("Timeout warming up session via homepage — listing pages may fail.")

    def _parse(
        self,
        facility_id: str,
        url: str,
        scraped_at: str,
        html: str,
    ) -> AirbnbFacilityRecord:
        """Parse HTML → AirbnbFacilityRecord via Niobe data-deferred-state-0 extraction."""
        sections = _get_niobe_sections(html)

        if not sections:
            logger.warning(
                "No page data found for facility %s — "
                "neither data-deferred-state-0 nor __NEXT_DATA__ was present.",
                facility_id,
            )
            return self._empty_record(facility_id, url, scraped_at)

        sec_overview    = sections.get(_SEC_OVERVIEW,     {})
        sec_description = sections.get(_SEC_DESCRIPTION,  {})
        sec_highlights  = sections.get(_SEC_HIGHLIGHTS,   {})
        sec_reviews     = sections.get(_SEC_REVIEWS,      {})
        sec_host        = sections.get(_SEC_HOST,         {})
        sec_location    = sections.get(_SEC_LOCATION,     {})
        sec_policies    = sections.get(_SEC_POLICIES,     {})
        sec_amenities   = sections.get(_SEC_AMENITIES,    {})

        overview   = _extract_overview(sec_overview)
        desc       = _extract_description(sec_description)
        label, gf  = _extract_highlights(sec_highlights)
        reviews    = _extract_reviews(sec_reviews)
        host       = _extract_host(sec_host)
        lat, lng   = _extract_location(sec_location)
        policies   = _extract_policies(sec_policies)
        amenities  = _extract_amenities(sec_amenities)

        # is_guest_favourite comes from REVIEWS_DEFAULT in Niobe
        is_gf = reviews.get("is_guest_favourite", False) or gf

        # Grab first 8 KB of raw data for debugging
        from bs4 import BeautifulSoup as _BS
        _tag = _BS(html, "lxml").find("script", id="data-deferred-state-0")
        raw_snippet = (_tag.string or "")[:8192] if _tag and _tag.string else html[:8192]

        return AirbnbFacilityRecord(
            facility_id             = facility_id,
            url                     = url,
            scraped_at              = scraped_at,
            name                    = overview["name"],
            accommodation_type      = overview["accommodation_type"],
            description             = desc,
            num_guests              = overview["num_guests"],
            num_bedrooms            = overview["num_bedrooms"],
            num_beds                = overview["num_beds"],
            num_bathrooms           = overview["num_bathrooms"],
            label                   = label,
            is_guest_favourite      = is_gf,
            rating                  = reviews["rating"],
            num_reviews             = reviews["num_reviews"],
            rating_cleanliness      = reviews["rating_cleanliness"],
            rating_accuracy         = reviews["rating_accuracy"],
            rating_checkin          = reviews["rating_checkin"],
            rating_communication    = reviews["rating_communication"],
            rating_location         = reviews["rating_location"],
            abnb_price_quality_score= reviews["abnb_price_quality_score"],
            host_name               = host["host_name"],
            host_id                 = host["host_id"],
            is_superhost            = host["is_superhost"],
            host_since_year         = host["host_since_year"],
            host_response_rate      = host["host_response_rate"],
            host_response_time      = host["host_response_time"],
            latitude                = lat,
            longitude               = lng,
            min_nights              = policies["min_nights"],
            max_nights              = policies["max_nights"],
            check_in_time           = policies["check_in_time"],
            check_out_time          = policies["check_out_time"],
            cancellation_policy     = policies["cancellation_policy"],
            house_rules             = policies["house_rules"],
            amenity_wifi            = amenities["amenity_wifi"],
            amenity_kitchen         = amenities["amenity_kitchen"],
            amenity_washing_machine = amenities["amenity_washing_machine"],
            amenity_dryer           = amenities["amenity_dryer"],
            amenity_free_parking    = amenities["amenity_free_parking"],
            amenity_air_conditioning= amenities["amenity_air_conditioning"],
            amenity_heating         = amenities["amenity_heating"],
            amenity_tv              = amenities["amenity_tv"],
            amenity_dedicated_workspace = amenities["amenity_dedicated_workspace"],
            amenity_pool            = amenities["amenity_pool"],
            amenity_hot_tub         = amenities["amenity_hot_tub"],
            amenity_gym             = amenities["amenity_gym"],
            amenity_bbq_grill       = amenities["amenity_bbq_grill"],
            amenity_breakfast       = amenities["amenity_breakfast"],
            amenity_pets_allowed    = amenities["amenity_pets_allowed"],
            amenity_smoking_allowed = amenities["amenity_smoking_allowed"],
            amenities_raw           = amenities["amenities_raw"],
            raw_json_snippet        = raw_snippet,
        )

    @staticmethod
    def _empty_record(
        facility_id: str,
        url: str,
        scraped_at: str,
    ) -> AirbnbFacilityRecord:
        """Return a record with all optional fields as None/False."""
        return AirbnbFacilityRecord(
            facility_id=facility_id, url=url, scraped_at=scraped_at,
            name=None, accommodation_type=None, description=None,
            num_guests=None, num_bedrooms=None, num_beds=None, num_bathrooms=None,
            label=None, is_guest_favourite=False,
            rating=None, num_reviews=None,
            rating_cleanliness=None, rating_accuracy=None, rating_checkin=None,
            rating_communication=None, rating_location=None, abnb_price_quality_score=None,
            host_name=None, host_id=None, is_superhost=False,
            host_since_year=None, host_response_rate=None, host_response_time=None,
            latitude=None, longitude=None,
            min_nights=None, max_nights=None,
            check_in_time=None, check_out_time=None,
            cancellation_policy=None, house_rules=None,
            amenity_wifi=False, amenity_kitchen=False, amenity_washing_machine=False,
            amenity_dryer=False, amenity_free_parking=False, amenity_air_conditioning=False,
            amenity_heating=False, amenity_tv=False, amenity_dedicated_workspace=False,
            amenity_pool=False, amenity_hot_tub=False, amenity_gym=False,
            amenity_bbq_grill=False, amenity_breakfast=False,
            amenity_pets_allowed=False, amenity_smoking_allowed=False,
            amenities_raw=None,
        )
