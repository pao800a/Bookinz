"""Tests for the AirBnB scraper — uses static HTML fixtures (no network calls)."""

from __future__ import annotations

import json
import textwrap

import pytest
from bs4 import BeautifulSoup

from bookinz.scraper.airbnb_scraper import (
    AirbnbScraper,
    _extract_coordinates_from_page,
    _parse_card,
    _safe_float,
    _safe_int,
)


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

class TestSafeFloat:
    def test_plain_number(self) -> None:
        assert _safe_float("176.50") == pytest.approx(176.50)

    def test_with_currency_symbol(self) -> None:
        assert _safe_float("€\xa01,234") == pytest.approx(1234.0)

    def test_none_input(self) -> None:
        assert _safe_float(None) is None

    def test_empty_string(self) -> None:
        assert _safe_float("") is None

    def test_european_format(self) -> None:
        assert _safe_float("1.234,56") == pytest.approx(1234.56)

    def test_comma_decimal(self) -> None:
        assert _safe_float("8,50") == pytest.approx(8.50)


class TestSafeInt:
    def test_plain_number(self) -> None:
        assert _safe_int("127 reviews") == 127

    def test_none_input(self) -> None:
        assert _safe_int(None) is None

    def test_no_digits(self) -> None:
        assert _safe_int("abc") is None


# ---------------------------------------------------------------------------
# HTML card fixture
# ---------------------------------------------------------------------------

AIRBNB_CARD_HTML = textwrap.dedent(
    """
    <div data-testid="card-container">
      <article>
        <a href="/rooms/67890?adults=2&checkin=2026-09-20&checkout=2026-09-27">
          <div data-testid="listing-card-title" id="title_67890">
            Cozy Trastevere Apartment
          </div>
        </a>
        <div data-testid="listing-card-subtitle">
          Entire apartment in Trastevere, Rome
        </div>
        <div data-testid="listing-card-subtitle">
          2 bedrooms &middot; 4 beds &middot; 1 bath
        </div>
        <span aria-label="4.86 out of 5 average rating, 127 reviews">4.86</span>
        <span aria-label="Superhost" data-testid="badge-superhost">Superhost</span>
        <span>Free cancellation</span>
        <div aria-label="$1,234 total" class="_price">
          $1,234
        </div>
      </article>
    </div>
    """
)

# __NEXT_DATA__ fixture for coordinate extraction tests
NEXT_DATA_JSON = json.dumps({
    "props": {
        "pageProps": {
            "niobeMinimalClientData": [
                [
                    "StaysPdpSections",
                    {
                        "data": {
                            "presentation": {
                                "stayProductDetailPage": {
                                    "sections": {
                                        "metadata": {
                                            "loggingContext": {
                                                "eventDataLogging": {
                                                    "listingId": "67890",
                                                    "listing": {
                                                        "id": "67890",
                                                        "latitude": 41.8892,
                                                        "longitude": 12.4711,
                                                    },
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                ]
            ]
        }
    }
})


@pytest.fixture
def card() -> BeautifulSoup:
    soup = BeautifulSoup(AIRBNB_CARD_HTML, "lxml")
    return soup.find("div", {"data-testid": "card-container"})


@pytest.fixture
def record(card):
    """Parsed record from the static card fixture (no coordinates)."""
    return _parse_card(
        card,
        coordinates_map={},
        search_area="Rome__Italy",
        checkin_date="2026-09-20",
        checkout_date="2026-09-27",
        scraped_at="2026-09-01T10:00:00",
        num_adults=2,
    )


@pytest.fixture
def record_with_coords(card):
    """Parsed record using coordinate map populated from NEXT_DATA_JSON."""
    coords = _extract_coordinates_from_page(
        f'<html><head><script id="__NEXT_DATA__">{NEXT_DATA_JSON}</script></head><body></body></html>'
    )
    return _parse_card(
        card,
        coordinates_map=coords,
        search_area="Rome__Italy",
        checkin_date="2026-09-20",
        checkout_date="2026-09-27",
        scraped_at="2026-09-01T10:00:00",
        num_adults=2,
    )


# ---------------------------------------------------------------------------
# Card-level parsing tests
# ---------------------------------------------------------------------------

class TestParseCard:
    def test_facility_id(self, record) -> None:
        assert record.facility_id == "67890"

    def test_name(self, record) -> None:
        assert "Trastevere" in record.name or "Cozy" in record.name

    def test_url(self, record) -> None:
        assert record.url.startswith("https://www.airbnb.com/rooms/67890")

    def test_price(self, record) -> None:
        assert record.total_price == pytest.approx(1234.0)

    def test_currency(self, record) -> None:
        assert record.currency == "USD"

    def test_rating(self, record) -> None:
        assert record.rating == pytest.approx(4.86)

    def test_num_reviews(self, record) -> None:
        assert record.num_reviews == 127

    def test_accommodation_type(self, record) -> None:
        assert record.accommodation_type is not None
        assert "apartment" in record.accommodation_type.lower()

    def test_neighbourhood(self, record) -> None:
        assert record.neighbourhood is not None
        assert "Trastevere" in record.neighbourhood

    def test_num_bedrooms(self, record) -> None:
        assert record.num_bedrooms == 2

    def test_num_beds(self, record) -> None:
        assert record.num_beds == 4

    def test_is_superhost_true(self, record) -> None:
        assert record.is_superhost is True

    def test_is_superhost_false(self) -> None:
        html = textwrap.dedent("""
            <div data-testid="card-container">
              <a href="/rooms/11111"><div data-testid="listing-card-title">Basic Room</div></a>
              <div data-testid="listing-card-subtitle">Private room in Testville, City</div>
              <div aria-label="$50 total">$50</div>
            </div>
        """)
        card = BeautifulSoup(html, "lxml").find("div", {"data-testid": "card-container"})
        r = _parse_card(card, {}, "City__Country", "2026-09-20", "2026-09-27", "2026-09-01T10:00:00", 2)
        assert r.is_superhost is False

    def test_is_free_cancellation(self, record) -> None:
        assert record.is_free_cancellation is True

    def test_tags(self, record) -> None:
        # Tags may be None or a string; we just check the type
        assert record.tags is None or isinstance(record.tags, str)

    def test_search_area(self, record) -> None:
        assert record.search_area == "Rome__Italy"

    def test_num_adults(self, record) -> None:
        assert record.num_adults == 2

    def test_is_available_true(self, record) -> None:
        assert record.is_available is True

    def test_is_available_when_no_price(self) -> None:
        html = textwrap.dedent("""
            <div data-testid="card-container">
              <a href="/rooms/22222"><div data-testid="listing-card-title">Ghost Listing</div></a>
              <div data-testid="listing-card-subtitle">Entire place in Nowhere, City</div>
            </div>
        """)
        card = BeautifulSoup(html, "lxml").find("div", {"data-testid": "card-container"})
        r = _parse_card(card, {}, "City__Country", "2026-09-20", "2026-09-27", "2026-09-01T10:00:00", 2)
        assert r.is_available is False

    def test_coordinates(self, record_with_coords) -> None:
        assert record_with_coords.latitude  == pytest.approx(41.8892, abs=1e-4)
        assert record_with_coords.longitude == pytest.approx(12.4711, abs=1e-4)

    def test_no_coordinates_without_map(self, record) -> None:
        assert record.latitude  is None
        assert record.longitude is None


# ---------------------------------------------------------------------------
# Coordinate extraction tests
# ---------------------------------------------------------------------------

class TestCoordinateExtraction:
    def test_extracts_coordinates_from_next_data(self) -> None:
        html = (
            f'<html><head><script id="__NEXT_DATA__">{NEXT_DATA_JSON}</script>'
            f'</head><body></body></html>'
        )
        coords = _extract_coordinates_from_page(html)
        assert "67890" in coords
        lat, lon = coords["67890"]
        assert lat == pytest.approx(41.8892, abs=1e-4)
        assert lon == pytest.approx(12.4711, abs=1e-4)

    def test_returns_empty_dict_on_missing_script(self) -> None:
        coords = _extract_coordinates_from_page("<html><body>nothing</body></html>")
        assert coords == {}

    def test_returns_empty_dict_on_invalid_json(self) -> None:
        coords = _extract_coordinates_from_page(
            '<html><head><script id="__NEXT_DATA__">not-json</script></head></html>'
        )
        assert coords == {}
