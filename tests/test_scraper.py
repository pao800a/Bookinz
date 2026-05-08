"""Tests for the booking.com scraper — uses mocked HTTP responses."""

from __future__ import annotations

import textwrap

import pytest

from bookinz.scraper.booking_scraper import (
    BookingComScraper,
    _safe_float,
    _safe_int,
    _parse_card,
)
from bs4 import BeautifulSoup


# ---------------------------------------------------------------------------
# Unit tests for helpers
# ---------------------------------------------------------------------------

class TestSafeFloat:
    def test_plain_number(self) -> None:
        assert _safe_float("120.50") == pytest.approx(120.50)

    def test_with_currency_symbol(self) -> None:
        assert _safe_float("€\xa0120,50") == pytest.approx(120.50)

    def test_none_input(self) -> None:
        assert _safe_float(None) is None

    def test_empty_string(self) -> None:
        assert _safe_float("") is None

    def test_thousands_dot_non_zero(self) -> None:
        # "1.101" → dot is a thousands separator → 1101
        assert _safe_float("1.101") == pytest.approx(1101.0)

    def test_trailing_zeros_treated_as_decimal(self) -> None:
        # "758.000" → trailing group is all zeros → round number, dot is decimal → 758
        assert _safe_float("758.000") == pytest.approx(758.0)

    def test_european_format_with_both_separators(self) -> None:
        # "1.234,56" → dot=thousands, comma=decimal → 1234.56
        assert _safe_float("1.234,56") == pytest.approx(1234.56)

    def test_comma_thousands_no_decimal(self) -> None:
        # "1,101" → comma is thousands separator (3-digit group) → 1101
        assert _safe_float("1,101") == pytest.approx(1101.0)

    def test_comma_thousands_round_number(self) -> None:
        # "1,000" → comma is thousands separator → 1000
        assert _safe_float("1,000") == pytest.approx(1000.0)

    def test_comma_decimal_two_digits(self) -> None:
        # "8,50" → comma is decimal (2-digit group) → 8.50
        assert _safe_float("8,50") == pytest.approx(8.50)


class TestSafeInt:
    def test_plain_number(self) -> None:
        assert _safe_int("342 reviews") == 342

    def test_none_input(self) -> None:
        assert _safe_int(None) is None

    def test_no_digits(self) -> None:
        assert _safe_int("abc") is None


# ---------------------------------------------------------------------------
# HTML card parsing
# ---------------------------------------------------------------------------

CARD_HTML = textwrap.dedent(
    """
    <div data-testid="property-card" data-hotelid="12345">
      <a data-testid="title-link" href="/hotel/nl/alpha.html">
        <span data-testid="title">Hotel Alpha</span>
      </a>
      <span data-testid="price-and-discounted-price">&euro;&nbsp;120</span>
      <div data-testid="review-score">
        <div>
          <div>Scored 8.5 </div>
          <div aria-hidden="true">8.5</div>
          <div aria-hidden="false">
            <span> Fabulous</span>
            <span>&middot;&nbsp;342 reviews</span>
          </div>
        </div>
      </div>
      <div data-testid="location">City Centre &#x2022; 1.2 km from centre</div>
      <div data-testid="badges">New to Booking.com</div>
      <div data-testid="unit-configuration">Entire apartment &#x2013; 80 m&#xb2;: 2 beds &#x2022; 1 bedroom</div>
      <div>
        <span data-testid="icon-with-text-icon"></span><span>Free WiFi</span>
      </div>
      <span>Only 3 rooms left</span>
    </div>
    """
)


@pytest.fixture
def card() -> BeautifulSoup:
    soup = BeautifulSoup(CARD_HTML, "lxml")
    return soup.find("div", {"data-testid": "property-card"})


class TestParseCard:
    def test_facility_id(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.facility_id == "12345"

    def test_name(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.name == "Hotel Alpha"

    def test_url_is_absolute(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        # Relative URL /hotel/nl/alpha.html should be converted to absolute.
        assert record.url.startswith("https://") and record.url.endswith("/hotel/nl/alpha.html")

    def test_price(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.total_price == pytest.approx(120.0)

    def test_rating(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.rating == pytest.approx(8.5)

    def test_rating_category(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.rating_category == "Fabulous"

    def test_num_reviews(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.num_reviews == 342

    def test_distance(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.distance_from_center_km == pytest.approx(1.2)

    def test_rooms_available(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.num_rooms_available == 3

    def test_is_available_true_when_price_present(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.is_available is True

    def test_search_area_preserved(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.search_area == "Amsterdam"

    def test_num_adults(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=4)
        assert record.num_adults == 4

    def test_neighbourhood(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.neighbourhood == "City Centre"

    def test_accommodation_type(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.accommodation_type == "Entire apartment"

    def test_tags_includes_badge(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.tags is not None
        assert "New to Booking.com" in record.tags

    def test_tags_includes_amenity(self, card: BeautifulSoup) -> None:
        record = _parse_card(card, "Amsterdam", "2024-02-01", "2024-02-03", "2024-01-15T08:00:00", num_adults=2)
        assert record.tags is not None
        assert "Free WiFi" in record.tags


# ---------------------------------------------------------------------------
# Scraper integration (mocked HTTP)
# ---------------------------------------------------------------------------

_SEARCH_HTML = f"""
<html><body>
{CARD_HTML * 25}
</body></html>
"""

_EMPTY_HTML = "<html><body><p>No results</p></body></html>"


class TestBookingComScraper:
    def test_scrape_as_dicts_returns_list_of_dicts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        scraper = BookingComScraper(
            search_area="Amsterdam",
            checkin_date="2024-02-01",
            checkout_date="2024-02-03",
            max_pages=1,
        )
        monkeypatch.setattr(scraper, "_fetch_page", lambda url: _SEARCH_HTML)
        records = scraper.scrape_as_dicts("2024-01-15T08:00:00")
        assert isinstance(records, list)
        assert len(records) == 25
        assert all(isinstance(r, dict) for r in records)
        assert records[0]["name"] == "Hotel Alpha"

    def test_scrape_stops_on_empty_page(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # First page returns 25 results, second returns none → scraper stops.
        scraper = BookingComScraper(
            search_area="Amsterdam",
            checkin_date="2024-02-01",
            checkout_date="2024-02-03",
            max_pages=5,
        )
        call_count = 0

        def fake_fetch(url: str) -> str:
            nonlocal call_count
            call_count += 1
            return _SEARCH_HTML if call_count == 1 else _EMPTY_HTML

        monkeypatch.setattr(scraper, "_fetch_page", fake_fetch)
        records = scraper.scrape_as_dicts("2024-01-15T08:00:00")
        assert call_count == 2  # stopped after empty second page
        assert len(records) == 25

    def test_scrape_request_error_stops_gracefully(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import requests

        scraper = BookingComScraper(
            search_area="Amsterdam",
            checkin_date="2024-02-01",
            checkout_date="2024-02-03",
            max_pages=3,
        )

        def raise_error(url: str) -> str:
            raise requests.RequestException("connection refused")

        monkeypatch.setattr(scraper, "_fetch_page", raise_error)
        records = scraper.scrape_as_dicts("2024-01-15T08:00:00")
        assert records == []
