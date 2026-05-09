"""Tests for the AirBnB facility scraper — uses static HTML fixtures with
embedded __NEXT_DATA__ JSON blobs. No Playwright or network I/O required."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from bookinz.scraper.airbnb_facility_scraper import (
    AirbnbFacilityRecord,
    AirbnbFacilityScraper,
    _find_section,
    _all_sections,
    _deep_find,
    _extract_overview,
    _extract_description,
    _extract_highlights,
    _extract_reviews,
    _extract_host,
    _extract_location,
    _extract_policies,
    _extract_amenities,
)


# ---------------------------------------------------------------------------
# __NEXT_DATA__ fixtures
# ---------------------------------------------------------------------------

def _make_next_data(sections: list[dict]) -> dict:
    """Wrap sections in a minimal __NEXT_DATA__ structure."""
    return {
        "props": {
            "pageProps": {
                "sections": sections,
            }
        }
    }


def _make_html(data: dict) -> str:
    raw = json.dumps(data)
    return f'<html><head></head><body><script id="__NEXT_DATA__">{raw}</script></body></html>'


OVERVIEW_SECTION = {
    "sectionId": "OVERVIEW_DEFAULT_V2",
    "name": "Cosy flat in Lisbon",
    "roomTypeCategory": "Entire apartment",
    "overviewItems": [
        {"title": "6 guests"},
        {"title": "2 bedrooms"},
        {"title": "3 beds"},
        {"title": "1.5 baths"},
    ],
}

DESCRIPTION_SECTION = {
    "sectionId": "DESCRIPTION_DEFAULT",
    "htmlDescription": "<p>Beautiful apartment with great views.</p>",
}

HIGHLIGHTS_SECTION = {
    "sectionId": "HIGHLIGHTS_DEFAULT",
    "headline": "Guest favourite",
}

REVIEWS_SECTION = {
    "sectionId": "REVIEWS_DEFAULT",
    "overallRating": 4.87,
    "numberOfReviews": 153,
    "reviewCategories": [
        {"category": "Cleanliness", "rating": 4.9},
        {"category": "Accuracy",    "rating": 4.8},
        {"category": "Check-in",    "rating": 4.95},
        {"category": "Communication", "rating": 5.0},
        {"category": "Location",    "rating": 4.7},
        {"category": "Value",       "rating": 4.6},
    ],
}

HOST_SECTION = {
    "sectionId": "HOST_PROFILE_DEFAULT",
    "hostName": "Maria",
    "hostId": "98765",
    "isSuperhost": True,
    "memberSince": "Joined in 2018",
    "responseRate": "100%",
    "responseTime": "within an hour",
}

LOCATION_SECTION = {
    "sectionId": "LOCATION_DEFAULT",
    "lat": 38.7169,
    "lng": -9.1399,
}

POLICIES_SECTION = {
    "sectionId": "POLICIES_DEFAULT",
    "minNights": 3,
    "maxNights": 60,
    "checkInTime": "15:00",
    "checkOutTime": "11:00",
    "cancellationPolicyName": "Moderate",
    "houseRules": [
        {"title": "No smoking"},
        {"title": "No parties"},
    ],
}

AMENITIES_SECTION = {
    "sectionId": "AMENITIES_DEFAULT",
    "seeAllAmenitiesGroups": [
        {
            "amenities": [
                {"title": "Wifi"},
                {"title": "Kitchen"},
                {"title": "Washing machine"},
                {"title": "Air conditioning"},
                {"title": "Dedicated workspace"},
            ]
        }
    ],
}

ALL_SECTIONS = [
    OVERVIEW_SECTION,
    DESCRIPTION_SECTION,
    HIGHLIGHTS_SECTION,
    REVIEWS_SECTION,
    HOST_SECTION,
    LOCATION_SECTION,
    POLICIES_SECTION,
    AMENITIES_SECTION,
]


# ---------------------------------------------------------------------------
# _find_section
# ---------------------------------------------------------------------------

class TestFindSection:
    def test_exact_prefix_match(self) -> None:
        sec = _find_section(ALL_SECTIONS, "OVERVIEW_DEFAULT")
        assert sec is not None
        assert sec["sectionId"] == "OVERVIEW_DEFAULT_V2"

    def test_returns_none_when_missing(self) -> None:
        assert _find_section(ALL_SECTIONS, "NONEXISTENT") is None

    def test_empty_list(self) -> None:
        assert _find_section([], "OVERVIEW_DEFAULT") is None

    def test_non_dict_items_are_skipped(self) -> None:
        sections = ["string", 42, None, {"sectionId": "REVIEWS_DEFAULT", "x": 1}]
        sec = _find_section(sections, "REVIEWS_DEFAULT")
        assert sec is not None


# ---------------------------------------------------------------------------
# _deep_find
# ---------------------------------------------------------------------------

class TestDeepFind:
    def test_finds_at_top_level(self) -> None:
        assert _deep_find({"key": "value"}, "key") == "value"

    def test_finds_nested(self) -> None:
        obj = {"a": {"b": {"c": 42}}}
        assert _deep_find(obj, "c") == 42

    def test_finds_in_list(self) -> None:
        obj = [{"x": 1}, {"y": 2}]
        assert _deep_find(obj, "y") == 2

    def test_returns_none_when_missing(self) -> None:
        assert _deep_find({"a": 1}, "z") is None


# ---------------------------------------------------------------------------
# _extract_overview
# ---------------------------------------------------------------------------

class TestExtractOverview:
    def test_full_section(self) -> None:
        result = _extract_overview(OVERVIEW_SECTION)
        assert result["name"] == "Cosy flat in Lisbon"
        assert result["accommodation_type"] == "Entire apartment"
        assert result["num_guests"] == 6
        assert result["num_bedrooms"] == 2
        assert result["num_beds"] == 3
        assert result["num_bathrooms"] == pytest.approx(1.5)

    def test_empty_section(self) -> None:
        result = _extract_overview({})
        assert result["name"] is None
        assert result["num_guests"] is None


# ---------------------------------------------------------------------------
# _extract_description
# ---------------------------------------------------------------------------

class TestExtractDescription:
    def test_strips_html_tags(self) -> None:
        desc = _extract_description(DESCRIPTION_SECTION)
        assert desc is not None
        assert "<p>" not in desc
        assert "Beautiful apartment" in desc

    def test_empty_section_returns_none(self) -> None:
        assert _extract_description({}) is None


# ---------------------------------------------------------------------------
# _extract_highlights
# ---------------------------------------------------------------------------

class TestExtractHighlights:
    def test_guest_favourite(self) -> None:
        label, is_gf = _extract_highlights(HIGHLIGHTS_SECTION)
        assert label == "Guest favourite"
        assert is_gf is True

    def test_non_favourite_label(self) -> None:
        label, is_gf = _extract_highlights({"sectionId": "HIGHLIGHTS_DEFAULT", "headline": "New listing"})
        assert label == "New listing"
        assert is_gf is False

    def test_empty_section(self) -> None:
        label, is_gf = _extract_highlights({})
        assert label is None
        assert is_gf is False


# ---------------------------------------------------------------------------
# _extract_reviews
# ---------------------------------------------------------------------------

class TestExtractReviews:
    def test_overall_rating_and_count(self) -> None:
        result = _extract_reviews(REVIEWS_SECTION)
        assert result["rating"] == pytest.approx(4.87)
        assert result["num_reviews"] == 153

    def test_sub_ratings(self) -> None:
        result = _extract_reviews(REVIEWS_SECTION)
        assert result["rating_cleanliness"] == pytest.approx(4.9)
        assert result["rating_accuracy"] == pytest.approx(4.8)
        assert result["rating_checkin"] == pytest.approx(4.95)
        assert result["rating_communication"] == pytest.approx(5.0)
        assert result["rating_location"] == pytest.approx(4.7)
        assert result["abnb_price_quality_score"] == pytest.approx(4.6)

    def test_empty_section(self) -> None:
        result = _extract_reviews({})
        assert result["rating"] is None
        assert result["num_reviews"] is None


# ---------------------------------------------------------------------------
# _extract_host
# ---------------------------------------------------------------------------

class TestExtractHost:
    def test_full_section(self) -> None:
        result = _extract_host(HOST_SECTION)
        assert result["host_name"] == "Maria"
        assert result["host_id"] == "98765"
        assert result["is_superhost"] is True
        assert result["host_since_year"] == 2018
        assert result["host_response_rate"] == "100%"
        assert result["host_response_time"] == "within an hour"

    def test_empty_section(self) -> None:
        result = _extract_host({})
        assert result["host_name"] is None
        assert result["is_superhost"] is False


# ---------------------------------------------------------------------------
# _extract_location
# ---------------------------------------------------------------------------

class TestExtractLocation:
    def test_from_section(self) -> None:
        lat, lng = _extract_location(LOCATION_SECTION, {})
        assert lat == pytest.approx(38.7169)
        assert lng == pytest.approx(-9.1399)

    def test_fallback_to_full_data(self) -> None:
        full_data = {"someKey": {"lat": 41.9028, "lng": 12.4964}}
        lat, lng = _extract_location({}, full_data)
        assert lat == pytest.approx(41.9028)

    def test_missing_returns_none(self) -> None:
        lat, lng = _extract_location({}, {})
        assert lat is None
        assert lng is None


# ---------------------------------------------------------------------------
# _extract_policies
# ---------------------------------------------------------------------------

class TestExtractPolicies:
    def test_full_section(self) -> None:
        result = _extract_policies(POLICIES_SECTION, {})
        assert result["min_nights"] == 3
        assert result["max_nights"] == 60
        assert result["check_in_time"] == "15:00"
        assert result["check_out_time"] == "11:00"
        assert result["cancellation_policy"] == "Moderate"
        assert result["house_rules"] is not None
        assert "No smoking" in result["house_rules"]

    def test_empty_sections(self) -> None:
        result = _extract_policies({}, {})
        assert result["min_nights"] is None


# ---------------------------------------------------------------------------
# _extract_amenities
# ---------------------------------------------------------------------------

class TestExtractAmenities:
    def test_detected_amenities(self) -> None:
        result = _extract_amenities(AMENITIES_SECTION)
        assert result["amenity_wifi"] is True
        assert result["amenity_kitchen"] is True
        assert result["amenity_washing_machine"] is True
        assert result["amenity_air_conditioning"] is True
        assert result["amenity_dedicated_workspace"] is True

    def test_absent_amenities_are_false(self) -> None:
        result = _extract_amenities(AMENITIES_SECTION)
        assert result["amenity_pool"] is False
        assert result["amenity_gym"] is False
        assert result["amenity_breakfast"] is False

    def test_amenities_raw_is_pipe_separated(self) -> None:
        result = _extract_amenities(AMENITIES_SECTION)
        assert result["amenities_raw"] is not None
        assert "Wifi" in result["amenities_raw"]
        assert "|" in result["amenities_raw"]

    def test_empty_section(self) -> None:
        result = _extract_amenities({})
        assert result["amenity_wifi"] is False
        assert result["amenities_raw"] is None


# ---------------------------------------------------------------------------
# AirbnbFacilityScraper._parse (integration-style, no browser)
# ---------------------------------------------------------------------------

class TestAirbnbFacilityScraperParse:
    """Test the _parse method directly without launching a browser."""

    def _make_scraper(self) -> AirbnbFacilityScraper:
        s = AirbnbFacilityScraper.__new__(AirbnbFacilityScraper)
        s.request_delay_s = 0
        s.headless        = True
        s._pw             = None
        s._browser        = None
        s._page           = None
        return s

    def test_full_parse(self) -> None:
        html    = _make_html(_make_next_data(ALL_SECTIONS))
        scraper = self._make_scraper()
        record  = scraper._parse("abc123", "https://www.airbnb.com/rooms/abc123", "2026-05-09T10:00:00", html)

        assert isinstance(record, AirbnbFacilityRecord)
        assert record.facility_id == "abc123"
        assert record.name == "Cosy flat in Lisbon"
        assert record.accommodation_type == "Entire apartment"
        assert record.num_guests == 6
        assert record.rating == pytest.approx(4.87)
        assert record.num_reviews == 153
        assert record.is_guest_favourite is True
        assert record.is_superhost is True
        assert record.latitude == pytest.approx(38.7169)
        assert record.min_nights == 3
        assert record.cancellation_policy == "Moderate"
        assert record.amenity_wifi is True
        assert record.amenity_pool is False
        assert record.raw_json_snippet != ""

    def test_parse_missing_next_data(self) -> None:
        html    = "<html><body><h1>No data here</h1></body></html>"
        scraper = self._make_scraper()
        record  = scraper._parse("x1", "https://www.airbnb.com/rooms/x1", "2026-05-09T10:00:00", html)

        assert record.facility_id == "x1"
        assert record.name is None
        assert record.rating is None
        assert record.amenity_wifi is False

    def test_parse_malformed_json(self) -> None:
        html    = '<html><body><script id="__NEXT_DATA__">{bad json}</script></body></html>'
        scraper = self._make_scraper()
        record  = scraper._parse("x2", "https://www.airbnb.com/rooms/x2", "2026-05-09T10:00:00", html)
        assert record.name is None
