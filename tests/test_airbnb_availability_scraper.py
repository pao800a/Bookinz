"""Tests for the AirBnB availability scraper — uses mock API responses.
No Playwright or network I/O required."""

from __future__ import annotations

import pytest

from bookinz.scraper.airbnb_availability_scraper import (
    AirbnbAvailabilityRecord,
    AirbnbAvailabilityScraper,
    _parse_calendar_response,
)


# ---------------------------------------------------------------------------
# Fixtures — mock API response JSON
# ---------------------------------------------------------------------------

CALENDAR_RESPONSE = {
    "calendar_months": [
        {
            "month": 5,
            "year": 2026,
            "days": [
                {"date": "2026-05-09", "available": True,  "min_nights": 3},
                {"date": "2026-05-10", "available": True,  "min_nights": 3},
                {"date": "2026-05-11", "available": False, "min_nights": None},
                {"date": "2026-05-12", "available": False, "min_nights": 2},
            ],
        },
        {
            "month": 6,
            "year": 2026,
            "days": [
                {"date": "2026-06-01", "available": True, "min_nights": 5},
            ],
        },
    ]
}

EMPTY_CALENDAR_RESPONSE: dict = {"calendar_months": []}

SCRAPED_AT = "2026-05-09T10:00:00"
FACILITY_ID = "listing_abc"


# ---------------------------------------------------------------------------
# _parse_calendar_response
# ---------------------------------------------------------------------------

class TestParseCalendarResponse:
    def test_returns_correct_count(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        assert len(records) == 5   # 4 days in May + 1 in June

    def test_record_types(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        for r in records:
            assert isinstance(r, AirbnbAvailabilityRecord)

    def test_facility_id_propagated(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        assert all(r.facility_id == FACILITY_ID for r in records)

    def test_scraped_at_propagated(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        assert all(r.scraped_at == SCRAPED_AT for r in records)

    def test_availability_flag(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        by_date = {r.date: r for r in records}
        assert by_date["2026-05-09"].is_available is True
        assert by_date["2026-05-11"].is_available is False

    def test_min_nights_parsed(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        by_date = {r.date: r for r in records}
        assert by_date["2026-05-09"].min_nights == 3
        assert by_date["2026-06-01"].min_nights == 5

    def test_min_nights_none_when_null(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        by_date = {r.date: r for r in records}
        assert by_date["2026-05-11"].min_nights is None

    def test_empty_response_returns_empty_list(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, EMPTY_CALENDAR_RESPONSE)
        assert records == []

    def test_date_format(self) -> None:
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, CALENDAR_RESPONSE)
        for r in records:
            parts = r.date.split("-")
            assert len(parts) == 3
            assert len(parts[0]) == 4  # year

    def test_day_without_date_field_is_skipped(self) -> None:
        response = {
            "calendar_months": [
                {
                    "month": 5,
                    "year": 2026,
                    "days": [
                        {"available": True, "min_nights": 1},      # no 'date' key
                        {"date": "2026-05-20", "available": True, "min_nights": 1},
                    ],
                }
            ]
        }
        records = _parse_calendar_response(FACILITY_ID, SCRAPED_AT, response)
        assert len(records) == 1
        assert records[0].date == "2026-05-20"


# ---------------------------------------------------------------------------
# AirbnbAvailabilityRecord.as_dict
# ---------------------------------------------------------------------------

class TestAvailabilityRecordAsDict:
    def test_as_dict_keys(self) -> None:
        r = AirbnbAvailabilityRecord(
            facility_id  = "abc",
            date         = "2026-05-09",
            is_available = True,
            min_nights   = 2,
            scraped_at   = SCRAPED_AT,
        )
        d = r.as_dict()
        assert set(d.keys()) == {"facility_id", "date", "is_available", "min_nights", "scraped_at"}

    def test_as_dict_values(self) -> None:
        r = AirbnbAvailabilityRecord(
            facility_id  = "abc",
            date         = "2026-05-09",
            is_available = False,
            min_nights   = None,
            scraped_at   = SCRAPED_AT,
        )
        d = r.as_dict()
        assert d["is_available"] is False
        assert d["min_nights"] is None
