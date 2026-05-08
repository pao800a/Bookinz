"""Tests for the availability monitor."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

from bookinz.alerts.availability_monitor import AvailabilityAlert, AvailabilityMonitor
from bookinz.storage.bronze_layer import BronzeLayer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_record(
    facility_id: str,
    scraped_at: str,
    is_available: bool,
    search_area: str = "Amsterdam",
) -> dict:
    return {
        "facility_id": facility_id,
        "name": f"Hotel {facility_id}",
        "url": f"https://www.booking.com/hotel/nl/{facility_id}.html",
        "search_area": search_area,
        "checkin_date": "2024-02-01",
        "checkout_date": "2024-02-03",
        "scraped_at": scraped_at,
        "price_per_night": 100.0 if is_available else None,
        "currency": "EUR" if is_available else None,
        "rating": 8.0,
        "rating_category": "Excellent",
        "num_reviews": 100,
        "distance_from_center_km": 1.5,
        "num_rooms_available": 2 if is_available else None,
        "is_available": is_available,
        "raw_html_snippet": "",
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAvailabilityMonitor:
    def test_no_alerts_when_no_history(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")
        # Only one scrape — no prior unavailability
        records = [_make_record("h1", "2024-01-15T08:00:00", is_available=False)]
        bronze.write(records, "2024-01-15T08:00:00")

        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")
        assert alerts == []

    def test_alert_fired_when_facility_recovers(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")

        # Day 1: hotel_001 is unavailable
        day1 = [_make_record("hotel_001", "2024-01-14T08:00:00", is_available=False)]
        bronze.write(day1, "2024-01-14T08:00:00")

        # Day 2: hotel_001 is now available
        day2 = [_make_record("hotel_001", "2024-01-15T08:00:00", is_available=True)]
        bronze.write(day2, "2024-01-15T08:00:00")

        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")

        assert len(alerts) == 1
        alert = alerts[0]
        assert isinstance(alert, AvailabilityAlert)
        assert alert.facility_id == "hotel_001"
        assert alert.recovered_at == "2024-01-15T08:00:00"

    def test_no_alert_when_facility_was_already_available(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")

        day1 = [_make_record("hotel_001", "2024-01-14T08:00:00", is_available=True)]
        bronze.write(day1, "2024-01-14T08:00:00")

        day2 = [_make_record("hotel_001", "2024-01-15T08:00:00", is_available=True)]
        bronze.write(day2, "2024-01-15T08:00:00")

        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")
        assert alerts == []

    def test_no_alert_when_still_unavailable(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")

        day1 = [_make_record("hotel_001", "2024-01-14T08:00:00", is_available=False)]
        bronze.write(day1, "2024-01-14T08:00:00")

        day2 = [_make_record("hotel_001", "2024-01-15T08:00:00", is_available=False)]
        bronze.write(day2, "2024-01-15T08:00:00")

        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")
        assert alerts == []

    def test_alert_str_representation(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")
        day1 = [_make_record("hotel_X", "2024-01-14T08:00:00", is_available=False)]
        bronze.write(day1, "2024-01-14T08:00:00")
        day2 = [_make_record("hotel_X", "2024-01-15T08:00:00", is_available=True)]
        bronze.write(day2, "2024-01-15T08:00:00")

        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")
        assert alerts
        text = str(alerts[0])
        assert "ALERT" in text
        assert "hotel_X" in text
        assert "Amsterdam" in text

    def test_error_in_query_returns_empty_list(self, tmp_path: Path) -> None:
        bronze_mock = MagicMock(spec=BronzeLayer)
        bronze_mock.query.side_effect = RuntimeError("DB error")

        monitor = AvailabilityMonitor(bronze_mock)
        alerts = monitor.check("Amsterdam", "2024-01-15T08:00:00")
        assert alerts == []

    def test_invalid_search_area_raises(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")
        monitor = AvailabilityMonitor(bronze)
        with pytest.raises(ValueError, match="invalid characters"):
            monitor.check("'; DROP TABLE bronze; --", "2024-01-15T08:00:00")

    def test_invalid_timestamp_raises(self, tmp_path: Path) -> None:
        bronze = BronzeLayer(tmp_path / "data")
        monitor = AvailabilityMonitor(bronze)
        with pytest.raises(ValueError, match="YYYY-MM-DDTHH:MM:SS"):
            monitor.check("Amsterdam", "not-a-timestamp")
