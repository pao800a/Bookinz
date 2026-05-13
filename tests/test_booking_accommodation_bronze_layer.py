"""Tests for the bronze layer storage."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from bookinz.storage.booking_accommodation_bronze_layer import BOOKING_ACCOMMODATION_BRONZE_SCHEMA, BookingAccommodationBronzeLayer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {
        "facility_id": "hotel_001",
        "name": "Hotel Alpha",
        "url": "https://www.booking.com/hotel/nl/alpha.html",
        "search_area": "Amsterdam",
        "checkin_date": "2024-02-01",
        "checkout_date": "2024-02-03",
        "scraped_at": "2024-01-15T08:00:00",
        "num_adults": 2,
        "total_price": 120.0,
        "currency": "EUR",
        "rating": 8.5,
        "rating_category": "Fabulous",
        "num_reviews": 342,
        "distance_from_center_km": 1.2,
        "num_rooms_available": 3,
        "neighbourhood": "City Centre",
        "accommodation_type": "Entire apartment",
        "tags": "New to Booking.com|Free WiFi",
        "is_available": True,
        "raw_html_snippet": "<div>Hotel Alpha</div>",
    },
    {
        "facility_id": "hotel_002",
        "name": "Hotel Beta",
        "url": "https://www.booking.com/hotel/nl/beta.html",
        "search_area": "Amsterdam",
        "checkin_date": "2024-02-01",
        "checkout_date": "2024-02-03",
        "scraped_at": "2024-01-15T08:00:00",
        "num_adults": 2,
        "total_price": None,
        "currency": None,
        "rating": 7.2,
        "rating_category": "Good",
        "num_reviews": 80,
        "distance_from_center_km": 2.5,
        "num_rooms_available": None,
        "neighbourhood": None,
        "accommodation_type": "Hotel room",
        "tags": None,
        "is_available": False,
        "raw_html_snippet": "<div>Hotel Beta</div>",
    },
]


@pytest.fixture
def tmp_data_path(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def bronze(tmp_data_path: Path) -> BookingAccommodationBronzeLayer:
    return BookingAccommodationBronzeLayer(tmp_data_path)


# ---------------------------------------------------------------------------
# Tests: write
# ---------------------------------------------------------------------------

class TestBronzeWrite:
    def test_write_creates_parquet_file(self, bronze: BookingAccommodationBronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_write_partition_directory_structure(self, bronze: BookingAccommodationBronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        # Hive-style: search_area=Amsterdam/scrape_date=2024-01-15/
        assert "search_area=Amsterdam" in str(path)
        assert "scrape_date=2024-01-15" in str(path)

    def test_write_file_name_contains_timestamp(self, bronze: BookingAccommodationBronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        assert "run_" in path.name

    def test_write_empty_records_raises(self, bronze: BookingAccommodationBronzeLayer) -> None:
        with pytest.raises(ValueError):
            bronze.write([], "2024-01-15T08:00:00")

    def test_write_schema_columns(self, bronze: BookingAccommodationBronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        # Read the file directly (without hive-partition discovery) to check
        # the stored schema without partition-column type conflicts.
        table = pq.ParquetFile(str(path)).read()
        expected_cols = {field.name for field in BOOKING_ACCOMMODATION_BRONZE_SCHEMA}
        assert expected_cols == set(table.schema.names)

    def test_write_row_count(self, bronze: BookingAccommodationBronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        table = pq.ParquetFile(str(path)).read()
        assert table.num_rows == len(SAMPLE_RECORDS)

    def test_write_sanitizes_special_chars_in_area(self, bronze: BookingAccommodationBronzeLayer) -> None:
        records = [{**r, "search_area": "New York/Manhattan"} for r in SAMPLE_RECORDS]
        path = bronze.write(records, "2024-01-15T08:00:00")
        # Directory name should not contain '/'
        assert "/" not in path.parent.parent.name.split("=")[1]


# ---------------------------------------------------------------------------
# Tests: path structure
# ---------------------------------------------------------------------------

class TestBronzePath:
    def test_bronze_root_uses_new_layout(self, bronze: BookingAccommodationBronzeLayer) -> None:
        assert bronze.bronze_root.parts[-3:] == ("bronze", "booking", "accommodations")


# ---------------------------------------------------------------------------
# Tests: DuckDB connection
# ---------------------------------------------------------------------------

class TestBronzeConnection:
    def test_connection_returns_open_connection(self, bronze: BookingAccommodationBronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        con = bronze.connection()
        result = con.execute("SELECT COUNT(*) AS n FROM booking_accommodation_bronze").fetchone()
        assert result[0] == len(SAMPLE_RECORDS)
        con.close()

    def test_multiple_dates_queryable(self, bronze: BookingAccommodationBronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        day2 = [{**r, "scraped_at": "2024-01-16T08:00:00"} for r in SAMPLE_RECORDS]
        bronze.write(day2, "2024-01-16T08:00:00")

        con = bronze.connection()
        df = con.execute(
            "SELECT DISTINCT CAST(scrape_date AS VARCHAR) AS scrape_date "
            "FROM booking_accommodation_bronze ORDER BY scrape_date"
        ).df()
        con.close()
        assert list(df["scrape_date"]) == ["2024-01-15", "2024-01-16"]
