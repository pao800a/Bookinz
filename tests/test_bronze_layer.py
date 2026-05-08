"""Tests for the bronze layer storage."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pandas as pd
import pytest

from bookinz.storage.bronze_layer import BRONZE_SCHEMA, BronzeLayer


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
        "price_per_night": 120.0,
        "currency": "EUR",
        "rating": 8.5,
        "rating_category": "Fabulous",
        "num_reviews": 342,
        "distance_from_center_km": 1.2,
        "num_rooms_available": 3,
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
        "price_per_night": None,
        "currency": None,
        "rating": 7.2,
        "rating_category": "Good",
        "num_reviews": 80,
        "distance_from_center_km": 2.5,
        "num_rooms_available": None,
        "is_available": False,
        "raw_html_snippet": "<div>Hotel Beta</div>",
    },
]


@pytest.fixture
def tmp_data_path(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def bronze(tmp_data_path: Path) -> BronzeLayer:
    return BronzeLayer(tmp_data_path)


# ---------------------------------------------------------------------------
# Tests: write
# ---------------------------------------------------------------------------

class TestBronzeWrite:
    def test_write_creates_parquet_file(self, bronze: BronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_write_partition_directory_structure(self, bronze: BronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        # Hive-style: search_area=Amsterdam/scrape_date=2024-01-15/
        assert "search_area=Amsterdam" in str(path)
        assert "scrape_date=2024-01-15" in str(path)

    def test_write_file_name_contains_timestamp(self, bronze: BronzeLayer) -> None:
        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        assert "run_" in path.name

    def test_write_empty_records_raises(self, bronze: BronzeLayer) -> None:
        with pytest.raises(ValueError):
            bronze.write([], "2024-01-15T08:00:00")

    def test_write_schema_columns(self, bronze: BronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        # Read the file directly (without hive-partition discovery) to check
        # the stored schema without partition-column type conflicts.
        table = pq.ParquetFile(str(path)).read()
        expected_cols = {field.name for field in BRONZE_SCHEMA}
        assert expected_cols == set(table.schema.names)

    def test_write_row_count(self, bronze: BronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        table = pq.ParquetFile(str(path)).read()
        assert table.num_rows == len(SAMPLE_RECORDS)

    def test_write_sanitizes_special_chars_in_area(self, bronze: BronzeLayer) -> None:
        records = [{**r, "search_area": "New York/Manhattan"} for r in SAMPLE_RECORDS]
        path = bronze.write(records, "2024-01-15T08:00:00")
        # Directory name should not contain '/'
        assert "/" not in path.parent.parent.name.split("=")[1]


# ---------------------------------------------------------------------------
# Tests: DuckDB query
# ---------------------------------------------------------------------------

class TestBronzeQuery:
    def test_query_returns_all_rows(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        df = bronze.query("SELECT * FROM bronze")
        assert len(df) == len(SAMPLE_RECORDS)

    def test_query_filter_by_search_area(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        # Write a second area
        paris_records = [
            {**r, "search_area": "Paris", "facility_id": f"p_{r['facility_id']}"}
            for r in SAMPLE_RECORDS
        ]
        bronze.write(paris_records, "2024-01-15T08:00:00")

        df = bronze.query("SELECT * FROM bronze WHERE search_area = 'Amsterdam'")
        assert len(df) == len(SAMPLE_RECORDS)
        assert set(df["search_area"].unique()) == {"Amsterdam"}

    def test_query_price_column_type(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        df = bronze.query("SELECT price_per_night FROM bronze WHERE price_per_night IS NOT NULL")
        assert pd.api.types.is_float_dtype(df["price_per_night"])

    def test_query_available_only(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        df = bronze.query("SELECT * FROM bronze WHERE is_available = true")
        assert all(df["is_available"])

    def test_connection_returns_open_connection(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        con = bronze.connection()
        result = con.execute("SELECT COUNT(*) AS n FROM bronze").fetchone()
        assert result[0] == len(SAMPLE_RECORDS)
        con.close()

    def test_multiple_dates_queryable(self, bronze: BronzeLayer) -> None:
        bronze.write(SAMPLE_RECORDS, "2024-01-15T08:00:00")
        day2 = [{**r, "scraped_at": "2024-01-16T08:00:00"} for r in SAMPLE_RECORDS]
        bronze.write(day2, "2024-01-16T08:00:00")

        df = bronze.query(
            "SELECT DISTINCT CAST(scrape_date AS VARCHAR) AS scrape_date "
            "FROM bronze ORDER BY scrape_date"
        )
        assert list(df["scrape_date"]) == ["2024-01-15", "2024-01-16"]
