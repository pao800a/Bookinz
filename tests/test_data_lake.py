"""Tests for the DataLake unified query interface."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from bookinz.storage.data_lake import DataLake
from bookinz.storage.airbnb_accommodation_bronze_layer import AirbnbAccommodationBronzeLayer, AIRBNB_ACCOMMODATION_BRONZE_SCHEMA
from bookinz.storage.airbnb_facility_bronze_layer import AirbnbFacilityBronzeLayer, AIRBNB_FACILITY_BRONZE_SCHEMA
from bookinz.storage.booking_accommodation_bronze_layer import BookingAccommodationBronzeLayer, BOOKING_ACCOMMODATION_BRONZE_SCHEMA


# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

_AIRBNB_RECORDS = [
    {
        "facility_id": "air_001",
        "name": "Test Apartment",
        "url": "https://www.airbnb.com/rooms/air_001",
        "search_area": "Tirana__Albania",
        "checkin_date": "2026-06-01",
        "checkout_date": "2026-06-07",
        "scraped_at": "2026-05-01T10:00:00",
        "num_adults": 2,
        "description": "Nice place",
        "accommodation_type": "Entire apartment",
        "neighbourhood": "Blloku",
        "num_bedrooms": 1,
        "num_beds": 2,
        "host_type": None,
        "total_price": 300.0,
        "currency": "EUR",
        "price_is_per_night": False,
        "rating": 4.8,
        "num_reviews": 50,
        "is_superhost": True,
        "is_free_cancellation": True,
        "tags": None,
        "latitude": 41.328,
        "longitude": 19.818,
        "is_available": True,
        "raw_html_snippet": "<div>Test</div>",
    },
]

_BOOKING_RECORDS = [
    {
        "facility_id": "book_001",
        "name": "Test Hotel",
        "url": "https://www.booking.com/hotel/al/test.html",
        "search_area": "Tirana__Albania",
        "checkin_date": "2026-06-01",
        "checkout_date": "2026-06-07",
        "scraped_at": "2026-05-01T10:00:00",
        "num_adults": 2,
        "total_price": 200.0,
        "currency": "EUR",
        "rating": 8.5,
        "rating_category": "Excellent",
        "num_reviews": 100,
        "distance_from_center_km": 0.5,
        "num_rooms_available": 3,
        "neighbourhood": "Centre",
        "accommodation_type": "Hotel",
        "tags": None,
        "is_available": True,
        "raw_html_snippet": "<div>Test Hotel</div>",
    },
]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def lake_path(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def lake_with_airbnb(lake_path: Path) -> DataLake:
    """DataLake with AirBnB accommodations bronze data written."""
    abl = AirbnbAccommodationBronzeLayer(lake_path)
    abl.write(_AIRBNB_RECORDS, "2026-05-01T10:00:00")
    return DataLake(lake_path)


@pytest.fixture
def lake_with_booking(lake_path: Path) -> DataLake:
    """DataLake with Booking.com accommodations bronze data written."""
    bbl = BookingAccommodationBronzeLayer(lake_path)
    bbl.write(_BOOKING_RECORDS, "2026-05-01T10:00:00")
    return DataLake(lake_path)


@pytest.fixture
def lake_with_facility(lake_path: Path) -> DataLake:
    """DataLake with AirBnB facility bronze data written."""
    facility_dir = lake_path / "bronze" / "airbnb" / "facility" / "facility_id=air_001" / "scrape_date=2026-05-01"
    facility_dir.mkdir(parents=True, exist_ok=True)
    record = {f.name: None for f in AIRBNB_FACILITY_BRONZE_SCHEMA}
    record["facility_id"] = "air_001"
    record["scraped_at"] = "2026-05-01T10:00:00"
    record["rating"] = 4.8
    df = pd.DataFrame([record])
    for fld in AIRBNB_FACILITY_BRONZE_SCHEMA:
        if fld.type == pa.bool_():
            df[fld.name] = df[fld.name].fillna(False).astype(bool)
        elif fld.type == pa.int64():
            df[fld.name] = pd.to_numeric(df[fld.name], errors="coerce").astype("Int64")
        elif fld.type == pa.float64():
            df[fld.name] = pd.to_numeric(df[fld.name], errors="coerce")
        else:
            df[fld.name] = df[fld.name].astype(object)
    table = pa.Table.from_pandas(df, schema=AIRBNB_FACILITY_BRONZE_SCHEMA, preserve_index=False)
    pq.write_table(table, facility_dir / "run_2026-05-01T10-00-00.parquet", compression="snappy")
    return DataLake(lake_path)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDataLakeQuery:
    def test_query_bronze_airbnb_accommodations(self, lake_with_airbnb: DataLake) -> None:
        df = lake_with_airbnb.query("SELECT * FROM bronze.airbnb.accommodations")
        assert len(df) == len(_AIRBNB_RECORDS)
        assert "facility_id" in df.columns

    def test_query_bronze_booking_accommodations(self, lake_with_booking: DataLake) -> None:
        df = lake_with_booking.query("SELECT * FROM bronze.booking.accommodations")
        assert len(df) == len(_BOOKING_RECORDS)
        assert "facility_id" in df.columns

    def test_query_bronze_airbnb_facility(self, lake_with_facility: DataLake) -> None:
        df = lake_with_facility.query("SELECT facility_id, rating FROM bronze.airbnb.facility")
        assert len(df) == 1
        assert df["facility_id"].iloc[0] == "air_001"

    def test_query_filter(self, lake_with_airbnb: DataLake) -> None:
        df = lake_with_airbnb.query(
            "SELECT * FROM bronze.airbnb.accommodations WHERE search_area = 'Tirana__Albania'"
        )
        assert len(df) == 1

    def test_query_missing_dataset_raises_at_runtime(self, lake_path: Path) -> None:
        """Querying a dataset whose directory doesn't exist raises a DuckDB error."""
        lake = DataLake(lake_path)
        with pytest.raises(Exception):  # noqa: B017
            lake.query("SELECT * FROM bronze.airbnb.accommodations")

    def test_connection_returns_open_connection(self, lake_with_airbnb: DataLake) -> None:
        con = lake_with_airbnb.connection()
        result = con.execute("SELECT COUNT(*) AS n FROM bronze.airbnb.accommodations").fetchone()
        assert result[0] == len(_AIRBNB_RECORDS)
        con.close()

    def test_invalid_base_path_raises(self) -> None:
        with pytest.raises(ValueError):
            DataLake("data/with'quote")

    def test_multiple_datasets_in_same_connection(self, lake_path: Path) -> None:
        """Both airbnb and booking data are accessible in the same connection."""
        abl = AirbnbAccommodationBronzeLayer(lake_path)
        abl.write(_AIRBNB_RECORDS, "2026-05-01T10:00:00")
        bbl = BookingAccommodationBronzeLayer(lake_path)
        bbl.write(_BOOKING_RECORDS, "2026-05-01T10:00:00")

        lake = DataLake(lake_path)
        df_airbnb  = lake.query("SELECT facility_id FROM bronze.airbnb.accommodations")
        df_booking = lake.query("SELECT facility_id FROM bronze.booking.accommodations")

        assert len(df_airbnb)  == 1
        assert len(df_booking) == 1
        assert df_airbnb["facility_id"].iloc[0]  == "air_001"
        assert df_booking["facility_id"].iloc[0] == "book_001"
