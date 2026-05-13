"""Tests for the AirBnB bronze layer storage."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from bookinz.storage.airbnb_accommodation_bronze_layer import AIRBNB_ACCOMMODATION_BRONZE_SCHEMA, AirbnbAccommodationBronzeLayer


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

AIRBNB_SAMPLE_RECORDS = [
    {
        "facility_id":          "67890",
        "name":                 "Cozy Trastevere Apartment",
        "url":                  "https://www.airbnb.com/rooms/67890",
        "search_area":          "Rome__Italy",
        "checkin_date":         "2026-09-20",
        "checkout_date":        "2026-09-27",
        "scraped_at":           "2026-09-01T10:00:00",
        "num_adults":           2,
        "description":          "Entire apartment in Trastevere, Rome",
        "accommodation_type":   "Entire apartment",
        "neighbourhood":        "Trastevere",
        "num_bedrooms":         2,
        "num_beds":             4,
        "host_type":            None,
        "total_price":          1234.0,
        "currency":             "USD",
        "price_is_per_night":   False,
        "rating":               4.86,
        "num_reviews":          127,
        "is_superhost":         True,
        "is_free_cancellation": True,
        "tags":                 None,
        "latitude":             41.8892,
        "longitude":            12.4711,
        "is_available":         True,
        "raw_html_snippet":     "<div>Cozy Trastevere Apartment</div>",
    },
    {
        "facility_id":          "11111",
        "name":                 "Basic Room in Rome",
        "url":                  "https://www.airbnb.com/rooms/11111",
        "search_area":          "Rome__Italy",
        "checkin_date":         "2026-09-20",
        "checkout_date":        "2026-09-27",
        "scraped_at":           "2026-09-01T10:00:00",
        "num_adults":           2,
        "description":          "Private room in Testville, Rome",
        "accommodation_type":   "Private room",
        "neighbourhood":        "Testville",
        "num_bedrooms":         1,
        "num_beds":             1,
        "host_type":            "Private host",
        "total_price":          None,
        "currency":             None,
        "price_is_per_night":   False,
        "rating":               None,
        "num_reviews":          None,
        "is_superhost":         False,
        "is_free_cancellation": False,
        "tags":                 None,
        "latitude":             None,
        "longitude":            None,
        "is_available":         False,
        "raw_html_snippet":     "<div>Basic Room</div>",
    },
]


@pytest.fixture
def tmp_data_path(tmp_path: Path) -> Path:
    return tmp_path / "data"


@pytest.fixture
def bronze(tmp_data_path: Path) -> AirbnbAccommodationBronzeLayer:
    return AirbnbAccommodationBronzeLayer(tmp_data_path)


# ---------------------------------------------------------------------------
# Tests: write
# ---------------------------------------------------------------------------

class TestBronzeWrite:
    def test_write_creates_parquet_file(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        path = bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        assert path.exists()
        assert path.suffix == ".parquet"

    def test_write_partition_directory_structure(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        path = bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        # Hive-style: search_area=Rome__Italy/scrape_date=2026-09-01/
        assert "search_area=Rome__Italy" in str(path)
        assert "scrape_date=2026-09-01" in str(path)

    def test_write_file_name_contains_timestamp(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        path = bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        assert "run_" in path.name

    def test_write_empty_records_raises(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        with pytest.raises(ValueError):
            bronze.write([], "2026-09-01T10:00:00")

    def test_write_schema_columns(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        table = pq.ParquetFile(str(path)).read()
        expected_cols = {field.name for field in AIRBNB_ACCOMMODATION_BRONZE_SCHEMA}
        assert expected_cols == set(table.schema.names)

    def test_write_row_count(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        import pyarrow.parquet as pq

        path = bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        table = pq.ParquetFile(str(path)).read()
        assert table.num_rows == len(AIRBNB_SAMPLE_RECORDS)

    def test_write_sanitizes_special_chars_in_area(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        records = [{**r, "search_area": "New York/Manhattan"} for r in AIRBNB_SAMPLE_RECORDS]
        path = bronze.write(records, "2026-09-01T10:00:00")
        assert "/" not in path.parent.parent.name.split("=")[1]


# ---------------------------------------------------------------------------
# Tests: path structure
# ---------------------------------------------------------------------------

class TestAirbnbBronzePath:
    def test_bronze_root_uses_new_layout(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        assert bronze.bronze_root.parts[-3:] == ("bronze", "airbnb", "accommodations")


# ---------------------------------------------------------------------------
# Tests: DuckDB connection
# ---------------------------------------------------------------------------

class TestBronzeConnection:
    def test_connection_returns_open_connection(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        con = bronze.connection()
        result = con.execute("SELECT COUNT(*) AS n FROM airbnb_accommodation_bronze").fetchone()
        assert result[0] == len(AIRBNB_SAMPLE_RECORDS)
        con.close()

    def test_schema_evolution_missing_column_is_null(
        self, bronze: AirbnbAccommodationBronzeLayer, tmp_data_path: Path
    ) -> None:
        """Write a file without latitude/longitude; they should appear as NULL in query."""
        import pyarrow as pa
        import pyarrow.parquet as pq

        # Create a file with the full schema except latitude/longitude
        fields_subset = [f for f in AIRBNB_ACCOMMODATION_BRONZE_SCHEMA if f.name not in ("latitude", "longitude")]
        schema_subset = pa.schema(fields_subset)
        record = {
            k: v for k, v in AIRBNB_SAMPLE_RECORDS[0].items()
            if k not in ("latitude", "longitude")
        }
        df = pd.DataFrame([record])
        for col in [f.name for f in fields_subset]:
            if col not in df.columns:
                df[col] = None

        partition_dir = (
            bronze.bronze_root
            / "search_area=Rome__Italy"
            / "scrape_date=2026-09-01"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)
        table_subset = pa.Table.from_pandas(df[list(record.keys())], schema=schema_subset, preserve_index=False)
        pq.write_table(table_subset, partition_dir / "run_old.parquet")

        # Connection must succeed and return NULL for latitude/longitude
        con = bronze.connection()
        df_out = con.execute("SELECT latitude, longitude FROM airbnb_accommodation_bronze").df()
        con.close()
        assert "latitude"  in df_out.columns
        assert "longitude" in df_out.columns

    def test_multiple_dates_queryable(self, bronze: AirbnbAccommodationBronzeLayer) -> None:
        bronze.write(AIRBNB_SAMPLE_RECORDS, "2026-09-01T10:00:00")
        day2 = [{**r, "scraped_at": "2026-09-02T10:00:00"} for r in AIRBNB_SAMPLE_RECORDS]
        bronze.write(day2, "2026-09-02T10:00:00")

        con = bronze.connection()
        df = con.execute(
            "SELECT DISTINCT CAST(scrape_date AS VARCHAR) AS scrape_date "
            "FROM airbnb_accommodation_bronze ORDER BY scrape_date"
        ).df()
        con.close()
        assert list(df["scrape_date"]) == ["2026-09-01", "2026-09-02"]
