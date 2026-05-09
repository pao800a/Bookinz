"""AirBnB facility bronze layer — writes raw facility detail records to
hive-partitioned Parquet files.

Directory layout::

    data/airbnb/
    └── bronze/
        └── facilities/
            └── facility_id=<id>/
                └── scrape_date=YYYY-MM-DD/
                    └── run_<scraped_at_ts>.parquet

The DuckDB view registered by :meth:`AirbnbFacilityBronzeLayer.connection` is
named ``airbnb_facility_bronze`` — fully independent from the accommodation
search bronze view (``airbnb_bronze``).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Sequence

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

AIRBNB_FACILITY_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("facility_id",              pa.string()),
        pa.field("url",                      pa.string()),
        pa.field("scraped_at",               pa.string()),
        pa.field("name",                     pa.string()),
        pa.field("accommodation_type",       pa.string()),
        pa.field("description",              pa.string()),
        pa.field("num_guests",               pa.int64()),
        pa.field("num_bedrooms",             pa.int64()),
        pa.field("num_beds",                 pa.int64()),
        pa.field("num_bathrooms",            pa.float64()),
        pa.field("label",                    pa.string()),
        pa.field("is_guest_favourite",       pa.bool_()),
        pa.field("rating",                   pa.float64()),
        pa.field("num_reviews",              pa.int64()),
        pa.field("rating_cleanliness",       pa.float64()),
        pa.field("rating_accuracy",          pa.float64()),
        pa.field("rating_checkin",           pa.float64()),
        pa.field("rating_communication",     pa.float64()),
        pa.field("rating_location",          pa.float64()),
        pa.field("abnb_price_quality_score", pa.float64()),
        pa.field("host_name",                pa.string()),
        pa.field("host_id",                  pa.string()),
        pa.field("is_superhost",             pa.bool_()),
        pa.field("host_since_year",          pa.int64()),
        pa.field("host_response_rate",       pa.string()),
        pa.field("host_response_time",       pa.string()),
        pa.field("latitude",                 pa.float64()),
        pa.field("longitude",                pa.float64()),
        pa.field("min_nights",               pa.int64()),
        pa.field("max_nights",               pa.int64()),
        pa.field("check_in_time",            pa.string()),
        pa.field("check_out_time",           pa.string()),
        pa.field("cancellation_policy",      pa.string()),
        pa.field("house_rules",              pa.string()),
        pa.field("amenity_wifi",             pa.bool_()),
        pa.field("amenity_kitchen",          pa.bool_()),
        pa.field("amenity_washing_machine",  pa.bool_()),
        pa.field("amenity_dryer",            pa.bool_()),
        pa.field("amenity_free_parking",     pa.bool_()),
        pa.field("amenity_air_conditioning", pa.bool_()),
        pa.field("amenity_heating",          pa.bool_()),
        pa.field("amenity_tv",               pa.bool_()),
        pa.field("amenity_dedicated_workspace", pa.bool_()),
        pa.field("amenity_pool",             pa.bool_()),
        pa.field("amenity_hot_tub",          pa.bool_()),
        pa.field("amenity_gym",              pa.bool_()),
        pa.field("amenity_bbq_grill",        pa.bool_()),
        pa.field("amenity_breakfast",        pa.bool_()),
        pa.field("amenity_pets_allowed",     pa.bool_()),
        pa.field("amenity_smoking_allowed",  pa.bool_()),
        pa.field("amenities_raw",            pa.string()),
        pa.field("raw_json_snippet",         pa.string()),
    ]
)

_TYPE_MAP: dict = {
    pa.string():  "VARCHAR",
    pa.float64(): "DOUBLE",
    pa.int64():   "BIGINT",
    pa.bool_():   "BOOLEAN",
}

_VIEW_NAME = "airbnb_facility_bronze"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize(value: str) -> str:
    """Replace characters invalid in file-system partition paths."""
    return re.sub(r"[^\w\-.]", "_", value)


# ---------------------------------------------------------------------------
# Bronze layer writer
# ---------------------------------------------------------------------------

class AirbnbFacilityBronzeLayer:
    """Manages the AirBnB facility bronze (raw) data layer.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
        Facility data is stored under
        ``<base_path>/airbnb/bronze/facilities/``.
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str  = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path   = base_path
        self.bronze_root = self.base_path / "airbnb" / "bronze" / "facilities"
        self.bronze_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, records: Sequence[dict], scraped_at: str) -> list[Path]:
        """Persist *records* to the facility bronze layer.

        Each record is written to its own ``facility_id=<id>`` partition so
        that existing data for different listings is never overwritten.

        Parameters
        ----------
        records:
            List of dicts produced by
            :meth:`~bookinz.scraper.airbnb_facility_scraper.AirbnbFacilityScraper.scrape_as_dicts`.
        scraped_at:
            ISO-8601 datetime string.

        Returns
        -------
        list[Path]
            Paths of the written Parquet files (one per unique facility_id).
        """
        if not records:
            logger.warning("write() called with empty records — nothing written.")
            raise ValueError("records must be non-empty")

        df          = pd.DataFrame(records)
        df          = self._coerce_schema(df)
        scrape_date = scraped_at[:10]
        ts_safe     = scraped_at.replace(":", "-")
        written: list[Path] = []

        for facility_id, group_df in df.groupby("facility_id"):
            partition_dir = (
                self.bronze_root
                / f"facility_id={_sanitize(str(facility_id))}"
                / f"scrape_date={scrape_date}"
            )
            partition_dir.mkdir(parents=True, exist_ok=True)
            file_path = partition_dir / f"run_{ts_safe}.parquet"

            table = pa.Table.from_pandas(
                group_df, schema=AIRBNB_FACILITY_BRONZE_SCHEMA, preserve_index=False
            )
            pq.write_table(table, file_path, compression="snappy")
            logger.info(
                "AirBnB facility bronze write: %d record(s) -> %s",
                len(group_df),
                file_path,
            )
            written.append(file_path)

        return written

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the view ``airbnb_facility_bronze``.

        Example
        -------
        >>> layer = AirbnbFacilityBronzeLayer("data")
        >>> df = layer.query("SELECT facility_id, rating FROM airbnb_facility_bronze LIMIT 10")
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        result: pd.DataFrame = con.execute(sql).df()
        con.close()
        return result

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return an open DuckDB connection with ``airbnb_facility_bronze`` pre-registered.

        The caller is responsible for closing the connection.
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        return con

    def already_scraped(self, facility_id: str, scrape_date: str) -> bool:
        """Return True if data for *(facility_id, scrape_date)* already exists."""
        partition_dir = (
            self.bronze_root
            / f"facility_id={_sanitize(facility_id)}"
            / f"scrape_date={scrape_date}"
        )
        return any(partition_dir.glob("*.parquet"))

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _coerce_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add missing columns and cast types to match the schema."""
        for fld in AIRBNB_FACILITY_BRONZE_SCHEMA:
            if fld.name not in df.columns:
                if fld.type == pa.bool_():
                    df[fld.name] = False
                elif fld.type == pa.int64():
                    df[fld.name] = pd.NA
                elif fld.type == pa.float64():
                    df[fld.name] = float("nan")
                else:
                    df[fld.name] = None

        # Cast types
        bool_cols  = [f.name for f in AIRBNB_FACILITY_BRONZE_SCHEMA if f.type == pa.bool_()]
        int_cols   = [f.name for f in AIRBNB_FACILITY_BRONZE_SCHEMA if f.type == pa.int64()]
        float_cols = [f.name for f in AIRBNB_FACILITY_BRONZE_SCHEMA if f.type == pa.float64()]
        str_cols   = [f.name for f in AIRBNB_FACILITY_BRONZE_SCHEMA if f.type == pa.string()]

        for col in bool_cols:
            df[col] = df[col].fillna(False).astype(bool)
        for col in int_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        for col in float_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
        for col in str_cols:
            df[col] = df[col].where(df[col].notna(), other=None).astype(object)

        return df

    @staticmethod
    def _build_view_sql(glob_pattern: str) -> str:
        try:
            probe = duckdb.connect()
            rows  = probe.execute(
                "SELECT column_name FROM ("
                f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}', "
                "hive_partitioning=true, union_by_name=true))"
            ).fetchall()
            probe.close()
            actual = {r[0] for r in rows}
        except Exception:
            actual = set()

        schema_names = {fld.name for fld in AIRBNB_FACILITY_BRONZE_SCHEMA}
        col_exprs: list[str] = []
        for fld in AIRBNB_FACILITY_BRONZE_SCHEMA:
            if fld.name in actual:
                col_exprs.append(fld.name)
            else:
                sql_type = _TYPE_MAP.get(fld.type, "VARCHAR")
                col_exprs.append(f"NULL::{sql_type} AS {fld.name}")
        for col in sorted(actual - schema_names):
            col_exprs.append(col)

        select_clause = ",\n        ".join(col_exprs)
        return (
            f"CREATE VIEW {_VIEW_NAME} AS\n"
            f"    SELECT {select_clause}\n"
            f"    FROM read_parquet(\n"
            f"        '{glob_pattern}',\n"
            f"        hive_partitioning = true,\n"
            f"        filename         = true,\n"
            f"        union_by_name    = true\n"
            f"    )"
        )
