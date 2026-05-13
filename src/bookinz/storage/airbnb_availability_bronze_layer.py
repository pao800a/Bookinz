"""AirBnB availability bronze layer — writes calendar availability records to
hive-partitioned Parquet files.

Directory layout::

    data/airbnb/
    └── bronze/
        └── availability/
            └── facility_id=<id>/
                └── scrape_date=YYYY-MM-DD/
                    └── run_<scraped_at_ts>.parquet

The DuckDB view registered by :meth:`AirbnbAvailabilityBronzeLayer.connection`
is named ``airbnb_availability_bronze`` — fully independent from all other
dataset views.
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

AIRBNB_AVAILABILITY_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("facility_id",  pa.string()),
        pa.field("date",         pa.string()),   # ISO-8601 date
        pa.field("is_available", pa.bool_()),
        pa.field("min_nights",   pa.int64()),
        pa.field("scraped_at",   pa.string()),
    ]
)

_TYPE_MAP: dict = {
    pa.string():  "VARCHAR",
    pa.float64(): "DOUBLE",
    pa.int64():   "BIGINT",
    pa.bool_():   "BOOLEAN",
}

_VIEW_NAME = "airbnb_availability_bronze"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sanitize(value: str) -> str:
    return re.sub(r"[^\w\-.]", "_", value)


# ---------------------------------------------------------------------------
# Bronze layer writer
# ---------------------------------------------------------------------------

class AirbnbAvailabilityBronzeLayer:
    """Manages the AirBnB availability bronze data layer.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
        Availability data is stored under
        ``<base_path>/airbnb/bronze/availability/``.
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str  = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path   = base_path
        self.bronze_root = self.base_path / "bronze" / "airbnb" / "availability"
        self.bronze_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, records: Sequence[dict], scraped_at: str) -> list[Path]:
        """Persist *records* to the availability bronze layer.

        Each unique ``facility_id`` gets its own hive partition so that
        existing data for other listings is never overwritten.

        Parameters
        ----------
        records:
            List of dicts produced by
            :meth:`~bookinz.scraper.airbnb_availability_scraper.AirbnbAvailabilityScraper.scrape_as_dicts`.
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
                group_df, schema=AIRBNB_AVAILABILITY_BRONZE_SCHEMA, preserve_index=False
            )
            pq.write_table(table, file_path, compression="snappy")
            logger.info(
                "AirBnB availability bronze write: %d record(s) → %s",
                len(group_df),
                file_path,
            )
            written.append(file_path)

        return written

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return an open DuckDB connection with ``airbnb_availability_bronze`` pre-registered.

        The caller is responsible for closing the connection.
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        return con

    def already_scraped(self, facility_id: str, scrape_date: str) -> bool:
        """Return True if availability data for *(facility_id, scrape_date)* exists."""
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
        for fld in AIRBNB_AVAILABILITY_BRONZE_SCHEMA:
            if fld.name not in df.columns:
                if fld.type == pa.bool_():
                    df[fld.name] = False
                elif fld.type == pa.int64():
                    df[fld.name] = pd.NA
                else:
                    df[fld.name] = None

        df["is_available"] = df["is_available"].fillna(False).astype(bool)
        df["min_nights"]   = pd.to_numeric(df["min_nights"], errors="coerce").astype("Int64")
        for col in ["facility_id", "date", "scraped_at"]:
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

        schema_names = {fld.name for fld in AIRBNB_AVAILABILITY_BRONZE_SCHEMA}
        col_exprs: list[str] = []
        for fld in AIRBNB_AVAILABILITY_BRONZE_SCHEMA:
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
