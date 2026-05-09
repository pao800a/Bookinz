"""AirBnB bronze layer storage — writes raw AirBnB scrape results to
hive-partitioned Parquet files.

Directory layout::

    data/airbnb/
    └── bronze/
        └── accommodations/
            └── search_area=Tirana__Albania/
                └── scrape_date=2026-09-20/
                    └── run_<scraped_at_ts>.parquet

The view registered by :meth:`AirbnbBronzeLayer.connection` is named
``airbnb_bronze`` (not ``bronze``) to allow both layers to coexist in the
same DuckDB session.
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

AIRBNB_BRONZE_SCHEMA = pa.schema(
    [
        pa.field("facility_id",          pa.string()),
        pa.field("name",                 pa.string()),
        pa.field("url",                  pa.string()),
        pa.field("search_area",          pa.string()),
        pa.field("checkin_date",         pa.string()),
        pa.field("checkout_date",        pa.string()),
        pa.field("scraped_at",           pa.string()),
        pa.field("num_adults",           pa.int64()),
        pa.field("description",          pa.string()),
        pa.field("accommodation_type",   pa.string()),
        pa.field("neighbourhood",        pa.string()),
        pa.field("num_bedrooms",         pa.int64()),
        pa.field("num_beds",             pa.int64()),
        pa.field("host_type",            pa.string()),
        pa.field("total_price",          pa.float64()),
        pa.field("currency",             pa.string()),
        pa.field("price_is_per_night",   pa.bool_()),
        pa.field("rating",               pa.float64()),   # 0–5 scale (raw AirBnB)
        pa.field("num_reviews",          pa.int64()),
        pa.field("is_superhost",         pa.bool_()),
        pa.field("is_free_cancellation", pa.bool_()),
        pa.field("tags",                 pa.string()),
        pa.field("latitude",             pa.float64()),
        pa.field("longitude",            pa.float64()),
        pa.field("is_available",         pa.bool_()),
        pa.field("raw_html_snippet",     pa.string()),
    ]
)


# ---------------------------------------------------------------------------
# Bronze layer writer
# ---------------------------------------------------------------------------

def _sanitize_partition_value(value: str) -> str:
    """Replace characters that are invalid in file-system partition paths."""
    return re.sub(r"[^\w\-.]", "_", value)


class AirbnbBronzeLayer:
    """Manages the AirBnB bronze (raw) data layer.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
        AirBnB data is stored under ``<base_path>/airbnb/bronze/accommodations/``.
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str  = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path   = base_path
        self.bronze_root = self.base_path / "airbnb" / "bronze" / "accommodations"
        self.bronze_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, records: Sequence[dict], scraped_at: str) -> Path:
        """Persist *records* to the AirBnB bronze layer.

        Parameters
        ----------
        records:
            List of raw accommodation dicts produced by
            :meth:`~bookinz.scraper.airbnb_scraper.AirbnbScraper.scrape_as_dicts`.
        scraped_at:
            ISO-8601 datetime string used for the file name and ``scrape_date``
            partition column.

        Returns
        -------
        Path
            The path of the written Parquet file.
        """
        if not records:
            logger.warning("write() called with empty records — nothing written.")
            raise ValueError("records must be non-empty")

        df = pd.DataFrame(records)
        df = self._coerce_schema(df)

        search_area = _sanitize_partition_value(str(df["search_area"].iloc[0]))
        scrape_date = scraped_at[:10]

        partition_dir = (
            self.bronze_root
            / f"search_area={search_area}"
            / f"scrape_date={scrape_date}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        ts_safe    = scraped_at.replace(":", "-")
        file_path  = partition_dir / f"run_{ts_safe}.parquet"

        table = pa.Table.from_pandas(df, schema=AIRBNB_BRONZE_SCHEMA, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy")

        logger.info("AirBnB bronze write: %d records → %s", len(records), file_path)
        return file_path

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the AirBnB bronze layer (table alias: ``airbnb_bronze``).

        Example
        -------
        >>> abl = AirbnbBronzeLayer("data")
        >>> df  = abl.query("SELECT * FROM airbnb_bronze WHERE search_area = 'Tirana__Albania'")
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        result: pd.DataFrame = con.execute(sql).df()
        con.close()
        return result

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return an open DuckDB connection with ``airbnb_bronze`` view pre-registered.

        The caller is responsible for closing the connection.
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        return con

    @staticmethod
    def _build_view_sql(glob_pattern: str) -> str:
        """Return a ``CREATE VIEW airbnb_bronze`` statement with schema-evolution support.

        Absent columns (added after older files were written) are projected as
        ``NULL::<TYPE>`` so queries always see the full current schema.
        """
        _TYPE_MAP: dict = {
            pa.string():  "VARCHAR",
            pa.float64(): "DOUBLE",
            pa.int64():   "BIGINT",
            pa.bool_():   "BOOLEAN",
        }
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

        schema_names = {field.name for field in AIRBNB_BRONZE_SCHEMA}
        col_exprs    = []
        for fld in AIRBNB_BRONZE_SCHEMA:
            if fld.name in actual:
                col_exprs.append(fld.name)
            else:
                sql_type = _TYPE_MAP.get(fld.type, "VARCHAR")
                col_exprs.append(f"NULL::{sql_type} AS {fld.name}")
        for col in sorted(actual - schema_names):
            col_exprs.append(col)

        select_clause = ",\n        ".join(col_exprs)
        return (
            "CREATE VIEW airbnb_bronze AS\n"
            f"    SELECT {select_clause}\n"
            f"    FROM read_parquet(\n"
            f"        '{glob_pattern}',\n"
            f"        hive_partitioning = true,\n"
            f"        filename         = true,\n"
            f"        union_by_name    = true\n"
            f"    )"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_schema(df: pd.DataFrame) -> pd.DataFrame:
        """Cast DataFrame columns to expected types, adding missing columns as None."""
        expected = [fld.name for fld in AIRBNB_BRONZE_SCHEMA]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        df = df[expected]

        df["total_price"]   = pd.to_numeric(df["total_price"],   errors="coerce")
        df["rating"]        = pd.to_numeric(df["rating"],         errors="coerce")
        df["latitude"]      = pd.to_numeric(df["latitude"],       errors="coerce")
        df["longitude"]     = pd.to_numeric(df["longitude"],      errors="coerce")
        df["num_adults"]    = pd.to_numeric(df["num_adults"],     errors="coerce").astype("Int64")
        df["num_reviews"]   = pd.to_numeric(df["num_reviews"],    errors="coerce").astype("Int64")
        df["num_bedrooms"]  = pd.to_numeric(df["num_bedrooms"],   errors="coerce").astype("Int64")
        df["num_beds"]      = pd.to_numeric(df["num_beds"],       errors="coerce").astype("Int64")

        for bool_col in ("is_available", "is_superhost", "is_free_cancellation", "price_is_per_night"):
            df[bool_col] = df[bool_col].astype(bool)

        for str_col in (
            "facility_id", "name", "url", "search_area", "checkin_date", "checkout_date",
            "scraped_at", "description", "accommodation_type", "neighbourhood",
            "host_type", "currency", "tags", "raw_html_snippet",
        ):
            df[str_col] = df[str_col].where(pd.notna(df[str_col]), other=None)

        return df
