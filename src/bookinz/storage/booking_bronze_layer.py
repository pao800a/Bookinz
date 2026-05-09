"""Bronze layer storage — writes raw scrape results to partitioned Parquet files.

Directory layout::

    data/
    └── bronze/
        └── accommodations/
            └── search_area=Amsterdam/
                └── scrape_date=2024-01-15/
                    └── run_<scraped_at_ts>.parquet

Files are readable directly with DuckDB using hive-style partition discovery::

    SELECT *
    FROM read_parquet(
        'data/bronze/accommodations/**/*.parquet',
        hive_partitioning = true
    )
    WHERE search_area = 'Amsterdam';
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

BRONZE_SCHEMA = pa.schema(
    [
        pa.field("facility_id", pa.string()),
        pa.field("name", pa.string()),
        pa.field("url", pa.string()),
        pa.field("search_area", pa.string()),
        pa.field("checkin_date", pa.string()),
        pa.field("checkout_date", pa.string()),
        pa.field("scraped_at", pa.string()),
        pa.field("num_adults", pa.int64()),
        pa.field("total_price", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("rating", pa.float64()),
        pa.field("rating_category", pa.string()),
        pa.field("num_reviews", pa.int64()),
        pa.field("distance_from_center_km", pa.float64()),
        pa.field("num_rooms_available", pa.int64()),
        pa.field("neighbourhood", pa.string()),
        pa.field("accommodation_type", pa.string()),
        pa.field("tags", pa.string()),
        pa.field("is_available", pa.bool_()),
        pa.field("raw_html_snippet", pa.string()),
    ]
)

# ---------------------------------------------------------------------------
# Bronze layer writer
# ---------------------------------------------------------------------------


def _sanitize_partition_value(value: str) -> str:
    """Replace characters that are invalid in file-system partition directories."""
    return re.sub(r"[^\w\-.]", "_", value)


class BookingBronzeLayer:
    """Manages the bronze (raw) data layer.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        # Reject paths that contain null bytes or single-quotes.
        # Single-quotes are the only character that could break the SQL string
        # literal used in the CREATE VIEW statement below, since DuckDB does not
        # support parameterized file-path arguments to read_parquet().
        path_str = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path = base_path
        self.bronze_root = self.base_path / "bronze" / "accommodations"
        self.bronze_root.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def write(self, records: Sequence[dict], scraped_at: str) -> Path:
        """Persist *records* to the bronze layer.

        Parameters
        ----------
        records:
            List of raw accommodation dicts (as produced by
            :meth:`~bookinz.scraper.BookingComScraper.scrape_as_dicts`).
        scraped_at:
            ISO-8601 datetime string used both for the file name and to derive
            the ``scrape_date`` partition column.

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

        # Partition key values
        search_area = _sanitize_partition_value(str(df["search_area"].iloc[0]))
        scrape_date = scraped_at[:10]  # YYYY-MM-DD

        partition_dir = (
            self.bronze_root
            / f"search_area={search_area}"
            / f"scrape_date={scrape_date}"
        )
        partition_dir.mkdir(parents=True, exist_ok=True)

        # File name: replace colons so it is valid on Windows/macOS too
        ts_safe = scraped_at.replace(":", "-")
        file_path = partition_dir / f"run_{ts_safe}.parquet"

        table = pa.Table.from_pandas(df, schema=BRONZE_SCHEMA, preserve_index=False)
        pq.write_table(table, file_path, compression="snappy")

        logger.info(
            "Bronze write: %d records → %s",
            len(records),
            file_path,
        )
        return file_path

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the bronze layer with DuckDB and return a DataFrame.

        The table name ``bronze`` is pre-registered and maps to all Parquet
        files under :attr:`bronze_root` with hive-style partition discovery.

        Example
        -------
        >>> bl = BronzeLayer("data")
        >>> df = bl.query("SELECT * FROM booking_bronze WHERE search_area = 'Amsterdam'")
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        result: pd.DataFrame = con.execute(sql).df()
        con.close()
        return result

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return an open DuckDB connection with the ``bronze`` view pre-registered.

        The caller is responsible for closing the connection.

        Example
        -------
        >>> bl = BronzeLayer("data")
        >>> con = bl.connection()
        >>> con.execute("SELECT DISTINCT search_area FROM booking_bronze").fetchall()
        >>> con.close()
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(self._build_view_sql(glob_pattern))
        return con

    @staticmethod
    def _build_view_sql(glob_pattern: str) -> str:
        """Return a CREATE VIEW statement that handles schema evolution.

        Columns present in the Parquet files are projected directly.
        Any column in :data:`BRONZE_SCHEMA` that is absent from the files
        (e.g. added after those files were written) is projected as
        ``NULL::TYPE``, so queries always see the full current schema.
        ``union_by_name=true`` is passed to ``read_parquet`` so that files
        written at different schema versions are merged correctly.
        """
        _TYPE_MAP: dict = {
            pa.string(): "VARCHAR",
            pa.float64(): "DOUBLE",
            pa.int64(): "BIGINT",
            pa.bool_(): "BOOLEAN",
        }
        try:
            probe = duckdb.connect()
            rows = probe.execute(
                "SELECT column_name FROM ("
                f"DESCRIBE SELECT * FROM read_parquet('{glob_pattern}', "
                "hive_partitioning=true, union_by_name=true))"
            ).fetchall()
            probe.close()
            actual = {r[0] for r in rows}
        except Exception:  # no files yet or probe error
            actual = set()

        schema_names = {field.name for field in BRONZE_SCHEMA}
        col_exprs = []
        for field in BRONZE_SCHEMA:
            if field.name in actual:
                col_exprs.append(field.name)
            else:
                sql_type = _TYPE_MAP.get(field.type, "VARCHAR")
                col_exprs.append(f"NULL::{sql_type} AS {field.name}")
        # Also forward any extra columns from the files (e.g. hive partition
        # columns like scrape_date, or the filename metadata column) that are
        # not part of BRONZE_SCHEMA.
        for col in sorted(actual - schema_names):
            col_exprs.append(col)

        select_clause = ",\n        ".join(col_exprs)
        return (
            "CREATE VIEW booking_bronze AS\n"
            f"    SELECT {select_clause}\n"
            f"    FROM read_parquet(\n"
            f"        '{glob_pattern}',\n"
            f"        hive_partitioning = true,\n"
            f"        filename = true,\n"
            f"        union_by_name = true\n"
            f"    )"
        )

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    @staticmethod
    def _coerce_schema(df: pd.DataFrame) -> pd.DataFrame:
        """Cast DataFrame columns to the expected types, adding missing columns."""
        expected_columns = [field.name for field in BRONZE_SCHEMA]
        for col in expected_columns:
            if col not in df.columns:
                df[col] = None
        # Reorder to match schema
        df = df[expected_columns]
        # Type coercions
        df["total_price"] = pd.to_numeric(df["total_price"], errors="coerce")
        df["num_adults"] = pd.to_numeric(df["num_adults"], errors="coerce").astype("Int64")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["num_reviews"] = pd.to_numeric(df["num_reviews"], errors="coerce").astype("Int64")
        df["num_rooms_available"] = pd.to_numeric(
            df["num_rooms_available"], errors="coerce"
        ).astype("Int64")
        df["is_available"] = df["is_available"].astype(bool)
        # String columns
        for col in ["facility_id", "name", "url", "search_area", "checkin_date",
                    "checkout_date", "scraped_at", "currency", "rating_category",
                    "neighbourhood", "accommodation_type", "tags",
                    "raw_html_snippet"]:
            df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        return df
