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
        pa.field("price_per_night", pa.float64()),
        pa.field("currency", pa.string()),
        pa.field("rating", pa.float64()),
        pa.field("rating_category", pa.string()),
        pa.field("num_reviews", pa.int64()),
        pa.field("distance_from_center_km", pa.float64()),
        pa.field("num_rooms_available", pa.int64()),
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


class BronzeLayer:
    """Manages the bronze (raw) data layer.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        self.base_path = Path(base_path)
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
        >>> df = bl.query("SELECT * FROM bronze WHERE search_area = 'Amsterdam'")
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(
            f"""
            CREATE VIEW bronze AS
            SELECT *
            FROM read_parquet(
                '{glob_pattern}',
                hive_partitioning = true,
                filename = true
            )
            """
        )
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
        >>> con.execute("SELECT DISTINCT search_area FROM bronze").fetchall()
        >>> con.close()
        """
        glob_pattern = str(self.bronze_root / "**" / "*.parquet")
        con = duckdb.connect()
        con.execute(
            f"""
            CREATE VIEW bronze AS
            SELECT *
            FROM read_parquet(
                '{glob_pattern}',
                hive_partitioning = true,
                filename = true
            )
            """
        )
        return con

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
        df["price_per_night"] = pd.to_numeric(df["price_per_night"], errors="coerce")
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        df["num_reviews"] = pd.to_numeric(df["num_reviews"], errors="coerce").astype("Int64")
        df["num_rooms_available"] = pd.to_numeric(
            df["num_rooms_available"], errors="coerce"
        ).astype("Int64")
        df["is_available"] = df["is_available"].astype(bool)
        # String columns
        for col in ["facility_id", "name", "url", "search_area", "checkin_date",
                    "checkout_date", "scraped_at", "currency", "rating_category",
                    "raw_html_snippet"]:
            df[col] = df[col].astype(str).where(df[col].notna(), other=None)
        return df
