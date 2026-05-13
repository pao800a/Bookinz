"""Unified DataLake query interface.

Registers all datasets as DuckDB 3-part identifiers so that SQL can reference
them naturally::

    lake = DataLake("data")
    df = lake.query("SELECT * FROM bronze.airbnb.facility LIMIT 10")
    df = lake.query("SELECT * FROM silver.booking.accommodations LIMIT 10")

Directory layout expected on disk (new canonical structure)::

    data/
    ├── bronze/
    │   ├── airbnb/
    │   │   ├── accommodations/   ← AirbnbBronzeLayer
    │   │   ├── availability/     ← AirbnbAvailabilityBronzeLayer
    │   │   └── facility/         ← AirbnbFacilityBronzeLayer
    │   └── booking/
    │       └── accommodations/   ← BookingBronzeLayer
    └── silver/
        ├── airbnb/
        │   └── accommodations/   ← AirbnbSilverLayer
        └── booking/
            └── accommodations/   ← BookingSilverLayer

Each dataset that has no files on disk is silently skipped — no error is
raised, but querying it will produce a DuckDB "table not found" error at
runtime.
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Dataset registry
# ---------------------------------------------------------------------------

# (catalog, schema, view, relative_glob)
_DATASETS: list[tuple[str, str, str, str]] = [
    ("bronze", "airbnb",   "accommodations", "bronze/airbnb/accommodations/**/*.parquet"),
    ("bronze", "airbnb",   "availability",   "bronze/airbnb/availability/**/*.parquet"),
    ("bronze", "airbnb",   "facility",       "bronze/airbnb/facility/**/*.parquet"),
    ("bronze", "booking",  "accommodations", "bronze/booking/accommodations/**/*.parquet"),
    ("silver", "airbnb",   "accommodations", "silver/airbnb/accommodations/**/*.parquet"),
    ("silver", "booking",  "accommodations", "silver/booking/accommodations/**/*.parquet"),
]


# ---------------------------------------------------------------------------
# DataLake
# ---------------------------------------------------------------------------

class DataLake:
    """Unified query interface over the full data lake.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path = base_path

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def query(self, sql: str) -> pd.DataFrame:
        """Execute *sql* against the data lake and return a DataFrame.

        All registered datasets are available as 3-part names:
        ``<catalog>.<schema>.<view>`` (e.g. ``bronze.airbnb.facility``).

        Datasets whose directory does not yet exist on disk are silently skipped
        and will raise a DuckDB "table not found" error only if referenced.

        Example
        -------
        >>> lake = DataLake("data")
        >>> df = lake.query("SELECT * FROM bronze.airbnb.facility LIMIT 10")
        >>> df = lake.query("SELECT * FROM silver.booking.accommodations LIMIT 10")
        """
        con = self._build_connection()
        try:
            result: pd.DataFrame = con.execute(sql).df()
        finally:
            con.close()
        return result

    def connection(self) -> duckdb.DuckDBPyConnection:
        """Return an open DuckDB connection with all datasets pre-registered.

        The caller is responsible for closing the connection.

        Example
        -------
        >>> lake = DataLake("data")
        >>> con = lake.connection()
        >>> con.execute("SELECT count(*) FROM bronze.airbnb.accommodations").fetchone()
        >>> con.close()
        """
        return self._build_connection()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _build_connection(self) -> duckdb.DuckDBPyConnection:
        """Build and return a DuckDB connection with all views registered."""
        con = duckdb.connect()

        registered_catalogs: set[str] = set()
        registered_schemas:  set[tuple[str, str]] = set()

        for catalog, schema, view, rel_glob in _DATASETS:
            glob_path = self.base_path / rel_glob

            # Check that at least one parquet file matching the glob exists
            if not any(self.base_path.glob(rel_glob)):
                logger.debug("Skipping %s.%s.%s — no parquet files found: %s", catalog, schema, view, glob_path)
                continue

            # Attach catalog (in-memory database aliased as <catalog>)
            if catalog not in registered_catalogs:
                con.execute(f"ATTACH ':memory:' AS {catalog}")
                registered_catalogs.add(catalog)

            # Create schema inside catalog
            if (catalog, schema) not in registered_schemas:
                con.execute(f"CREATE SCHEMA {catalog}.{schema}")
                registered_schemas.add((catalog, schema))

            # Escape the glob path for use in SQL string literal (forward slashes)
            glob_str = str(glob_path).replace("\\", "/").replace("'", "''")

            con.execute(
                f"CREATE VIEW {catalog}.{schema}.{view} AS "
                f"SELECT * FROM read_parquet('{glob_str}', "
                f"hive_partitioning = true, union_by_name = true)"
            )
            logger.debug("Registered view %s.%s.%s → %s", catalog, schema, view, glob_str)

        return con
