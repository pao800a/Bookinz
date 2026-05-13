"""AirBnB silver layer — promotes the AirBnB bronze dataset to a cleaned,
deduplicated Parquet file with the same column schema as the Booking.com silver.

Key transformations applied on top of the SQL transform:

- AirBnB ``rating`` (0–5) is doubled to 0–10 in the SQL so that
  ``quality_price_score`` is comparable with Booking.com records.
- ``distance_from_center_km`` is computed from the ``latitude``/``longitude``
  bronze fields using ``geopy`` (Nominatim geocoder for city-centre lookup,
  geodesic distance for each listing).  Coordinates are then dropped so the
  final Parquet matches the shared silver schema exactly.

Directory layout::

    data/airbnb/
    └── silver/
        └── accommodations/
            └── silver_<timestamp>.parquet

Usage (Python API)::

    from bookinz.storage.airbnb_silver_layer import AirbnbSilverLayer

    sl = AirbnbSilverLayer("data")
    output_path = sl.push()

Usage (CLI)::

    python -m bookinz.storage.airbnb_silver_layer
    python -m bookinz.storage.airbnb_silver_layer --data-path /path/to/data
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from bookinz.storage.airbnb_accommodation_bronze_layer import AirbnbAccommodationBronzeLayer
from bookinz.storage.airbnb_facility_bronze_layer import AirbnbFacilityBronzeLayer
from bookinz.storage.booking_silver_layer import SILVER_SCHEMA

logger = logging.getLogger(__name__)

_SQL_FILE = Path(__file__).parent / "sql" / "airbnb_silver_layer.sql"

# The AirBnB silver schema is identical to the Booking.com silver schema so
# that both tables can be unioned without column-alignment work in the gold layer.
AIRBNB_SILVER_SCHEMA = SILVER_SCHEMA


# ---------------------------------------------------------------------------
# Geocoding helpers
# ---------------------------------------------------------------------------

def _geocode_city_centre(city: str, country: str, user_agent: str = "bookinz/1.0") -> tuple[float, float] | None:
    """Return ``(lat, lon)`` for the centre of *city*, *country* via Nominatim.

    Returns ``None`` when the city cannot be geocoded (network error, not found).
    Nominatim requires a unique ``user_agent`` string and enforces 1 req/sec.
    """
    try:
        from geopy.geocoders import Nominatim  # noqa: PLC0415
    except ImportError:
        logger.warning("geopy is not installed. distance_from_center_km will be NULL.")
        return None

    geolocator = Nominatim(user_agent=user_agent)
    query = f"{city}, {country}".replace("_", " ")
    try:
        location = geolocator.geocode(query, timeout=10)
        if location:
            return float(location.latitude), float(location.longitude)
    except Exception as exc:  # noqa: BLE001
        logger.warning("Geocoding failed for '%s, %s': %s", city, country, exc)
    return None


def _geodesic_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float | None:
    """Return the geodesic distance in km between two (lat, lon) points."""
    try:
        from geopy.distance import geodesic  # noqa: PLC0415
        return geodesic((lat1, lon1), (lat2, lon2)).km
    except Exception:  # noqa: BLE001
        return None


def _compute_distances(
    table: pa.Table,
    geocode_delay_s: float = 1.1,
) -> pa.Table:
    """Add / fill the ``distance_from_center_km`` column using geopy.

    One Nominatim call is made per unique (city, country) pair; results are
    cached for the duration of this call.  A ``geocode_delay_s`` sleep is
    inserted between geocoding requests to respect Nominatim's rate limit.

    The ``latitude`` and ``longitude`` columns are **dropped** from the
    returned table (they are bronze-only fields).
    """
    import pandas as pd  # local import

    df = table.to_pandas()

    # Build city-centre cache
    city_centre_cache: dict[tuple[str, str], tuple[float, float] | None] = {}
    unique_cities = df[["city", "country"]].drop_duplicates().values.tolist()
    for city, country in unique_cities:
        key = (str(city), str(country))
        if key not in city_centre_cache:
            if city_centre_cache:              # don't sleep before the first request
                time.sleep(geocode_delay_s)
            city_centre_cache[key] = _geocode_city_centre(city, country)
            logger.debug("Geocoded '%s, %s' → %s", city, country, city_centre_cache[key])

    # Compute per-row distance
    def _row_distance(row: pd.Series) -> float | None:
        centre = city_centre_cache.get((str(row["city"]), str(row["country"])))
        if centre is None:
            return None
        lat = row.get("latitude")
        lon = row.get("longitude")
        if lat is None or lon is None or pd.isna(lat) or pd.isna(lon):
            return None
        return _geodesic_km(centre[0], centre[1], float(lat), float(lon))

    df["distance_from_center_km"] = df.apply(_row_distance, axis=1)

    # Drop bronze-only coordinate columns
    df = df.drop(columns=["latitude", "longitude"], errors="ignore")

    return pa.Table.from_pandas(df, preserve_index=False)


# ---------------------------------------------------------------------------
# Schema coercion
# ---------------------------------------------------------------------------

def _coerce_airbnb_silver_table(table: pa.Table) -> pa.Table:
    """Cast all columns to match AIRBNB_SILVER_SCHEMA; fill absent ones with null."""
    schema_fields = {f.name: f.type for f in AIRBNB_SILVER_SCHEMA}
    arrays: list[pa.Array] = []
    names:  list[str]      = []
    for name in [f.name for f in AIRBNB_SILVER_SCHEMA]:
        target_type = schema_fields[name]
        if name not in table.schema.names:
            arrays.append(pa.array([None] * table.num_rows, type=target_type))
        else:
            col = table.column(name)
            if col.type != target_type:
                try:
                    col = col.cast(target_type, safe=False)
                except Exception:  # noqa: BLE001
                    col = pa.array([None] * table.num_rows, type=target_type)
            arrays.append(col)
        names.append(name)
    return pa.table(dict(zip(names, arrays)), schema=AIRBNB_SILVER_SCHEMA)


# ---------------------------------------------------------------------------
# AirbnbSilverLayer
# ---------------------------------------------------------------------------

class AirbnbSilverLayer:
    """Promotes the AirBnB bronze layer to a cleaned, deduplicated silver file.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str  = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path   = base_path
        self.silver_root = self.base_path / "silver" / "airbnb" / "accommodations"
        self.silver_root.mkdir(parents=True, exist_ok=True)
        self._bronze   = AirbnbAccommodationBronzeLayer(base_path)
        self._facility = AirbnbFacilityBronzeLayer(base_path)

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

    def push(self, timestamp: str | None = None, geocode_delay_s: float = 1.1) -> Path:
        """Run the silver transformation and write the result to a Parquet file.

        Parameters
        ----------
        timestamp:
            ISO-8601 string for the output file name. Defaults to current UTC time.
        geocode_delay_s:
            Sleep between Nominatim geocoding calls to respect the 1 req/sec
            rate limit.

        Returns
        -------
        Path
            The path of the written Parquet file.
        """
        ts  = timestamp or datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        sql = _SQL_FILE.read_text(encoding="utf-8")

        # Open bronze connection and also register the facility view on it
        con: duckdb.DuckDBPyConnection = self._bronze.connection()
        try:
            facility_glob = str(self._facility.bronze_root / "**" / "*.parquet")
            facility_sql  = self._facility._build_view_sql(facility_glob)
            con.execute(facility_sql)

            # Geocode all unique (city, country) pairs from the accommodation bronze
            city_rows = con.execute(
                "SELECT DISTINCT "
                "split_part(search_area, '__', 1) AS city, "
                "split_part(search_area, '__', 2) AS country "
                "FROM airbnb_accommodation_bronze"
            ).fetchall()

            city_centre_rows: list[tuple[str, str, float | None, float | None]] = []
            first = True
            for city, country in city_rows:
                if not first:
                    time.sleep(geocode_delay_s)
                first = False
                coords = _geocode_city_centre(str(city), str(country))
                lat = coords[0] if coords else None
                lon = coords[1] if coords else None
                city_centre_rows.append((str(city), str(country), lat, lon))
                logger.debug("Geocoded '%s, %s' → %s", city, country, coords)

            # Inject city centres as an in-memory DuckDB table so the SQL can JOIN it
            centres_df = pd.DataFrame(
                city_centre_rows,
                columns=["city", "country", "centre_lat", "centre_lon"],
            )
            con.register("city_centres", centres_df)

            table = con.execute(sql).fetch_arrow_table()
        finally:
            con.close()

        if table.num_rows == 0:
            logger.warning("AirBnB silver push: query returned 0 rows — nothing written.")
            raise ValueError("AirBnB silver query returned no rows; bronze layer may be empty.")

        # Align to shared silver schema (no post-SQL geocoding needed — distance computed in SQL)
        table = _coerce_airbnb_silver_table(table)

        out_path = self.silver_root / f"silver_{ts}.parquet"
        pq.write_table(table, out_path, compression="snappy")

        logger.info("AirBnB silver push: %d rows → %s", table.num_rows, out_path)
        return out_path

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )

    parser = argparse.ArgumentParser(
        description="Push AirBnB bronze data to the silver layer.",
    )
    parser.add_argument(
        "--data-path",
        default="data",
        metavar="PATH",
        help="Root of the data lake (default: data).",
    )
    args = parser.parse_args(argv)

    sl  = AirbnbSilverLayer(args.data_path)
    out = sl.push()
    print(f"AirBnB silver file written: {out}")


if __name__ == "__main__":
    _main()
