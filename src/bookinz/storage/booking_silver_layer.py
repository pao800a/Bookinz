"""Silver layer storage — promotes the bronze dataset to a cleaned, deduplicated
Parquet file using the transformation defined in ``silver_layer.sql``.

Directory layout::

    data/
    └── silver/
        └── accommodations/
            └── silver_<timestamp>.parquet

Usage (Python API)::

    from bookinz.storage.silver_layer import SilverLayer

    sl = SilverLayer("data")
    output_path = sl.push()
    print(f"Silver file written to: {output_path}")

Usage (CLI)::

    python -m bookinz.storage.silver_layer
    python -m bookinz.storage.silver_layer --data-path /path/to/data

"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from bookinz.storage.booking_bronze_layer import BookingBronzeLayer

logger = logging.getLogger(__name__)

_SQL_FILE = Path(__file__).parent / "sql" / "booking_silver_layer.sql"

SILVER_SCHEMA = pa.schema(
    [
        pa.field("facility_id", pa.string()),
        pa.field("url", pa.string()),
        pa.field("name", pa.string()),
        pa.field("accommodation_type", pa.string()),
        pa.field("city", pa.string()),
        pa.field("country", pa.string()),
        pa.field("neighbourhood", pa.string()),
        pa.field("distance_from_center_km", pa.float64()),
        pa.field("checkin_date", pa.string()),
        pa.field("checkout_date", pa.string()),
        pa.field("num_nights", pa.int64()),
        pa.field("num_adults", pa.int64()),
        pa.field("currency", pa.string()),
        pa.field("total_price", pa.float64()),
        pa.field("price_per_night", pa.float64()),
        pa.field("price_per_adult", pa.float64()),
        pa.field("price_per_adult_per_night", pa.float64()),
        pa.field("rating", pa.float64()),
        pa.field("quality_price_score", pa.float64()),
        pa.field("num_reviews", pa.int64()),
        pa.field("num_rooms_available", pa.int64()),
        pa.field("is_available", pa.bool_()),
        pa.field("_pk", pa.string()),
        pa.field("_source", pa.string()),
    ]
)


class BookingSilverLayer:
    """Promotes the bronze layer to a cleaned, deduplicated silver Parquet file.

    Parameters
    ----------
    base_path:
        Root of the data lake (e.g. ``Path("data")``).  Must be the same
        ``base_path`` used by :class:`~bookinz.storage.bronze_layer.BronzeLayer`.
    """

    def __init__(self, base_path: str | Path = "data") -> None:
        base_path = Path(base_path).resolve()
        path_str = str(base_path)
        if "\x00" in path_str or "'" in path_str:
            raise ValueError(f"base_path contains invalid characters: {base_path!r}")
        self.base_path = base_path
        self.silver_root = self.base_path / "silver" / "booking" / "accommodations"
        self.silver_root.mkdir(parents=True, exist_ok=True)
        self._bronze = BookingBronzeLayer(base_path)

    # ------------------------------------------------------------------
    # Push
    # ------------------------------------------------------------------

    def push(self, timestamp: str | None = None) -> Path:
        """Run the silver transformation and write the result to a Parquet file.

        Parameters
        ----------
        timestamp:
            ISO-8601 string used for the output file name.  Defaults to the
            current UTC time.

        Returns
        -------
        Path
            The path of the written Parquet file.
        """
        ts = timestamp or datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")
        sql = _SQL_FILE.read_text(encoding="utf-8")

        con: duckdb.DuckDBPyConnection = self._bronze.connection()
        try:
            table = con.execute(sql).fetch_arrow_table()
        finally:
            con.close()

        if table.num_rows == 0:
            logger.warning("Silver push: query returned 0 rows — nothing written.")
            raise ValueError("Silver query returned no rows; bronze layer may be empty.")

        # Align to schema — cast integer columns that DuckDB may return as int32
        table = _coerce_silver_table(table)

        out_path = self.silver_root / f"silver_{ts}.parquet"
        pq.write_table(table, out_path, compression="snappy")

        logger.info(
            "Silver push: %d rows → %s",
            table.num_rows,
            out_path,
        )
        return out_path

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _coerce_silver_table(table: pa.Table) -> pa.Table:
    """Cast columns to match SILVER_SCHEMA types, ignoring unknown columns."""
    schema_fields = {f.name: f.type for f in SILVER_SCHEMA}
    arrays = []
    names = []
    for name in [f.name for f in SILVER_SCHEMA]:
        if name not in table.schema.names:
            # Fill absent columns with null
            target_type = schema_fields[name]
            arrays.append(pa.array([None] * table.num_rows, type=target_type))
        else:
            col = table.column(name)
            target_type = schema_fields[name]
            if col.type != target_type:
                col = col.cast(target_type, safe=False)
            arrays.append(col)
        names.append(name)
    return pa.table(dict(zip(names, arrays)), schema=SILVER_SCHEMA)


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
        description="Push bronze data to the silver layer.",
    )
    parser.add_argument(
        "--data-path",
        default="data",
        metavar="PATH",
        help="Root of the data lake (default: data).",
    )
    args = parser.parse_args(argv)

    sl = BookingSilverLayer(args.data_path)
    out = sl.push()
    print(f"Silver file written: {out}")


if __name__ == "__main__":
    _main()
