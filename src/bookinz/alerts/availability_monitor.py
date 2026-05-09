"""Availability monitor — tracks unavailable facilities and fires alerts.

The monitor reads the bronze layer to discover facilities that were previously
marked as unavailable (``is_available = false``) and checks whether any of
them have become available in the most recent scrape.  When a recovery is
detected an :class:`AvailabilityAlert` is emitted.

Usage example::

    from bookinz.alerts.availability_monitor import AvailabilityMonitor
    from bookinz.storage import BronzeLayer

    bl = BronzeLayer("data")
    monitor = AvailabilityMonitor(bl)
    alerts = monitor.check(search_area="Amsterdam", latest_scraped_at="2024-01-15T08:00:00")
    for alert in alerts:
        print(alert)
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

from bookinz.storage.booking_bronze_layer import BookingBronzeLayer

logger = logging.getLogger(__name__)

# Allowed characters for search_area and ISO-8601 scraped_at timestamps.
_AREA_RE = re.compile(r"^[\w\s\-,.]+$")
_TIMESTAMP_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")


def _validate_area(value: str) -> str:
    """Raise ValueError if *value* contains characters outside the safe set."""
    if not _AREA_RE.match(value):
        raise ValueError(
            f"search_area contains invalid characters: {value!r}. "
            "Only word characters, spaces, hyphens, commas and dots are allowed."
        )
    return value


def _validate_timestamp(value: str) -> str:
    """Raise ValueError if *value* is not a valid ISO-8601 datetime string."""
    if not _TIMESTAMP_RE.match(value):
        raise ValueError(
            f"timestamp must be in YYYY-MM-DDTHH:MM:SS format, got: {value!r}"
        )
    return value


# ---------------------------------------------------------------------------
# Alert data model
# ---------------------------------------------------------------------------

@dataclass
class AvailabilityAlert:
    """Fired when a previously unavailable facility becomes available."""

    facility_id: str
    name: str
    url: str
    search_area: str
    first_unavailable_date: str   # scrape_date of the first unavailability
    recovered_at: str             # scraped_at of the recovery
    price_per_night: float | None
    currency: str | None
    rating: float | None

    def __str__(self) -> str:
        price_str = (
            f"{self.price_per_night} {self.currency}"
            if self.price_per_night is not None
            else "price unknown"
        )
        return (
            f"[ALERT] '{self.name}' (id={self.facility_id}) in {self.search_area} "
            f"is AVAILABLE again as of {self.recovered_at}. "
            f"Price: {price_str}. URL: {self.url}"
        )


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class AvailabilityMonitor:
    """Compares the latest scrape against historical availability data.

    Parameters
    ----------
    bronze_layer:
        An initialised :class:`~bookinz.storage.bronze_layer.BronzeLayer`.
    """

    def __init__(self, bronze_layer: BookingBronzeLayer) -> None:
        self.bronze = bronze_layer

    def check(self, search_area: str, latest_scraped_at: str) -> list[AvailabilityAlert]:
        """Detect facilities that recovered availability in the latest scrape.

        Parameters
        ----------
        search_area:
            The area to inspect (must match ``search_area`` partition value).
        latest_scraped_at:
            The ``scraped_at`` timestamp of the most recent scrape run
            (format: ``YYYY-MM-DDTHH:MM:SS``).

        Returns
        -------
        list[AvailabilityAlert]
            One alert per facility that transitioned from unavailable → available.

        Raises
        ------
        ValueError
            If *search_area* or *latest_scraped_at* contain invalid characters.
        """
        search_area = _validate_area(search_area)
        latest_scraped_at = _validate_timestamp(latest_scraped_at)

        try:
            con = self.bronze.connection()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not open bronze connection: %s", exc)
            return []

        try:
            return self._check_with_connection(con, search_area, latest_scraped_at)
        finally:
            con.close()

    def _check_with_connection(
        self,
        con,  # duckdb.DuckDBPyConnection
        search_area: str,
        latest_scraped_at: str,
    ) -> list[AvailabilityAlert]:
        """Internal: run both queries on the already-open *con*."""
        try:
            previously_unavailable = self._get_previously_unavailable(
                con, search_area, latest_scraped_at
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not query historical availability: %s", exc)
            return []

        if not previously_unavailable:
            logger.info("No previously unavailable facilities found for '%s'.", search_area)
            return []

        # Build positional placeholders for the facility IDs.
        # $1 = search_area, $2 = latest_scraped_at (already used below),
        # so facility IDs start at $3.
        _FACILITY_PARAM_OFFSET = 3
        ids_placeholder = ", ".join(
            f"${i + _FACILITY_PARAM_OFFSET}" for i in range(len(previously_unavailable))
        )

        try:
            now_available_df = con.execute(
                f"""
                SELECT
                    facility_id,
                    name,
                    url,
                    search_area,
                    scraped_at,
                    price_per_night,
                    currency,
                    rating
                FROM booking_bronze
                WHERE search_area = $1
                  AND scraped_at  = $2
                  AND is_available = true
                  AND facility_id IN ({ids_placeholder})
                """,
                [search_area, latest_scraped_at, *previously_unavailable.keys()],
            ).df()
        except Exception as exc:  # noqa: BLE001
            logger.error("Error querying latest availability: %s", exc)
            return []

        alerts: list[AvailabilityAlert] = []
        for _, row in now_available_df.iterrows():
            fid = row["facility_id"]
            alert = AvailabilityAlert(
                facility_id=fid,
                name=row["name"],
                url=row["url"],
                search_area=row["search_area"],
                first_unavailable_date=previously_unavailable[fid],
                recovered_at=row["scraped_at"],
                price_per_night=row.get("price_per_night"),
                currency=row.get("currency"),
                rating=row.get("rating"),
            )
            alerts.append(alert)
            logger.info("%s", alert)

        return alerts

    @staticmethod
    def _get_previously_unavailable(
        con,  # duckdb.DuckDBPyConnection
        search_area: str,
        latest_scraped_at: str,
    ) -> dict[str, str]:
        """Return ``{facility_id: first_unavailable_date}`` for all facilities
        that have **never** been seen as available before the current run.
        """
        df = con.execute(
            """
            WITH history AS (
                SELECT
                    facility_id,
                    scrape_date,
                    MAX(CAST(is_available AS INTEGER)) AS ever_available
                FROM booking_bronze
                WHERE search_area = $1
                  AND scraped_at  < $2
                GROUP BY facility_id, scrape_date
            ),
            unavailable AS (
                SELECT
                    facility_id,
                    MIN(scrape_date) AS first_unavailable_date
                FROM history
                WHERE ever_available = 0
                GROUP BY facility_id
            ),
            ever_available AS (
                SELECT DISTINCT facility_id
                FROM history
                WHERE ever_available = 1
            )
            SELECT u.facility_id, u.first_unavailable_date
            FROM unavailable u
            LEFT JOIN ever_available ea USING (facility_id)
            WHERE ea.facility_id IS NULL
            """,
            [search_area, latest_scraped_at],
        ).df()
        if df.empty:
            return {}
        return dict(zip(df["facility_id"], df["first_unavailable_date"]))
