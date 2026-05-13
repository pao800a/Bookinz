"""AirBnB facility pipeline — scrapes detail pages for known listings and
writes results to the ``airbnb_facilities`` bronze dataset.

Usage (CLI)::

    airbnb-facility-run --data-path data

    # Explicitly provide listing IDs
    airbnb-facility-run --data-path data --facility-id 12345 --facility-id 67890

    # Limit how many listings to process
    airbnb-facility-run --data-path data --max-listings 50

Usage (Python API)::

    from bookinz.pipeline.airbnb_facility_pipeline import run_pipeline

    run_pipeline(data_path="data")

The pipeline queries ``airbnb_bronze`` (the search-result dataset) to obtain
the latest set of distinct ``(facility_id, url)`` pairs, then scrapes each
listing's detail page — unless that ``(facility_id, scrape_date)`` pair
already exists in the facility bronze layer (idempotent).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from bookinz.scraper.airbnb_facility_scraper import AirbnbFacilityScraper
from bookinz.storage.airbnb_accommodation_bronze_layer import AirbnbAccommodationBronzeLayer
from bookinz.storage.airbnb_facility_bronze_layer import AirbnbFacilityBronzeLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File-logging setup
# ---------------------------------------------------------------------------

def _setup_file_logging(logs_root: Path, run_ts: str) -> None:
    fmt     = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_ts = run_ts.replace("-", "")

    entries: list[tuple[str, list[str]]] = [
        ("airbnb_facility_pipeline",    ["bookinz.pipeline.airbnb_facility_pipeline", "__main__"]),
        ("airbnb_facility_scraper",     ["bookinz.scraper.airbnb_facility_scraper"]),
        ("airbnb_facility_bronze_layer",["bookinz.storage.airbnb_facility_bronze_layer"]),
    ]

    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler) and h.level == logging.NOTSET:
            h.setLevel(logging.INFO)

    try:
        for script_name, logger_names in entries:
            log_dir  = logs_root / script_name / run_ts
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"{script_name}_log_{file_ts}.log"

            handler = logging.FileHandler(log_file, encoding="utf-8")
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(fmt)

            for name in logger_names:
                lg = logging.getLogger(name)
                lg.setLevel(logging.DEBUG)
                lg.addHandler(handler)
    except Exception as exc:  # noqa: BLE001
        print(f"[airbnb-facility] WARNING: Could not initialise file logging: {exc}", flush=True)
        return

    logger.info("File logging initialised. Logs root: %s", logs_root)


# ---------------------------------------------------------------------------
# Listing discovery
# ---------------------------------------------------------------------------

def _discover_listings(
    data_path: Path,
    facility_ids: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Return ``[(facility_id, url), ...]`` from the ``airbnb_bronze`` view.

    If *facility_ids* is provided, only those IDs are returned.
    """
    try:
        abl = AirbnbAccommodationBronzeLayer(data_path)
        sql = (
            "SELECT DISTINCT facility_id, url "
            "FROM airbnb_bronze "
            "WHERE facility_id IS NOT NULL AND url IS NOT NULL"
        )
        if facility_ids:
            ids_str = ", ".join(f"'{fid}'" for fid in facility_ids)
            sql += f" AND facility_id IN ({ids_str})"
        con = abl.connection()
        try:
            df = con.execute(sql).df()
        finally:
            con.close()
        listings = list(zip(df["facility_id"].astype(str), df["url"].astype(str)))
        logger.info("Discovered %d unique listing(s) from airbnb_bronze.", len(listings))
        return listings
    except Exception as exc:
        logger.warning("Could not query airbnb_bronze for listings: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    data_path: str | Path = "data",
    facility_ids: list[str] | None = None,
    max_listings: int | None = None,
    request_delay_s: float = 3.0,
    headless: bool = True,
    skip_existing: bool = True,
) -> None:
    """Scrape AirBnB detail pages and write to the facility bronze layer.

    Parameters
    ----------
    data_path:
        Root directory for the data lake.
    facility_ids:
        Optional explicit list of listing IDs to scrape. If ``None``, all
        listings found in ``airbnb_bronze`` are used.
    max_listings:
        Cap the number of listings to process in this run.
    request_delay_s:
        Polite delay (seconds) between page loads.
    headless:
        Run the browser in headless mode.
    skip_existing:
        If ``True`` (default), skip listings already scraped today.
    """
    data_path   = Path(data_path)
    scraped_at  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    scrape_date = scraped_at[:10]

    listings = _discover_listings(data_path, facility_ids)
    if not listings:
        logger.warning("No listings to scrape. Exiting.")
        return

    facility_layer = AirbnbFacilityBronzeLayer(data_path)

    if skip_existing:
        before = len(listings)
        listings = [
            (fid, url)
            for fid, url in listings
            if not facility_layer.already_scraped(fid, scrape_date)
        ]
        skipped = before - len(listings)
        if skipped:
            logger.info("Skipping %d listing(s) already scraped on %s.", skipped, scrape_date)

    if max_listings is not None:
        listings = listings[:max_listings]

    if not listings:
        logger.info("All listings already scraped for today (%s). Nothing to do.", scrape_date)
        return

    logger.info("=== AirBnB facility pipeline: %d listing(s) to scrape ===", len(listings))

    with AirbnbFacilityScraper(
        request_delay_s=request_delay_s,
        headless=headless,
    ) as scraper:
        records = scraper.scrape_as_dicts(listings, scraped_at)

    if not records:
        logger.warning("No facility records returned. Nothing written.")
        return

    try:
        written = facility_layer.write(records, scraped_at)
        logger.info(
            "=== Facility pipeline complete: %d record(s) -> %d file(s) ===",
            len(records),
            len(written),
        )
    except (ValueError, OSError) as exc:
        logger.error("Facility bronze write failed: %s", exc)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airbnb-facility-run",
        description=(
            "Scrape AirBnB facility detail pages and store results in the "
            "airbnb_facility_bronze dataset."
        ),
    )
    parser.add_argument(
        "--data-path",
        default="data",
        metavar="PATH",
        help="Root of the data lake (default: data).",
    )
    parser.add_argument(
        "--facility-id",
        dest="facility_ids",
        action="append",
        default=None,
        metavar="ID",
        help=(
            "Specific facility ID(s) to scrape. Can be specified multiple times. "
            "If omitted, all IDs from airbnb_bronze are used."
        ),
    )
    parser.add_argument(
        "--max-listings",
        type=int,
        default=None,
        metavar="N",
        help="Maximum number of listings to process in this run.",
    )
    parser.add_argument(
        "--delay",
        dest="request_delay_s",
        type=float,
        default=3.0,
        help="Delay in seconds between page loads (default: 3.0).",
    )
    parser.add_argument(
        "--visible",
        dest="headless",
        action="store_false",
        default=True,
        help="Run browser in visible (non-headless) mode.",
    )
    parser.add_argument(
        "--no-skip",
        dest="skip_existing",
        action="store_false",
        default=True,
        help="Re-scrape listings even if today's data already exists.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: INFO).",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args   = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    run_ts    = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    logs_root = Path(args.data_path).resolve().parent / "logs"
    _setup_file_logging(logs_root, run_ts)

    run_pipeline(
        data_path       = args.data_path,
        facility_ids    = args.facility_ids,
        max_listings    = args.max_listings,
        request_delay_s = args.request_delay_s,
        headless        = args.headless,
        skip_existing   = args.skip_existing,
    )


if __name__ == "__main__":
    main()
