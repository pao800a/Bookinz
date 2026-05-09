"""AirBnB availability pipeline — fetches calendar availability for known
listings via the AirBnB internal calendar API and writes results to the
``airbnb_availability`` bronze dataset.

Usage (CLI)::

    airbnb-availability-run --data-path data

    # Explicitly provide listing IDs
    airbnb-availability-run --data-path data --facility-id 12345 --facility-id 67890

    # Fetch 3 months ahead instead of the default 6
    airbnb-availability-run --data-path data --months 3

Usage (Python API)::

    from bookinz.pipeline.airbnb_availability_pipeline import run_pipeline

    run_pipeline(data_path="data", months_ahead=6)

Listing IDs are discovered from ``airbnb_bronze`` (the search-results dataset)
unless ``--facility-id`` is supplied explicitly. Runs are idempotent: if data
for a ``(facility_id, scrape_date)`` pair already exists in the availability
layer the listing is skipped by default.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from bookinz.scraper.airbnb_availability_scraper import AirbnbAvailabilityScraper
from bookinz.storage.airbnb_bronze_layer import AirbnbBronzeLayer
from bookinz.storage.airbnb_availability_bronze_layer import AirbnbAvailabilityBronzeLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File-logging setup
# ---------------------------------------------------------------------------

def _setup_file_logging(logs_root: Path, run_ts: str) -> None:
    fmt     = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_ts = run_ts.replace("-", "")

    entries: list[tuple[str, list[str]]] = [
        ("airbnb_availability_pipeline",    ["bookinz.pipeline.airbnb_availability_pipeline", "__main__"]),
        ("airbnb_availability_scraper",     ["bookinz.scraper.airbnb_availability_scraper"]),
        ("airbnb_availability_bronze_layer",["bookinz.storage.airbnb_availability_bronze_layer"]),
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
        print(f"[airbnb-availability] WARNING: Could not initialise file logging: {exc}", flush=True)
        return

    logger.info("File logging initialised. Logs root: %s", logs_root)


# ---------------------------------------------------------------------------
# Listing discovery
# ---------------------------------------------------------------------------

def _discover_facility_ids(
    data_path: Path,
    facility_ids: list[str] | None = None,
) -> list[str]:
    """Return distinct facility IDs from ``airbnb_bronze``.

    If *facility_ids* is provided, it is returned directly (after dedup).
    """
    if facility_ids:
        return list(dict.fromkeys(facility_ids))  # preserve order, dedup

    try:
        abl = AirbnbBronzeLayer(data_path)
        df  = abl.query(
            "SELECT DISTINCT facility_id FROM airbnb_bronze "
            "WHERE facility_id IS NOT NULL"
        )
        ids = df["facility_id"].astype(str).tolist()
        logger.info("Discovered %d unique facility ID(s) from airbnb_bronze.", len(ids))
        return ids
    except Exception as exc:
        logger.warning("Could not query airbnb_bronze for facility IDs: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Core pipeline
# ---------------------------------------------------------------------------

def run_pipeline(
    data_path: str | Path = "data",
    facility_ids: list[str] | None = None,
    max_listings: int | None = None,
    months_ahead: int = 6,
    request_delay_s: float = 2.0,
    headless: bool = True,
    skip_existing: bool = True,
) -> None:
    """Fetch calendar availability and write to the availability bronze layer.

    Parameters
    ----------
    data_path:
        Root directory for the data lake.
    facility_ids:
        Optional explicit list of listing IDs. If ``None``, all IDs from
        ``airbnb_bronze`` are used.
    max_listings:
        Cap the number of listings to process in this run.
    months_ahead:
        Number of calendar months to request per listing (1–12).
    request_delay_s:
        Polite delay (seconds) between API calls.
    headless:
        Run the browser in headless mode.
    skip_existing:
        If ``True`` (default), skip listings already fetched today.
    """
    data_path   = Path(data_path)
    scraped_at  = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    scrape_date = scraped_at[:10]

    ids = _discover_facility_ids(data_path, facility_ids)
    if not ids:
        logger.warning("No facility IDs to process. Exiting.")
        return

    avail_layer = AirbnbAvailabilityBronzeLayer(data_path)

    if skip_existing:
        before = len(ids)
        ids    = [fid for fid in ids if not avail_layer.already_scraped(fid, scrape_date)]
        skipped = before - len(ids)
        if skipped:
            logger.info("Skipping %d facility ID(s) already fetched on %s.", skipped, scrape_date)

    if max_listings is not None:
        ids = ids[:max_listings]

    if not ids:
        logger.info("All facilities already fetched for today (%s). Nothing to do.", scrape_date)
        return

    logger.info("=== AirBnB availability pipeline: %d facility ID(s) to fetch ===", len(ids))

    with AirbnbAvailabilityScraper(
        months_ahead    = months_ahead,
        request_delay_s = request_delay_s,
        headless        = headless,
    ) as scraper:
        records = scraper.scrape_as_dicts(ids, scraped_at)

    if not records:
        logger.warning("No availability records returned. Nothing written.")
        return

    try:
        written = avail_layer.write(records, scraped_at)
        logger.info(
            "=== Availability pipeline complete: %d record(s) → %d file(s) ===",
            len(records),
            len(written),
        )
    except (ValueError, OSError) as exc:
        logger.error("Availability bronze write failed: %s", exc)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airbnb-availability-run",
        description=(
            "Fetch AirBnB calendar availability for known listings and store "
            "results in the airbnb_availability_bronze dataset."
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
            "Specific facility ID(s) to fetch. Can be specified multiple times. "
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
        "--months",
        dest="months_ahead",
        type=int,
        default=6,
        metavar="N",
        help="Calendar months to fetch per listing (1–12, default: 6).",
    )
    parser.add_argument(
        "--delay",
        dest="request_delay_s",
        type=float,
        default=2.0,
        help="Delay in seconds between API calls (default: 2.0).",
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
        help="Re-fetch listings even if today's data already exists.",
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
        months_ahead    = args.months_ahead,
        request_delay_s = args.request_delay_s,
        headless        = args.headless,
        skip_existing   = args.skip_existing,
    )


if __name__ == "__main__":
    main()
