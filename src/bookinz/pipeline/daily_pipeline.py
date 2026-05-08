"""Daily pipeline — orchestrates scrape → bronze write → availability monitoring.

Usage (CLI)::

    # Run once immediately
    bookinz-run --area Amsterdam --checkin 2024-02-01 --checkout 2024-02-03

    # Run once and then keep running on a daily schedule
    bookinz-run --area Amsterdam --checkin 2024-02-01 --checkout 2024-02-03 --schedule

    # Multiple areas
    bookinz-run --area Amsterdam --area Paris --checkin 2024-02-01 --checkout 2024-02-03

Usage (Python API)::

    from bookinz.pipeline.daily_pipeline import run_pipeline

    run_pipeline(
        search_areas=["Amsterdam", "Paris"],
        checkin_date="2024-02-01",
        checkout_date="2024-02-03",
        data_path="data",
    )
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests
import schedule
import time

from bookinz.alerts.availability_monitor import AvailabilityMonitor
from bookinz.scraper.booking_scraper import BookingComScraper
from bookinz.storage.bronze_layer import BronzeLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File-logging setup
# ---------------------------------------------------------------------------

def _setup_file_logging(logs_root: Path, run_ts: str) -> None:
    """Attach a DEBUG-level FileHandler to every module logger for this run.

    Each module gets its own log directory::

        logs/<script_name>/<yyyyMMdd-HHmmss>/<script_name>_log_<yyyyMMddHHmmss>.log

    Console output is left unchanged (INFO by default via basicConfig).

    Parameters
    ----------
    logs_root:
        Root directory for all log trees (e.g. ``<repo>/logs``).
    run_ts:
        Execution-start timestamp in ``yyyyMMdd-HHmmss`` format used as the
        sub-directory name.
    """
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_ts = run_ts.replace("-", "")  # yyyyMMddHHmmss for the filename

    # (script_name, [logger names that write to that script's log file])
    entries: list[tuple[str, list[str]]] = [
        ("daily_pipeline",       ["bookinz.pipeline.daily_pipeline", "__main__"]),
        ("booking_scraper",      ["bookinz.scraper.booking_scraper"]),
        ("bronze_layer",         ["bookinz.storage.bronze_layer"]),
        ("availability_monitor", ["bookinz.alerts.availability_monitor"]),
    ]

    # Clamp the root StreamHandler to INFO so that DEBUG records coming from
    # module loggers (set to DEBUG below) don't bleed onto the console.
    for h in logging.getLogger().handlers:
        if isinstance(h, logging.StreamHandler) and h.level == logging.NOTSET:
            h.setLevel(logging.INFO)

    try:
        for script_name, logger_names in entries:
            log_dir = logs_root / script_name / run_ts
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / f"{script_name}_log_{file_ts}.log"

            handler = logging.FileHandler(log_file, encoding="utf-8")
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(fmt)

            for name in logger_names:
                lg = logging.getLogger(name)
                lg.setLevel(logging.DEBUG)  # allow DEBUG through to the file handler
                lg.addHandler(handler)
    except Exception as exc:  # noqa: BLE001
        print(f"[bookinz] WARNING: Could not initialise file logging: {exc}", flush=True)
        return

    logger.info("File logging initialised. Logs root: %s", logs_root)


# ---------------------------------------------------------------------------
# Core run function
# ---------------------------------------------------------------------------

def run_pipeline(
    search_areas: list[str],
    checkin_date: str,
    checkout_date: str,
    data_path: str | Path = "data",
    num_adults: int = 2,
    max_pages: int = 5,
    request_delay_s: float = 2.0,
) -> None:
    """Execute one full scrape cycle for all *search_areas*.

    Steps
    -----
    1. For each area scrape booking.com.
    2. Write raw records to the bronze layer (Parquet, partitioned by area + date).
    3. Run the availability monitor and log any alerts.

    Parameters
    ----------
    search_areas:
        List of city/region names to search (e.g. ``["Amsterdam", "Paris"]``).
    checkin_date:
        ISO-8601 check-in date (``YYYY-MM-DD``).
    checkout_date:
        ISO-8601 check-out date (``YYYY-MM-DD``).
    data_path:
        Root directory for the data lake.
    num_adults:
        Number of adult guests.
    max_pages:
        Maximum result pages to scrape per area.
    request_delay_s:
        Polite delay (seconds) between HTTP requests.
    """
    scraped_at = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    bronze = BronzeLayer(data_path)

    for area in search_areas:
        logger.info("=== Starting pipeline for area: %s ===", area)

        # 1. Scrape
        scraper = BookingComScraper(
            search_area=area,
            checkin_date=checkin_date,
            checkout_date=checkout_date,
            num_adults=num_adults,
            max_pages=max_pages,
            request_delay_s=request_delay_s,
        )
        try:
            records = scraper.scrape_as_dicts(scraped_at)
        except (requests.RequestException, OSError) as exc:
            logger.error("Scraping failed for '%s': %s", area, exc)
            continue

        if not records:
            logger.warning("No records returned for '%s'. Skipping bronze write.", area)
            continue

        logger.info("Scraped %d records for '%s'.", len(records), area)

        # 2. Write bronze
        try:
            parquet_path = bronze.write(records, scraped_at)
            logger.info("Bronze file: %s", parquet_path)
        except (ValueError, OSError) as exc:
            logger.error("Bronze write failed for '%s': %s", area, exc)
            continue

        # 3. Availability monitoring
        monitor = AvailabilityMonitor(bronze)
        alerts = monitor.check(search_area=area, latest_scraped_at=scraped_at)
        if alerts:
            logger.warning("=== %d availability alert(s) for '%s' ===", len(alerts), area)
            for alert in alerts:
                logger.warning("%s", alert)
        else:
            logger.info("No availability alerts for '%s'.", area)

    logger.info("=== Pipeline run complete (scraped_at=%s) ===", scraped_at)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bookinz-run",
        description="Scrape booking.com accommodation stats and store them in the bronze layer.",
    )
    parser.add_argument(
        "--area",
        dest="areas",
        action="append",
        required=True,
        metavar="AREA",
        help="Search area (city/region). Can be specified multiple times.",
    )
    parser.add_argument(
        "--checkin",
        dest="checkin_date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Check-in date.",
    )
    parser.add_argument(
        "--checkout",
        dest="checkout_date",
        required=True,
        metavar="YYYY-MM-DD",
        help="Check-out date.",
    )
    parser.add_argument(
        "--data-path",
        default="data",
        metavar="PATH",
        help="Root of the data lake (default: data).",
    )
    parser.add_argument(
        "--adults",
        dest="num_adults",
        type=int,
        default=2,
        help="Number of adult guests (default: 2).",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=5,
        help="Maximum result pages to scrape per area (default: 5).",
    )
    parser.add_argument(
        "--delay",
        dest="request_delay_s",
        type=float,
        default=2.0,
        help="Delay in seconds between HTTP requests (default: 2.0).",
    )
    parser.add_argument(
        "--schedule",
        dest="use_schedule",
        action="store_true",
        help="Keep running and re-execute the pipeline every 24 hours.",
    )
    parser.add_argument(
        "--schedule-time",
        default="08:00",
        metavar="HH:MM",
        help="Daily execution time when --schedule is used (default: 08:00).",
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
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    run_ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    # Place logs/ next to the data/ directory (i.e. at the repo root).
    logs_root = Path(args.data_path).resolve().parent / "logs"
    _setup_file_logging(logs_root, run_ts)

    pipeline_kwargs = {
        "search_areas": args.areas,
        "checkin_date": args.checkin_date,
        "checkout_date": args.checkout_date,
        "data_path": args.data_path,
        "num_adults": args.num_adults,
        "max_pages": args.max_pages,
        "request_delay_s": args.request_delay_s,
    }

    # Run once immediately
    run_pipeline(**pipeline_kwargs)

    if args.use_schedule:
        logger.info("Scheduling daily run at %s.", args.schedule_time)
        schedule.every().day.at(args.schedule_time).do(run_pipeline, **pipeline_kwargs)
        while True:
            schedule.run_pending()
            time.sleep(30)


if __name__ == "__main__":
    main()
