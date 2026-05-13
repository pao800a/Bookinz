"""AirBnB pipeline — orchestrates scrape → AirBnB bronze write.

Usage (CLI)::

    airbnb-run --area "Tirana, Albania" --checkin 2026-09-20 --checkout 2026-09-27

    # Multiple areas
    airbnb-run --area "Tirana, Albania" --area "Rome, Italy" \\
               --checkin 2026-09-20 --checkout 2026-09-27 --adults 5

Usage (Python API)::

    from bookinz.pipeline.airbnb_pipeline import run_pipeline

    run_pipeline(
        search_areas=["Tirana, Albania", "Rome, Italy"],
        checkin_date="2026-09-20",
        checkout_date="2026-09-27",
        data_path="data",
        num_adults=5,
    )
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import requests

from bookinz.scraper.airbnb_scraper import AirbnbScraper
from bookinz.storage.airbnb_accommodation_bronze_layer import AirbnbAccommodationBronzeLayer

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# File-logging setup
# ---------------------------------------------------------------------------

def _setup_file_logging(logs_root: Path, run_ts: str) -> None:
    """Attach a DEBUG-level FileHandler to every AirBnB module logger.

    Log files are written under::

        logs/<script_name>/<yyyyMMdd-HHmmss>/<script_name>_log_<yyyyMMddHHmmss>.log
    """
    fmt     = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    file_ts = run_ts.replace("-", "")

    entries: list[tuple[str, list[str]]] = [
        ("airbnb_pipeline",    ["bookinz.pipeline.airbnb_pipeline", "__main__"]),
        ("airbnb_scraper",     ["bookinz.scraper.airbnb_scraper"]),
        ("airbnb_accommodation_bronze_layer",["bookinz.storage.airbnb_accommodation_bronze_layer"]),
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
        print(f"[airbnb] WARNING: Could not initialise file logging: {exc}", flush=True)
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
    """Execute one full AirBnB scrape cycle for all *search_areas*.

    Steps
    -----
    1. For each area: scrape AirBnB search results.
    2. Write raw records to the AirBnB bronze layer (Parquet, hive-partitioned).

    Parameters
    ----------
    search_areas:
        List of city/region names (e.g. ``["Tirana, Albania", "Rome, Italy"]``).
    checkin_date:
        ISO-8601 check-in date.
    checkout_date:
        ISO-8601 check-out date.
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
    bronze     = AirbnbAccommodationBronzeLayer(data_path)

    for area in search_areas:
        logger.info("=== Starting AirBnB pipeline for area: %s ===", area)

        # 1. Scrape
        scraper = AirbnbScraper(
            search_area      = area,
            checkin_date     = checkin_date,
            checkout_date    = checkout_date,
            num_adults       = num_adults,
            max_pages        = max_pages,
            request_delay_s  = request_delay_s,
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
            logger.info("AirBnB bronze file: %s", parquet_path)
        except (ValueError, OSError) as exc:
            logger.error("Bronze write failed for '%s': %s", area, exc)
            continue

    logger.info("=== AirBnB pipeline run complete (scraped_at=%s) ===", scraped_at)


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="airbnb-run",
        description="Scrape AirBnB accommodation stats and store them in the AirBnB bronze layer.",
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

    run_ts   = datetime.now(tz=timezone.utc).strftime("%Y%m%d-%H%M%S")
    logs_root = Path(args.data_path).resolve().parent / "logs"
    _setup_file_logging(logs_root, run_ts)

    run_pipeline(
        search_areas     = args.areas,
        checkin_date     = args.checkin_date,
        checkout_date    = args.checkout_date,
        data_path        = args.data_path,
        num_adults       = args.num_adults,
        max_pages        = args.max_pages,
        request_delay_s  = args.request_delay_s,
    )


if __name__ == "__main__":
    main()
