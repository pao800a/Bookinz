"""
Bookinz — daily booking.com accommodation stats scraper.
Bronze layer infrastructure.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("bookinz")
except PackageNotFoundError:
    __version__ = "0.1.0"

__all__ = ["__version__"]
