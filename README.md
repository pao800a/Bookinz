# Bookinz

Daily booking.com accommodation stats scraper with a **bronze–silver–gold** data lake architecture.

## Features

- Scrapes accommodation listings from booking.com for any city/region
- Collects per-facility stats:
  - Price per night (with currency)
  - Guest review score and category
  - Distance from city centre (km)
  - Number of rooms available
  - Availability status
- Stores raw data in the **bronze layer** as partitioned Parquet files (Snappy-compressed)
- Datasets are queryable with **DuckDB** (hive-style partition discovery)
- **Availability monitor** fires alerts when a previously unavailable facility comes back
- Daily scheduling via `schedule`

## Project layout

```
src/bookinz/
├── scraper/          # HTTP scraper (booking_scraper.py)
├── storage/          # Bronze layer writer + DuckDB helpers (bronze_layer.py)
├── alerts/           # Availability monitor (availability_monitor.py)
└── pipeline/         # Daily orchestration CLI (daily_pipeline.py)

data/
├── bronze/           # Raw partitioned Parquet (auto-created)
├── silver/           # (future) Cleaned / typed data
└── gold/             # (future) Aggregated / enriched data

tests/
├── test_bronze_layer.py
├── test_scraper.py
└── test_availability_monitor.py
```

### Bronze layer partition layout

```
data/bronze/accommodations/
└── search_area=Amsterdam/
    └── scrape_date=2024-01-15/
        └── run_2024-01-15T08-00-00.parquet
```

## Quick start

```bash
# Install
pip install -e ".[dev]"

# Run once (scrape Amsterdam, check-in 2024-02-01, check-out 2024-02-03)
bookinz-run --area Amsterdam --checkin 2024-02-01 --checkout 2024-02-03

# Run on a daily schedule at 08:00
bookinz-run --area Amsterdam --area Paris \
            --checkin 2024-02-01 --checkout 2024-02-03 \
            --schedule --schedule-time 08:00
```

## Querying with DuckDB

```python
from bookinz.storage import BronzeLayer

bl = BronzeLayer("data")

# All records
df = bl.query("SELECT * FROM bronze")

# Cheapest available hotels in Amsterdam
df = bl.query("""
    SELECT name, price_per_night, rating, distance_from_center_km
    FROM bronze
    WHERE search_area = 'Amsterdam'
      AND is_available = true
    ORDER BY price_per_night
""")

# Raw DuckDB connection for complex analysis
con = bl.connection()
con.execute("SELECT DISTINCT search_area FROM bronze").df()
con.close()
```

## Running tests

```bash
pytest tests/ -v
```
