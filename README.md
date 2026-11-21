# Subway Ridership Forecast

Forecast next-day hourly entries at selected NYC subway station complexes. The planned evaluation compares weekly seasonal baselines with a pooled model while simulating reporting delays.

## Current status

Implemented: a tested local-calendar forecast cutoff, a bounded official MTA downloader, source-revision guards, row validation, a typed DuckDB snapshot, and offline byte-integrity verification. Station-hour aggregation, gap/DST auditing, models, and backtests are next. No forecasting accuracy or hourly coverage is claimed yet.

## Setup and checks

Use Python 3.9+ in an isolated environment:

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python -B -m unittest -v
```

The tests use generated fixtures and a real local DuckDB database; they require no network. DuckDB is pinned in `requirements.txt`.

## Download and validate a snapshot

```sh
python ingest.py download \
  --start 2025-01-06 --end 2025-01-13 \
  --stations 611 602 628 --page-size 1000 \
  --output data/raw/smoke-2025-01-06-2025-01-13
python ingest.py verify data/raw/smoke-2025-01-06-2025-01-13
```

Dates use New York local calendar days; `--end` is exclusive. The command selects only `transit_mode='subway'` and the requested station complex IDs. IDs are strings, not station names or individual platform IDs. The smoke selection is Times Sq–42 St/Port Authority (611), 14 St–Union Sq (602), and Fulton St (628).

The downloader fetches source metadata and an exact selected-row count, then pages with explicit `$order=:id`. Each row retains that unique source ID. It checks required column types, nonempty fields, station/mode/date boundaries, whole local hours, finite nonnegative numeric estimates, and `0 <= transfers <= ridership`. It rejects duplicate source row IDs across pages and count mismatches. Decimal estimates are allowed; they are not rounded to integers. Numeric values in the derived DuckDB table use DOUBLE; source numeric strings remain in JSONL.

HTTP 429, selected server errors, and network failures receive at most three attempts with bounded backoff. Each request has a 60-second timeout. `--max-rows` defaults to 250,000 to guard against accidentally downloading the whole dataset; increase it explicitly for a larger selection. Page size must be 1–50,000. An empty selection fails rather than producing a valid-looking empty dataset.

Output is staged and moved to a new directory only after validation passes. Existing output directories are rejected. Do not run concurrent writers to the same output path. A failed transfer leaves no completed snapshot. Use a new directory for each retrieval; the downloader does not resume partial files.

Each snapshot contains:

| File | Purpose |
| --- | --- |
| `rows.jsonl` | Selected source fields as canonical JSON lines, retaining source row IDs |
| `source_metadata.json` | Official metadata retrieved before downloading |
| `snapshot.duckdb` | Typed `ridership_rows` table for local analysis |
| `manifest.json` | Selection, query, page counts, source revision, retrieval times, checksums, and validation totals |

Raw downloads, databases, environments, and caches stay outside public Git. Small smoke-run manifests may be saved under `reports/`; they record what ran without distributing the dataset. The JSONL is the authoritative input for rebuilding derived tables.

## Data and evaluation limits

- Source: [MTA Subway Hourly Ridership: Beginning 2025](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-Beginning-2025/5wq4-mkjj), [JSON API](https://data.ny.gov/resource/5wq4-mkjj.json), and [column metadata](https://data.ny.gov/api/views/5wq4-mkjj.json).
- The source estimates entries by station complex, hour, payment method, and fare category. **Transfers are a subset of ridership** and must not be added again. Aggregating the fare/payment rows into station-hour totals is a separate step.
- Source timestamps are local wall times without a UTC offset. Do not silently convert them to UTC or assume every day has 24 distinct hours. DST ambiguity, gaps, fare splits, and station coverage are reserved for the aggregation audit. Missing source observations are not automatically zero.
- The downloader compares dataset `rowsUpdatedAt` and required column types before and after retrieval. A revision change aborts the run. This is a guard, not a transactional API snapshot; metadata could lag a concurrent change. Offset pagination uses the [documented stable ordering](https://dev.socrata.com/docs/queries/order.html), [limit](https://dev.socrata.com/docs/queries/limit.html), and [offset](https://dev.socrata.com/docs/queries/offset.html) parameters.
- These are **current revised historical records**. Dataset revision and retrieval times do not reconstruct when each historical service day first became available. Later backtests must simulate and disclose reporting-lag assumptions using `forecast_window.py`; they cannot treat retrieval metadata as historical publication evidence.
- A rerun can legitimately have a different checksum after source revisions. Keep the original manifest to identify the selection/revision that was tested. Validation totals are ingestion checks, not evaluation results.
