# Subway Ridership Forecast

Forecast next-day hourly entries at selected NYC subway station complexes. The planned evaluation compares weekly seasonal baselines with a pooled model while simulating reporting delays.

## Current status

Implemented: a tested local-calendar forecast cutoff, a bounded official MTA downloader, source-revision guards, row validation, a typed DuckDB snapshot, and offline byte-integrity verification. Station-hour aggregation and gap/fare/DST auditing are implemented; models and backtests are next. No forecasting accuracy is claimed. Coverage reports describe observed local-hour labels, not certified completeness of every fare partition.

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

## Aggregate station-hours and audit coverage

```sh
python aggregate.py data/raw/smoke-2025-01-06-2025-01-13 \
  --output data/processed/smoke-2025-01-06-2025-01-13
```

The command verifies the snapshot's checksums first, reads its DuckDB database without modifying it, and writes a new directory containing `station_hours.duckdb`, `station_hours.parquet`, and `audit.json`. Existing output directories are rejected. Time-zone rules come explicitly from pinned `tzdata==2025.2`, independent of the host's zone database.

The panel key is `(station_complex_id, local_timestamp)`. Each requested station gets 24 **wall-hour labels** per local calendar date, including stations absent from the input. `service_date` is that local calendar date with a midnight boundary; there is no 4am service-day shift. `target` sums the observed fare/payment rows' ridership once; transfers remain a separate audit column. A fully absent station-hour has status `missing` and a null target. A reported zero has status `observed` and target zero. No zero filling or gap interpolation is performed.

Non-DST duplicate `(station, hour, payment_method, fare_class_category)` keys and unknown/contradictory fare/payment labels cause aggregation to fail. Category counts and absent-pair diagnostics are reported without inventing zeros. A missing fare category could mean no transactions or incomplete reporting, so even an observed hour is not certified as having every partition. The diagnostic's reference pairs come from the current snapshot; do not use them as model inputs or historical availability evidence.

For **both DST transition days**, all 24 wall-hour labels have status `excluded_dst_day` and null model targets. Raw grouped values remain in `ridership_observed` for inspection. Nonexistent spring and ambiguous fall labels are identified by UTC round trips. The source has no UTC offset to separate the two fall occurrences, so no artificial UTC instants or 25th hourly target are fabricated.

### Measured smoke audits

| Selection, three station complexes | Raw rows | Wall-hour grid rows | Missing eligible hours | DST rows excluded from targets |
| --- | ---: | ---: | ---: | ---: |
| Jan 6–12, 2025 | 4,841 | 504 | 0 | 0 |
| Mar 8–10, 2025 | 2,220 | 216 | 0 | 72 |
| Nov 1–3, 2025 | 2,282 | 216 | 0 | 72 |

Each audit conserved the input ridership sum. January's fare/payment pair counts varied from 4 to 11 per observed hour across these stations. The spring source contains rows labeled `2025-03-09T02:00:00`, a nonexistent local hour; those records are retained as a source anomaly, not reinterpreted or silently dropped. Fall's `2025-11-02T01:00:00` has no offset distinguishing its two physical occurrences. These findings support the conservative day-exclusion policy, and they do not establish accuracy or system-wide data quality. Small measured audit reports are in `reports/aggregation_*.json`.

Reproduce the DST probes with `ingest.py download --start 2025-03-08 --end 2025-03-11 --stations 611 602 628 --output data/raw/dst-spring-2025` and the same command for November 1–4 into `data/raw/dst-fall-2025`, then run `aggregate.py` on each. End dates are exclusive. Snapshots are not combined across revisions without recording their provenance.

The predeclared development/calibration/final-period boundaries are in [EVALUATION_PLAN.md](EVALUATION_PLAN.md) and `study_config.json`. October 2026 is reserved for final evaluation; no model fitting or holdout scoring has occurred. The next milestone implements chronological folds and the first seasonal baselines with availability cutoffs.
