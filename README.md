# Subway Ridership Forecast

Forecast next-day hourly entries at selected NYC subway station complexes. The planned evaluation compares weekly seasonal baselines with a pooled model while simulating reporting delays.

## Current status

Implemented: a tested local-calendar forecast cutoff, a bounded official MTA downloader, source-revision guards, row validation, a typed DuckDB snapshot, and offline byte-integrity verification. Station-hour aggregation and gap/fare/DST auditing are implemented; chronological seasonal-baseline backtesting is implemented; the pooled model is next. Development-baseline metrics are reported separately from the final holdout. Coverage reports describe observed local-hour labels, not certified completeness of every fare partition.

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

The predeclared development/calibration/final-period boundaries are in [EVALUATION_PLAN.md](EVALUATION_PLAN.md) and `study_config.json`. October 2026 is reserved for final evaluation; no model fitting or holdout scoring has occurred. The backtest runner implements chronological folds and both seasonal baselines with availability cutoffs.

## Chronological seasonal-baseline backtest

```sh
python ingest.py download --start 2025-01-01 --end 2026-09-01 \
  --stations 602 611 628 --page-size 50000 --max-rows 500000 \
  --output data/raw/development-2025-01-01-2026-09-01
python aggregate.py data/raw/development-2025-01-01-2026-09-01 \
  --output data/processed/development-2025-01-01-2026-09-01
python backtest.py data/processed/development-2025-01-01-2026-09-01 \
  --output data/processed/seasonal-development
```

The runner requires the complete declared development panel, verifies its artifact checksums and station-hour grid, and rejects input selections containing calibration or holdout dates. No calibration/holdout command is exposed. It generates the six declared month-long development folds and walks forward one issuance day at a time. Earlier validation observations may become available to later issuances when the assumed reporting delay has elapsed; observations later than each cutoff cannot enter the predictor.

For a target service day `T`, issuance is `T-1` and the latest available complete service day is `T-1-lag`. For example, forecasting Monday March 17, 2025 on Sunday March 16 under a seven-day publication delay gives a March 9 cutoff. The previous Monday (March 10) is unavailable, so the latest weekly reference is March 3. A reference exactly on the cutoff is allowed.

Two baselines share identical scoreable cases:

- `seasonal_latest`: the latest observed same station, weekday, and hour at/before the issuance cutoff.
- `seasonal_mean4`: the unweighted mean of up to four such observations. It needs at least one; missing and DST-excluded references are skipped. There is no cross-station fallback, interpolation, or assumed zero for missing history.

DST transition days receive no forecasts or scores. Missing targets are unscored; an available prediction may still be retained for diagnosis. Reported zeros are valid targets. The output `predictions.parquet` retains issuance/target/cutoff dates, reference counts and oldest/newest reference dates, actuals, predictions, statuses, and scoreability. A final SQL chronology audit rejects any newest reference after its cutoff. The source/aggregate files remain unchanged.

`metrics.json` reports scheduled/observed/predictable/scored counts plus MAE, RMSE, and WAPE, overall and by fold, station, weekday/weekend, and clock-hour band. MAE is mean absolute error in entries per station-hour; RMSE is root mean square error in the same units; WAPE is total absolute error divided by total observed entries on scored cases, stored as a fraction. WAPE is null when its denominator is zero. Peak clock hours are fixed at 07–09 and 16–18, including weekends; other hours are off-peak. Compare error metrics with their coverage counts, not as if missing targets were zero-error cases.

Small measured metrics and ingestion/aggregation provenance reports are kept under `reports/`. Prediction arrays, panels, and raw data remain outside Git/checkpoints. Development-fold results describe the three selected Manhattan hubs using currently revised records and simulated publication delays; they are not final holdout scores or evidence of historical as-published availability.

### First measured development results

Six declared validation months, three complexes, 13,104 scored station-hours per baseline/delay, with 72 transition-day station-hours excluded per run. No validation target or seasonal reference was imputed. September calibration and October final holdout were not loaded.

| Assumed lag (days) | Latest weekly MAE | Mean of four MAE | Latest weekly WAPE | Mean of four WAPE |
| --- | ---: | ---: | ---: | ---: |
| 1 | 368.47 | 382.37 | 10.37% | 10.76% |
| 3 | 368.47 | 382.37 | 10.37% | 10.76% |
| 7 | 445.55 | 404.94 | 12.54% | 11.40% |
| 14 | 495.85 | 411.76 | 13.96% | 11.59% |

Lags of 1 and 3 days select the same weekly references, explaining their identical scores; a seven-day delay makes the immediately preceding week's matching day unavailable at issuance. At one-day delay, the four-reference mean has worse MAE but lower RMSE (736.56 vs 800.97), so the preferred baseline depends on the error objective. These are development comparisons, not a final model selection or holdout result.

The full development source snapshot contained 414,728 fare/payment rows and aggregated to 43,776 station-hour labels. The audit found one missing non-DST hour (Fulton Street, March 28, 2026 at 02:00), outside the declared validation months; it remains null. All three transition days in the development range were excluded. The small measured reports are `reports/ingest_development.json`, `aggregation_development.json`, `baseline_development.json`, and `baseline_verification.json`.

An independent checker steps backward in seven-day increments rather than using the predictor's history index. It checked all **105,408 saved prediction records**, including unscored records, and recalculated the overall metrics in Python with zero discrepancies:

```sh
python verify_backtest.py data/processed/development-2025-01-01-2026-09-01 \
  data/processed/seasonal-development
```

Run verification without Python's `-O` option. Checksums identify the exact inputs/results tested; a later official-source revision can change a re-download.
