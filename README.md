# Subway Ridership Forecast

Forecast next-day hourly entries at selected NYC subway station complexes. The planned evaluation compares weekly seasonal baselines with a pooled model while simulating reporting delays.

## Current status

Implemented: a tested local-calendar forecast cutoff, a bounded official MTA downloader, source-revision guards, row validation, a typed DuckDB snapshot, and offline byte-integrity verification. Station-hour aggregation and gap/fare/DST auditing are implemented; chronological seasonal-baseline backtesting is implemented; cutoff-bounded forecasting feature generation with explicit source-date provenance and leakage/perturbation guards is implemented (`features.py`); the pooled histogram boosted-tree model (`model.py`, `reports/model_development.json`), reproducible Google Colab experiment notebook ([`notebooks/ridership_forecast_experiment.ipynb`](notebooks/ridership_forecast_experiment.ipynb)), and GitHub Actions CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) are implemented; past-residual prediction interval calibration and reporting-lag slice error walkthroughs (`calibrate.py`, [`reports/calibration_development.json`](reports/calibration_development.json), [`reports/slice_error_walkthrough.json`](reports/slice_error_walkthrough.json)) are implemented. Development metrics are reported separately from the final holdout. Coverage reports describe observed local-hour labels, not certified completeness of every fare partition.

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

## Cutoff-bounded forecasting features and leakage guards

```sh
python features.py data/processed/development-2025-01-01-2026-09-01 \
  --output data/processed/features-development
```

`features.py` verifies the full declared development panel (`2025-01-01` to `2026-09-01`, refusing calibration/holdout dates and checksum mismatches), runs canary future-perturbation checks across every station and lag, writes `features.parquet` and `feature_audit.json`, and saves the measured audit summary in [`reports/features_development.json`](reports/features_development.json).

For each `(station_complex_id, target_day, hour, lag_days)` with issuance `issued_on = target_day - 1` and cutoff `observed_through = issued_on - lag_days`, `FeatureHistory` builds 24 model feature columns strictly from `status == 'observed'` observations on service dates `d <= observed_through`:

- **Calendar and cyclic features**: `day_of_week`, `is_weekend`, `is_peak_hour`, `month`, `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`.
- **Cutoff-aligned same-weekday/same-hour weekly lags**: `same_weekday_lag1..lag4`, `same_weekday_count4`, `same_weekday_mean4`, `same_weekday_min4`, `same_weekday_max4`, `same_weekday_std4`, `same_weekday_trend` (`lag1 - lag2`), and `same_weekday_age_days` (`target_day - same_weekday_newest_date`).
- **Trailing 7-day same-hour and intraday profile features** (`[observed_through - 6 days, observed_through]`): `recent_same_hour_latest`, `recent_same_hour_mean7`, `recent_same_hour_count7`, `recent_same_hour_adjacent_mean` (mean of observed `hour - 1` and `hour + 1` on `recent_same_hour_newest_date`), and `cutoff_day_total_24h` (sum of all 24 observed hours on the newest complete 24-hour non-DST day in that trailing 7-day window).
- **Per-row source-date provenance**: `same_weekday_oldest_date`, `same_weekday_newest_date`, `recent_same_hour_oldest_date`, `recent_same_hour_newest_date`, `cutoff_full_day_date`, `feature_oldest_source_date`, and `feature_newest_source_date`.

Three independent leakage barriers protect model inputs and training splits:
1. **Row provenance validation (`validate_feature_row`)**: Rejects any feature vector where any source date exceeds `observed_through`, touches an `excluded_dst_day`, violates `same_weekday_age_days > lag_days`, or emits historical features for a DST transition target day.
2. **Training-label cutoff validation (`validate_training_cut`)**: At any fold or walk-forward issuance cutoff `C`, rejects any training row with `target_day > C` (excluding targets in the unpublished reporting-lag gap `(C, fold_start)` as well as any missing/DST/unready rows).
3. **Canary perturbation invariance (`verify_cutoff_invariance`)**: Replaces every observation on service days `d > observed_through` with `1e12`, recomputes the feature vector, and asserts bit-for-bit equality across all feature and provenance columns.

### Measured development feature audit (`reports/features_development.json`)

Generated from the verified development panel (`173,016` total feature rows across lags `1`, `3`, `7`, and `14`; `0` leakage violations; `12/12` canary perturbation checks passed):

| Assumed lag (days) | Total rows | Model-ready rows | Validation scheduled | Validation scoreable | Validation DST excluded | Same-weekday age (days) | Unpublished gap rows excluded at each fold start |
| ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| 1 | 43,632 | 43,055 | 13,176 | 13,104 | 72 | 7–14 | 72 |
| 3 | 43,488 | 43,055 | 13,176 | 13,104 | 72 | 7–14 | 216 |
| 7 | 43,200 | 42,551 | 13,176 | 13,104 | 72 | 14–21 | 504 |
| 14 | 42,696 | 42,047 | 13,176 | 13,104 | 72 | 21–28 | 1,008 |

Every lag produces the exact same `13,104` scoreable validation station-hours (`2,160` in 30-day validation months and March 2025 with its DST transition day; `2,232` in January and July 2026) as the seasonal baseline evaluation, ensuring a strictly matched comparison for the pooled model.

## Pooled boosted-tree development evaluation and Colab notebook

```sh
python model.py data/processed/development-2025-01-01-2026-09-01 \
  --output data/processed/model-development
```

`model.py` trains a deterministic **pooled histogram gradient-boosted regression tree ensemble** (`PooledBoostedTrees`, method name `pooled_boosted_trees`) across all three station complexes (`602`, `611`, `628`) jointly for each of the six chronological development folds and four reporting lags (`1`, `3`, `7`, and `14` days). At each fold start, `validate_training_cut` enforces that only eligible observations with `target_day <= fold_start_observed_through` enter model fitting. The ensemble selects a fold-specific convex anchor blend between `same_weekday_lag1` and `same_weekday_mean4` on the pre-cutoff training set and fits 20 depth-2 quantile-binned regression trees with L2 leaf regularization (`l2_reg = 25.0`, `min_samples_leaf = 48`, `learning_rate = 0.10`) using Huber-clipped pseudo-residuals (`75th` percentile threshold).

### Measured development comparison (`reports/model_development.json`)

Evaluated across all six validation months (`13,104` scored station-hours per method and lag; `72` DST transition-day station-hours excluded per lag; `0` chronology violations):

| Assumed lag (days) | Method | Scored hours | MAE | RMSE | WAPE |
| ---: | --- | ---: | ---: | ---: | ---: |
| 1 | **`pooled_boosted_trees`** | 13,104 | **357.76** | **701.89** | **10.07%** |
| 1 | `seasonal_latest` | 13,104 | 368.47 | 800.97 | 10.37% |
| 1 | `seasonal_mean4` | 13,104 | 382.37 | 736.56 | 10.76% |
| 3 | **`pooled_boosted_trees`** | 13,104 | **360.45** | **705.67** | **10.15%** |
| 3 | `seasonal_latest` | 13,104 | 368.47 | 800.97 | 10.37% |
| 3 | `seasonal_mean4` | 13,104 | 382.37 | 736.56 | 10.76% |
| 7 | `pooled_boosted_trees` | 13,104 | 406.56 | 786.42 | 11.44% |
| 7 | `seasonal_latest` | 13,104 | 445.55 | 961.37 | 12.54% |
| 7 | **`seasonal_mean4`** | 13,104 | **404.94** | **773.67** | **11.40%** |
| 14 | `pooled_boosted_trees` | 13,104 | 431.17 | 815.15 | 12.14% |
| 14 | `seasonal_latest` | 13,104 | 495.85 | 1,023.01 | 13.96% |
| 14 | **`seasonal_mean4`** | 13,104 | **411.76** | **782.63** | **11.59%** |

- At **1-day and 3-day reporting delays**, `pooled_boosted_trees` outperforms both `seasonal_latest` and `seasonal_mean4` simultaneously on MAE, RMSE, and WAPE across all three station complexes.
- At **7-day and 14-day reporting delays**, `pooled_boosted_trees` reduces MAE by **38.98** and **64.68** entries/hour relative to `seasonal_latest`, while the simple unweighted four-week average (`seasonal_mean4`) achieves slightly lower MAE/RMSE because residual tree splits fit before the 1–2 week unpublished label gap experience mild seasonal drift.
- The reproducible Google Colab-compatible experiment notebook [`notebooks/ridership_forecast_experiment.ipynb`](notebooks/ridership_forecast_experiment.ipynb) installs pinned dependencies, loads or downloads the verified dataset, executes the leakage checks and model comparison, renders diagnostic charts, and verifies [`reports/model_development.json`](reports/model_development.json).

## Past-residual prediction interval calibration and slice error walkthrough

```sh
python calibrate.py data/processed/model-development/predictions.parquet \
  --calibration-report reports/calibration_development.json \
  --slice-report reports/slice_error_walkthrough.json
```

`calibrate.py` calibrates empirical signed-residual quantile intervals (`50%`, `80%`, and `90%` nominal coverage, non-negative clamped) for each validation fold $f_k$ ($k \ge 2$, `10,944` scored station-hours per method/lag) strictly from preceding validation folds with `target_day <= fold_start_observed_through` (`390,528` cutoff assertions verified, `0` leakage violations, October 2026 holdout strictly refused), stratified by `(station_complex_id, day_type, hour_band)`:

| Assumed lag (days) | Method | p50 Empirical Coverage | p80 Empirical Coverage | p90 Empirical Coverage | p80 Mean Width | p90 Mean Width | p80 Winkler Score | p90 Winkler Score |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | **`pooled_boosted_trees`** | 40.72% | 68.69% | 79.93% | **810.06** | **1,288.20** | **2,569.93** | **3,721.55** |
| 1 | `seasonal_latest` | 43.59% | 72.03% | 83.28% | 819.40 | 1,383.94 | 2,735.20 | 4,027.89 |
| 1 | `seasonal_mean4` | 40.60% | 68.07% | 78.89% | 863.51 | 1,358.67 | 2,692.55 | 3,943.35 |

- Because validation folds occur at 3-to-4-month intervals across changing seasonal volume regimes, empirical coverage on sparse preceding-fold residuals exhibits ~7–11 percentage points of under-coverage across all methods, while `pooled_boosted_trees` achieves the best (lowest) **Winkler proper interval score** across `p50`, `p80`, and `p90` at 1-day and 3-day lags due to smaller tail excursions.
- [`reports/slice_error_walkthrough.json`](reports/slice_error_walkthrough.json) quantifies reporting-lag degradation (`lag 1 -> 3 -> 7 -> 14` days: `pooled_boosted_trees` MAE `+0.00`, `+2.69`, `+48.80`, `+73.41` entries/hour; WAPE `+0.00`, `+0.076`, `+1.374`, `+2.067` percentage points) and joint `(station_complex_id, day_type, hour_band)` error distributions across Union Square (`602`), Times Square (`611`), and Fulton Street (`628`).
