"""Chronological feature construction and publication-cutoff leakage guards."""
import argparse
from bisect import bisect_left, bisect_right
from datetime import date, timedelta
import json
import math
from pathlib import Path
import shutil
import tempfile

import duckdb

from aggregate import calendar_hours, sql_path
from backtest import PEAK_HOURS, Observation, chronological_folds, load_panel, next_month, validate_config
from forecast_window import next_day_window
from ingest import ValidationError, sha256

FEATURE_COLUMNS = (
    "day_of_week",
    "is_weekend",
    "is_peak_hour",
    "month",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "same_weekday_lag1",
    "same_weekday_lag2",
    "same_weekday_lag3",
    "same_weekday_lag4",
    "same_weekday_count4",
    "same_weekday_mean4",
    "same_weekday_min4",
    "same_weekday_max4",
    "same_weekday_std4",
    "same_weekday_trend",
    "same_weekday_age_days",
    "recent_same_hour_latest",
    "recent_same_hour_mean7",
    "recent_same_hour_count7",
    "recent_same_hour_adjacent_mean",
    "cutoff_day_total_24h",
)

SOURCE_DATE_COLUMNS = (
    "same_weekday_oldest_date",
    "same_weekday_newest_date",
    "recent_same_hour_oldest_date",
    "recent_same_hour_newest_date",
    "cutoff_full_day_date",
    "feature_oldest_source_date",
    "feature_newest_source_date",
)

FEATURE_ROW_TYPES = {
    "fold_start": "DATE",
    "split": "VARCHAR",
    "station_complex_id": "VARCHAR",
    "target_day": "DATE",
    "hour": "INTEGER",
    "issued_on": "DATE",
    "lag_days": "INTEGER",
    "observed_through": "DATE",
    "target": "DOUBLE",
    "target_status": "VARCHAR",
    "eligible_target": "BOOLEAN",
    "feature_ready": "BOOLEAN",
    "day_of_week": "INTEGER",
    "is_weekend": "INTEGER",
    "is_peak_hour": "INTEGER",
    "month": "INTEGER",
    "hour_sin": "DOUBLE",
    "hour_cos": "DOUBLE",
    "dow_sin": "DOUBLE",
    "dow_cos": "DOUBLE",
    "same_weekday_lag1": "DOUBLE",
    "same_weekday_lag2": "DOUBLE",
    "same_weekday_lag3": "DOUBLE",
    "same_weekday_lag4": "DOUBLE",
    "same_weekday_count4": "INTEGER",
    "same_weekday_mean4": "DOUBLE",
    "same_weekday_min4": "DOUBLE",
    "same_weekday_max4": "DOUBLE",
    "same_weekday_std4": "DOUBLE",
    "same_weekday_trend": "DOUBLE",
    "same_weekday_age_days": "INTEGER",
    "recent_same_hour_latest": "DOUBLE",
    "recent_same_hour_mean7": "DOUBLE",
    "recent_same_hour_count7": "INTEGER",
    "recent_same_hour_adjacent_mean": "DOUBLE",
    "cutoff_day_total_24h": "DOUBLE",
    "same_weekday_oldest_date": "DATE",
    "same_weekday_newest_date": "DATE",
    "recent_same_hour_oldest_date": "DATE",
    "recent_same_hour_newest_date": "DATE",
    "cutoff_full_day_date": "DATE",
    "feature_oldest_source_date": "DATE",
    "feature_newest_source_date": "DATE",
}


def _as_date(value):
    if value is None or isinstance(value, date):
        return value
    return date.fromisoformat(value)


def validate_feature_row(row, dst_days=(), bounds=None):
    """Reject any feature vector that leaks post-cutoff, DST, or out-of-bounds data."""
    target_day = _as_date(row["target_day"])
    issued_on = _as_date(row["issued_on"])
    observed_through = _as_date(row["observed_through"])
    lag_days = row["lag_days"]
    if type(lag_days) is not int or lag_days < 1:
        raise ValidationError("lag_days must be a positive integer")
    window = next_day_window(issued_on, lag_days)
    if window.target_day != target_day or window.observed_through != observed_through:
        raise ValidationError("feature row issuance or cutoff window mismatch")
    if not (observed_through < issued_on < target_day):
        raise ValidationError("invalid chronological ordering in feature row")
    if bounds is not None:
        start, end_exclusive = bounds
        if not (start <= target_day < end_exclusive):
            raise ValidationError("feature row target_day extends outside allowed period")

    status = row["target_status"]
    if status not in ("observed", "missing", "excluded_dst_day"):
        raise ValidationError("invalid target_status in feature row")
    if status == "observed":
        if row["target"] is None or not math.isfinite(row["target"]) or row["target"] < 0:
            raise ValidationError("observed target must be finite and nonnegative")
        if row["eligible_target"] is not True:
            raise ValidationError("observed target must be marked eligible_target")
    else:
        if row["target"] is not None or row["eligible_target"] is not False:
            raise ValidationError("missing or DST target must remain null and ineligible")

    if status == "excluded_dst_day":
        if row["feature_ready"] is not False:
            raise ValidationError("excluded DST target day cannot be marked feature_ready")
        for col in SOURCE_DATE_COLUMNS:
            if row[col] is not None:
                raise ValidationError("excluded DST target day must not emit historical source dates")
        for col in (
            "same_weekday_lag1", "same_weekday_lag2", "same_weekday_lag3", "same_weekday_lag4",
            "same_weekday_mean4", "same_weekday_min4", "same_weekday_max4", "same_weekday_std4",
            "same_weekday_trend", "same_weekday_age_days", "recent_same_hour_latest",
            "recent_same_hour_mean7", "recent_same_hour_adjacent_mean", "cutoff_day_total_24h",
        ):
            if row[col] is not None:
                raise ValidationError("excluded DST target day must not emit historical feature values")
        return

    dst_set = {_as_date(d) for d in dst_days}
    source_dates = []
    for col in SOURCE_DATE_COLUMNS:
        d = _as_date(row[col])
        if d is None:
            continue
        if not window.can_use_observation(d) or d > observed_through or d >= issued_on or d >= target_day:
            raise ValidationError(f"unavailable observation after cutoff reached {col}: {d} > {observed_through}")
        if d in dst_set:
            raise ValidationError(f"excluded DST date reached {col}: {d}")
        if bounds is not None and not (bounds[0] <= d < bounds[1]):
            raise ValidationError(f"out-of-bounds date reached {col}: {d}")
        source_dates.append(d)

    w_old, w_new = _as_date(row["same_weekday_oldest_date"]), _as_date(row["same_weekday_newest_date"])
    if (w_old is None) != (w_new is None) or (w_old is not None and w_old > w_new):
        raise ValidationError("inconsistent same-weekday source date range")
    r_old, r_new = _as_date(row["recent_same_hour_oldest_date"]), _as_date(row["recent_same_hour_newest_date"])
    if (r_old is None) != (r_new is None) or (r_old is not None and r_old > r_new):
        raise ValidationError("inconsistent recent same-hour source date range")
    if r_old is not None and (observed_through - r_old).days > 6:
        raise ValidationError("recent same-hour window exceeds seven calendar days")

    f_old, f_new = _as_date(row["feature_oldest_source_date"]), _as_date(row["feature_newest_source_date"])
    base_dates = [d for d in (w_old, w_new, r_old, r_new, _as_date(row["cutoff_full_day_date"])) if d is not None]
    if not base_dates:
        if f_old is not None or f_new is not None or row["feature_ready"]:
            raise ValidationError("empty source dates cannot produce nonnull feature bounds or ready status")
    else:
        if f_old != min(base_dates) or f_new != max(base_dates):
            raise ValidationError("feature_oldest_source_date/feature_newest_source_date do not match source dates")

    if w_new is not None:
        if (target_day - w_new).days != row["same_weekday_age_days"] or w_new.weekday() != target_day.weekday():
            raise ValidationError("same-weekday reference weekday or age mismatch")
        if row["same_weekday_age_days"] <= lag_days:
            raise ValidationError("same-weekday reference violates publication lag")

    expected_ready = (row["same_weekday_count4"] >= 1 and row["recent_same_hour_count7"] >= 1)
    if row["feature_ready"] != expected_ready:
        raise ValidationError("feature_ready flag inconsistent with reference counts")


def validate_training_cut(train_rows, validation_rows, cutoff_day, dst_days=(), bounds=None):
    """Verify that a model training split at cutoff_day contains zero unpublished targets or features."""
    cutoff_day = _as_date(cutoff_day)
    for row in train_rows:
        validate_feature_row(row, dst_days=dst_days, bounds=bounds)
        if not row["eligible_target"] or not row["feature_ready"] or row["target"] is None:
            raise ValidationError("ineligible or incomplete row included in model training set")
        t_day = _as_date(row["target_day"])
        if t_day > cutoff_day:
            raise ValidationError(f"unpublished training target {t_day} beyond cutoff {cutoff_day} reached model")
        f_new = _as_date(row["feature_newest_source_date"])
        if f_new is None or f_new > cutoff_day or f_new >= t_day:
            raise ValidationError("training row feature source date exceeds cutoff")
    for row in validation_rows:
        validate_feature_row(row, dst_days=dst_days, bounds=bounds)
        v_day = _as_date(row["target_day"])
        if v_day <= cutoff_day:
            raise ValidationError(f"validation target {v_day} does not strictly follow training cutoff {cutoff_day}")
        v_cut = _as_date(row["observed_through"])
        f_new = _as_date(row["feature_newest_source_date"])
        if f_new is not None and f_new > v_cut:
            raise ValidationError("validation feature source date exceeds row issuance cutoff")


class FeatureHistory:
    """Index observed station-hours for O(log N) cutoff-bounded feature extraction."""

    def __init__(self, observations):
        self.by_station_hour = {}
        self.dates_by_station_hour = {}
        self.by_station_day = {}
        self.actuals = {}
        self.dst_days = set()
        self.min_date = None
        self.max_date = None
        seen = set()

        for obs in observations:
            key = (obs.station, obs.stamp)
            if key in seen:
                raise ValidationError("duplicate station-hour in panel")
            seen.add(key)
            day = obs.stamp.date()
            hour = obs.stamp.hour
            self.actuals[(obs.station, day, hour)] = obs
            if self.min_date is None or day < self.min_date:
                self.min_date = day
            if self.max_date is None or day > self.max_date:
                self.max_date = day

            if obs.status == "excluded_dst_day":
                if obs.target is not None:
                    raise ValidationError("excluded DST observation must have null target")
                self.dst_days.add(day)
                continue
            if obs.status == "missing":
                if obs.target is not None:
                    raise ValidationError("missing observation must have null target")
                continue
            if obs.status != "observed" or obs.target is None or not math.isfinite(obs.target) or obs.target < 0:
                raise ValidationError("invalid observed station-hour target")

            self.by_station_hour.setdefault((obs.station, hour), []).append((day, float(obs.target)))
            self.by_station_day.setdefault((obs.station, day), {})[hour] = float(obs.target)

        for key, rows in self.by_station_hour.items():
            rows.sort()
            self.dates_by_station_hour[key] = [day for day, _ in rows]

    def weekly_references(self, station, hour, window):
        """Return up to four observed same-weekday references at/before window.observed_through, newest first."""
        rows = self.by_station_hour.get((station, hour), [])
        dates = self.dates_by_station_hour.get((station, hour), [])
        stop = bisect_right(dates, window.observed_through)
        refs = []
        for day, value in reversed(rows[:stop]):
            if day.weekday() == window.target_day.weekday():
                if not window.can_use_observation(day) or day >= window.target_day or day in self.dst_days:
                    raise ValidationError("unavailable observation reached weekly feature builder")
                refs.append((day, value))
                if len(refs) == 4:
                    break
        return refs

    def recent_same_hour(self, station, hour, window):
        """Return observed same-hour records in [observed_through - 6 days, observed_through], oldest first."""
        rows = self.by_station_hour.get((station, hour), [])
        dates = self.dates_by_station_hour.get((station, hour), [])
        floor = window.observed_through - timedelta(days=6)
        start = bisect_left(dates, floor)
        stop = bisect_right(dates, window.observed_through)
        recent = rows[start:stop]
        for day, _ in recent:
            if day < floor or not window.can_use_observation(day) or day >= window.target_day or day in self.dst_days:
                raise ValidationError("unavailable observation reached recent same-hour feature builder")
        return recent

    def adjacent_hour_mean(self, station, day, hour, window):
        """Mean of observed adjacent hours (hour-1, hour+1) on a verified historical date <= observed_through."""
        if day is None:
            return None
        if not window.can_use_observation(day) or day >= window.target_day or day in self.dst_days:
            raise ValidationError("unavailable date reached adjacent-hour feature builder")
        hours_map = self.by_station_day.get((station, day), {})
        neighbors = [hours_map[h] for h in (hour - 1, hour + 1) if 0 <= h < 24 and h in hours_map]
        return sum(neighbors) / len(neighbors) if neighbors else None

    def latest_full_day_total(self, station, window):
        """Sum of all 24 observed hours on the newest complete non-DST day in [observed_through - 6 days, observed_through]."""
        floor = window.observed_through - timedelta(days=6)
        if self.min_date is not None and floor < self.min_date:
            floor = self.min_date
        candidate = window.observed_through
        while candidate >= floor:
            if candidate not in self.dst_days:
                hours_map = self.by_station_day.get((station, candidate), {})
                if len(hours_map) == 24:
                    if not window.can_use_observation(candidate) or candidate >= window.target_day:
                        raise ValidationError("unavailable date reached daily total feature builder")
                    return candidate, sum(hours_map.values())
            candidate -= timedelta(days=1)
        return None, None

    def build_row(self, station, target_day, hour, lag_days, fold_start=None, split="validation", bounds=None):
        issued_on = target_day - timedelta(days=1)
        window = next_day_window(issued_on, lag_days)
        actual = self.actuals.get((station, target_day, hour))
        if actual is None:
            raise ValidationError(f"panel missing grid cell for station={station} day={target_day} hour={hour}")

        dow = target_day.weekday()
        row = {
            "fold_start": fold_start.isoformat() if isinstance(fold_start, date) else fold_start,
            "split": split,
            "station_complex_id": station,
            "target_day": target_day.isoformat(),
            "hour": hour,
            "issued_on": issued_on.isoformat(),
            "lag_days": lag_days,
            "observed_through": window.observed_through.isoformat(),
            "target": actual.target,
            "target_status": actual.status,
            "eligible_target": actual.status == "observed" and actual.target is not None,
            "feature_ready": False,
            "day_of_week": dow,
            "is_weekend": 1 if dow >= 5 else 0,
            "is_peak_hour": 1 if hour in PEAK_HOURS else 0,
            "month": target_day.month,
            "hour_sin": math.sin(2.0 * math.pi * hour / 24.0),
            "hour_cos": math.cos(2.0 * math.pi * hour / 24.0),
            "dow_sin": math.sin(2.0 * math.pi * dow / 7.0),
            "dow_cos": math.cos(2.0 * math.pi * dow / 7.0),
            "same_weekday_lag1": None,
            "same_weekday_lag2": None,
            "same_weekday_lag3": None,
            "same_weekday_lag4": None,
            "same_weekday_count4": 0,
            "same_weekday_mean4": None,
            "same_weekday_min4": None,
            "same_weekday_max4": None,
            "same_weekday_std4": None,
            "same_weekday_trend": None,
            "same_weekday_age_days": None,
            "recent_same_hour_latest": None,
            "recent_same_hour_mean7": None,
            "recent_same_hour_count7": 0,
            "recent_same_hour_adjacent_mean": None,
            "cutoff_day_total_24h": None,
            "same_weekday_oldest_date": None,
            "same_weekday_newest_date": None,
            "recent_same_hour_oldest_date": None,
            "recent_same_hour_newest_date": None,
            "cutoff_full_day_date": None,
            "feature_oldest_source_date": None,
            "feature_newest_source_date": None,
        }

        if actual.status != "excluded_dst_day":
            weekly = self.weekly_references(station, hour, window)
            if weekly:
                w_vals = [val for _, val in weekly]
                w_mean = sum(w_vals) / len(w_vals)
                row["same_weekday_lag1"] = w_vals[0]
                if len(w_vals) >= 2:
                    row["same_weekday_lag2"] = w_vals[1]
                    row["same_weekday_trend"] = w_vals[0] - w_vals[1]
                    row["same_weekday_std4"] = math.sqrt(sum((v - w_mean) ** 2 for v in w_vals) / len(w_vals))
                if len(w_vals) >= 3:
                    row["same_weekday_lag3"] = w_vals[2]
                if len(w_vals) >= 4:
                    row["same_weekday_lag4"] = w_vals[3]
                row["same_weekday_count4"] = len(w_vals)
                row["same_weekday_mean4"] = w_mean
                row["same_weekday_min4"] = min(w_vals)
                row["same_weekday_max4"] = max(w_vals)
                row["same_weekday_age_days"] = (target_day - weekly[0][0]).days
                row["same_weekday_oldest_date"] = weekly[-1][0].isoformat()
                row["same_weekday_newest_date"] = weekly[0][0].isoformat()

            recent = self.recent_same_hour(station, hour, window)
            if recent:
                r_vals = [val for _, val in recent]
                newest_recent_day = recent[-1][0]
                row["recent_same_hour_latest"] = r_vals[-1]
                row["recent_same_hour_mean7"] = sum(r_vals) / len(r_vals)
                row["recent_same_hour_count7"] = len(r_vals)
                row["recent_same_hour_oldest_date"] = recent[0][0].isoformat()
                row["recent_same_hour_newest_date"] = newest_recent_day.isoformat()
                row["recent_same_hour_adjacent_mean"] = self.adjacent_hour_mean(
                    station, newest_recent_day, hour, window
                )

            full_day, full_total = self.latest_full_day_total(station, window)
            if full_day is not None:
                row["cutoff_day_total_24h"] = full_total
                row["cutoff_full_day_date"] = full_day.isoformat()

            source_dates = [
                d for d in (
                    _as_date(row["same_weekday_oldest_date"]),
                    _as_date(row["same_weekday_newest_date"]),
                    _as_date(row["recent_same_hour_oldest_date"]),
                    _as_date(row["recent_same_hour_newest_date"]),
                    full_day,
                )
                if d is not None
            ]
            if source_dates:
                row["feature_oldest_source_date"] = min(source_dates).isoformat()
                row["feature_newest_source_date"] = max(source_dates).isoformat()
            row["feature_ready"] = (row["same_weekday_count4"] >= 1 and row["recent_same_hour_count7"] >= 1)

        validate_feature_row(row, dst_days=self.dst_days, bounds=bounds)
        return row


def verify_cutoff_invariance(observations, station, target_day, hour, lag_days):
    """Prove that mutating or deleting observations after observed_through cannot alter features."""
    issued_on = target_day - timedelta(days=1)
    window = next_day_window(issued_on, lag_days)
    baseline_history = FeatureHistory(observations)
    baseline = baseline_history.build_row(station, target_day, hour, lag_days)

    poisoned = []
    for obs in observations:
        if obs.stamp.date() > window.observed_through and obs.status != "excluded_dst_day":
            poisoned.append(Observation(obs.station, obs.stamp, 1e12, "observed"))
        else:
            poisoned.append(obs)
    perturbed = FeatureHistory(poisoned).build_row(station, target_day, hour, lag_days)

    for key in FEATURE_ROW_TYPES:
        if key in ("target", "target_status", "eligible_target"):
            continue
        if baseline[key] != perturbed[key]:
            raise ValidationError(f"future observation perturbation changed {key}: {baseline[key]} vs {perturbed[key]}")
    return baseline


def development_feature_rows(observations, config):
    """Yield validated feature rows across the development period after initial warm-up."""
    dates, months = validate_config(config)
    bounds = (dates["development_start"], dates["development_end_exclusive"])
    if any(not bounds[0] <= obs.stamp.date() < bounds[1] for obs in observations):
        raise ValidationError("non-development observation refused")
    history = FeatureHistory(observations)
    val_ranges = [(m, next_month(m)) for m in months]

    for lag in config["publication_lag_days"]:
        # First target day whose issuance cutoff is at/after development_start:
        # observed_through = (target_day - 1) - lag >= development_start
        first_target = dates["development_start"] + timedelta(days=lag + 1)
        day = first_target
        while day < dates["development_end_exclusive"]:
            fold_start = None
            split = "history"
            for m_start, m_end in val_ranges:
                if m_start <= day < m_end:
                    fold_start = m_start.isoformat()
                    split = "validation"
                    break
            for station in config["station_complex_ids"]:
                for hour in range(24):
                    yield history.build_row(
                        station, day, hour, lag, fold_start=fold_start, split=split, bounds=bounds
                    )
            day += timedelta(days=1)


def fold_split_summary(db, config):
    """Audit each (fold_start, lag_days) training and validation split under the fold's issuance cutoff."""
    _, months = validate_config(config)
    summaries = []
    for month in months:
        m_start = month.isoformat()
        m_end = next_month(month).isoformat()
        first_issued = month - timedelta(days=1)
        for lag in config["publication_lag_days"]:
            first_cutoff = next_day_window(first_issued, lag).observed_through.isoformat()
            row = db.execute(
                """
                SELECT
                    count(*) FILTER (
                        WHERE target_day <= ? AND eligible_target AND feature_ready
                    ) AS train_eligible_rows,
                    max(target_day) FILTER (
                        WHERE target_day <= ? AND eligible_target AND feature_ready
                    ) AS train_newest_target_day,
                    max(feature_newest_source_date) FILTER (
                        WHERE target_day <= ? AND eligible_target AND feature_ready
                    ) AS train_newest_feature_source_date,
                    count(*) FILTER (
                        WHERE target_day > ? AND target_day < ?
                    ) AS unpublished_gap_rows_excluded_from_fold_start_train,
                    count(*) FILTER (
                        WHERE fold_start = ? AND split = 'validation'
                    ) AS validation_scheduled_rows,
                    count(*) FILTER (
                        WHERE fold_start = ? AND split = 'validation' AND eligible_target AND feature_ready
                    ) AS validation_scoreable_rows,
                    count(*) FILTER (
                        WHERE fold_start = ? AND split = 'validation' AND target_status = 'excluded_dst_day'
                    ) AS validation_dst_excluded_rows,
                    count(*) FILTER (
                        WHERE feature_newest_source_date > observed_through
                           OR same_weekday_newest_date > observed_through
                           OR recent_same_hour_newest_date > observed_through
                           OR cutoff_full_day_date > observed_through
                    ) AS cutoff_leakage_violations
                FROM features
                WHERE lag_days = ?
                """,
                [first_cutoff, first_cutoff, first_cutoff, first_cutoff, m_start, m_start, m_start, m_start, lag],
            ).fetchone()
            if row[7] != 0:
                raise ValidationError("feature cutoff leakage detected in SQL fold audit")
            summaries.append({
                "fold_start": m_start,
                "validation_end_exclusive": m_end,
                "lag_days": lag,
                "fold_start_issued_on": first_issued.isoformat(),
                "fold_start_observed_through": first_cutoff,
                "train_eligible_rows_at_fold_start": row[0],
                "train_newest_target_day": row[1].isoformat() if isinstance(row[1], date) else row[1],
                "train_newest_feature_source_date": row[2].isoformat() if isinstance(row[2], date) else row[2],
                "unpublished_gap_rows_excluded": row[3],
                "validation_scheduled_rows": row[4],
                "validation_scoreable_rows": row[5],
                "validation_dst_excluded_rows": row[6],
                "cutoff_leakage_violations": row[7],
            })
    return summaries


def build_feature_store(panel_dir, destination, config):
    """Build and verify the development feature Parquet artifact and audit report."""
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError("feature output directory already exists")
    observations, audit = load_panel(panel_dir, config)
    if (
        audit["selection"]["start"] != config["development_start"]
        or audit["selection"]["end_exclusive"] != config["development_end_exclusive"]
    ):
        raise ValidationError("full declared development panel required for feature generation")

    # Run canary perturbation invariance checks across each station and declared lag before writing.
    history = FeatureHistory(observations)
    canary_checks = 0
    sample_day = date.fromisoformat(config["validation_month_starts"][0]) + timedelta(days=15)
    for lag in config["publication_lag_days"]:
        for station in config["station_complex_ids"]:
            verify_cutoff_invariance(observations, station, sample_day, 8, lag)
            canary_checks += 1

    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".features-", dir=destination.parent))
    try:
        with (stage / "features.jsonl").open("w") as stream:
            for row in development_feature_rows(observations, config):
                stream.write(json.dumps(row, allow_nan=False) + "\n")

        with duckdb.connect() as db:
            columns = "{" + ",".join(f"'{name}':'{kind}'" for name, kind in FEATURE_ROW_TYPES.items()) + "}"
            db.execute(
                f"CREATE TABLE features AS SELECT * FROM read_json(?, format='newline_delimited', columns={columns})",
                [str(stage / "features.jsonl")],
            )
            dst_dates_sql = ",".join(f"DATE '{d.isoformat()}'" for d in sorted(history.dst_days))
            dst_clause = (
                f"OR same_weekday_oldest_date IN ({dst_dates_sql}) "
                f"OR same_weekday_newest_date IN ({dst_dates_sql}) "
                f"OR recent_same_hour_oldest_date IN ({dst_dates_sql}) "
                f"OR recent_same_hour_newest_date IN ({dst_dates_sql}) "
                f"OR cutoff_full_day_date IN ({dst_dates_sql}) "
                f"OR feature_oldest_source_date IN ({dst_dates_sql}) "
                f"OR feature_newest_source_date IN ({dst_dates_sql})"
                if dst_dates_sql
                else ""
            )
            violations = db.execute(
                f"""
                SELECT count(*) FROM features
                WHERE feature_newest_source_date > observed_through
                   OR same_weekday_newest_date > observed_through
                   OR recent_same_hour_newest_date > observed_through
                   OR cutoff_full_day_date > observed_through
                   OR observed_through >= issued_on
                   OR issued_on >= target_day
                   OR target_day >= ?
                   OR (target_status = 'excluded_dst_day' AND (feature_ready OR eligible_target OR feature_newest_source_date IS NOT NULL))
                   OR (target_status = 'missing' AND (eligible_target OR target IS NOT NULL))
                   {dst_clause}
                """,
                [config["development_end_exclusive"]],
            ).fetchone()[0]
            if violations:
                raise ValidationError(f"SQL feature leakage audit failed with {violations} violations")

            by_lag_cursor = db.execute(
                """
                SELECT
                    lag_days,
                    count(*) AS total_rows,
                    count(*) FILTER (WHERE eligible_target AND feature_ready) AS model_ready_rows,
                    count(*) FILTER (WHERE split = 'validation') AS validation_scheduled_rows,
                    count(*) FILTER (WHERE split = 'validation' AND eligible_target AND feature_ready) AS validation_scoreable_rows,
                    count(*) FILTER (WHERE split = 'validation' AND target_status = 'excluded_dst_day') AS validation_dst_excluded_rows,
                    count(*) FILTER (WHERE target_status = 'missing') AS missing_target_rows,
                    min(same_weekday_age_days) FILTER (WHERE feature_ready) AS min_same_weekday_age_days,
                    max(same_weekday_age_days) FILTER (WHERE feature_ready) AS max_same_weekday_age_days
                FROM features
                GROUP BY lag_days
                ORDER BY lag_days
                """
            )
            by_lag_names = [col[0] for col in by_lag_cursor.description]
            by_lag = [dict(zip(by_lag_names, r)) for r in by_lag_cursor.fetchall()]
            fold_splits = fold_split_summary(db, config)
            total_rows = db.execute("SELECT count(*) FROM features").fetchone()[0]

            db.execute("COPY features TO " + sql_path(stage / "features.parquet") + " (FORMAT PARQUET)")

        (stage / "features.jsonl").unlink()
        parquet_sha = sha256(stage / "features.parquet")
        report = {
            "format_version": 1,
            "study_config": config,
            "folds": chronological_folds(config),
            "feature_columns": list(FEATURE_COLUMNS),
            "source_date_columns": list(SOURCE_DATE_COLUMNS),
            "input_source_rows_sha256": audit["source_rows_sha256"],
            "input_source_version": audit["source_version"],
            "panel_parquet_sha256": audit["artifacts"]["station_hours.parquet"],
            "features_parquet_sha256": parquet_sha,
            "total_feature_rows": total_rows,
            "leakage_violations": violations,
            "canary_perturbation_checks_passed": canary_checks,
            "by_lag": by_lag,
            "by_fold_and_lag": fold_splits,
            "duckdb_version": duckdb.__version__,
            "limitations": [
                "Development period only (2025-01-01 through 2026-08-31); September calibration and October holdout remain untouched.",
                "All historical features use strictly observed station-hours at or before observed_through = issued_on - lag_days.",
                "At each fold start, training labels after fold_start_observed_through are excluded because they are unpublished under the assumed lag.",
                "Missing targets and missing references remain null without zero imputation; entire DST transition days are excluded.",
            ],
        }
        (stage / "feature_audit.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        if destination.exists():
            raise FileExistsError("feature output appeared during run")
        stage.rename(destination)
        return report
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("panel", type=Path)
    parser.add_argument("--config", type=Path, default=Path("study_config.json"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = build_feature_store(args.panel, args.output, json.loads(args.config.read_text()))
    print(
        json.dumps(
            {
                "total_feature_rows": report["total_feature_rows"],
                "leakage_violations": report["leakage_violations"],
                "canary_perturbation_checks_passed": report["canary_perturbation_checks_passed"],
                "by_lag": report["by_lag"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
