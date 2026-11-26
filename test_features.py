import copy
from datetime import date, datetime, timedelta
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from aggregate import aggregate, calendar_hours
from backtest import Observation
from features import (
    FEATURE_COLUMNS,
    SOURCE_DATE_COLUMNS,
    FeatureHistory,
    build_feature_store,
    validate_feature_row,
    validate_training_cut,
    verify_cutoff_invariance,
)
from forecast_window import next_day_window
from ingest import Selection, ValidationError, download
from test_backtest import config, observation
from test_ingest import FakeSource, row


def complete_panel_observations(start_day, end_day, station="611"):
    observations = []
    for stamp, _, _, _, excluded in calendar_hours(start_day, end_day):
        if excluded:
            observations.append(Observation(station, stamp, None, "excluded_dst_day"))
        else:
            value = float(stamp.day * 100 + stamp.hour)
            observations.append(Observation(station, stamp, value, "observed"))
    return observations


class FeatureTests(unittest.TestCase):
    def test_cutoff_aligned_weekly_recent_adjacent_and_daily_total_features(self):
        obs = complete_panel_observations(date(2025, 1, 1), date(2025, 3, 18))
        history = FeatureHistory(obs)

        # Target: Monday 2025-03-17 hour 08 (issued Sunday 2025-03-16).
        # Under lag=1, observed_through is 2025-03-15.
        # Note: Sunday 2025-03-09 was a spring DST transition day (excluded).
        r1 = history.build_row("611", date(2025, 3, 17), 8, 1)
        self.assertEqual(r1["observed_through"], "2025-03-15")
        self.assertEqual(r1["same_weekday_newest_date"], "2025-03-10")
        self.assertEqual(r1["same_weekday_oldest_date"], "2025-02-17")
        self.assertEqual(r1["same_weekday_count4"], 4)
        self.assertEqual(r1["same_weekday_age_days"], 7)
        self.assertEqual(r1["same_weekday_lag1"], 1008.0)  # March 10 hour 8
        self.assertEqual(r1["same_weekday_lag2"], 308.0)   # March 3 hour 8
        self.assertEqual(r1["same_weekday_lag3"], 2408.0)  # Feb 24 hour 8
        self.assertEqual(r1["same_weekday_lag4"], 1708.0)  # Feb 17 hour 8
        self.assertEqual(r1["same_weekday_trend"], 1008.0 - 308.0)
        # Trailing 7 days [2025-03-09..2025-03-15] has 6 eligible days because 2025-03-09 is excluded DST.
        self.assertEqual(r1["recent_same_hour_count7"], 6)
        self.assertEqual(r1["recent_same_hour_oldest_date"], "2025-03-10")
        self.assertEqual(r1["recent_same_hour_newest_date"], "2025-03-15")
        self.assertEqual(r1["recent_same_hour_latest"], 1508.0)
        # Adjacent hours on 2025-03-15 around hour 8 are hour 7 (1507.0) and hour 9 (1509.0) -> mean 1508.0.
        self.assertEqual(r1["recent_same_hour_adjacent_mean"], 1508.0)
        # Full 24h total on 2025-03-15: sum(1500 + h for h in 0..23) = 24*1500 + 276 = 36276.0.
        self.assertEqual(r1["cutoff_full_day_date"], "2025-03-15")
        self.assertEqual(r1["cutoff_day_total_24h"], 24 * 1500.0 + sum(range(24)))
        self.assertTrue(r1["feature_ready"])
        self.assertTrue(r1["eligible_target"])

        # Under lag=7, observed_through is 2025-03-09 (excluded DST day!).
        # So Monday 2025-03-10 is after cutoff, and 2025-03-09 itself is excluded DST.
        r7 = history.build_row("611", date(2025, 3, 17), 8, 7)
        self.assertEqual(r7["observed_through"], "2025-03-09")
        self.assertEqual(r7["same_weekday_newest_date"], "2025-03-03")
        self.assertEqual(r7["same_weekday_age_days"], 14)
        self.assertEqual(r7["same_weekday_lag1"], 308.0)
        self.assertEqual(r7["recent_same_hour_newest_date"], "2025-03-08")
        self.assertEqual(r7["cutoff_full_day_date"], "2025-03-08")
        self.assertLessEqual(r7["feature_newest_source_date"], "2025-03-08")

    def test_canary_perturbation_and_explicit_leakage_rejection(self):
        obs = complete_panel_observations(date(2025, 1, 1), date(2025, 3, 25))
        for lag in (1, 3, 7, 14):
            row_checked = verify_cutoff_invariance(obs, "611", date(2025, 3, 17), 8, lag)
            self.assertTrue(row_checked["feature_ready"])

        history = FeatureHistory(obs)
        valid = history.build_row("611", date(2025, 3, 17), 8, 7)

        # Injecting any source date after observed_through (2025-03-09) must be rejected.
        for col in SOURCE_DATE_COLUMNS:
            leaked = copy.deepcopy(valid)
            leaked[col] = "2025-03-10"
            with self.assertRaisesRegex(ValidationError, "unavailable observation after cutoff"):
                validate_feature_row(leaked, dst_days=history.dst_days)

        # Injecting an excluded DST date (2025-03-09) into any source date column must be rejected.
        for col in SOURCE_DATE_COLUMNS:
            dst_leaked = copy.deepcopy(valid)
            dst_leaked[col] = "2025-03-09"
            with self.assertRaisesRegex(ValidationError, "excluded DST date"):
                validate_feature_row(dst_leaked, dst_days=history.dst_days)

        # Violating same_weekday_age_days <= lag_days must be rejected.
        bad_age = copy.deepcopy(valid)
        bad_age["same_weekday_age_days"] = 7
        with self.assertRaises(ValidationError):
            validate_feature_row(bad_age, dst_days=history.dst_days)

        # Poisoning verify_cutoff_invariance by tampering with baseline row detection:
        # verify_cutoff_invariance itself checks that future changes never leak.
        tampered_dst = copy.deepcopy(valid)
        tampered_dst["target_status"] = "excluded_dst_day"
        tampered_dst["target"] = None
        tampered_dst["eligible_target"] = False
        with self.assertRaisesRegex(ValidationError, "excluded DST target day"):
            validate_feature_row(tampered_dst, dst_days=history.dst_days)

    def test_training_cut_rejects_unpublished_labels_and_features(self):
        obs = complete_panel_observations(date(2025, 1, 1), date(2025, 3, 15))
        history = FeatureHistory(obs)
        # Suppose validation fold starts 2025-03-01 (first issuance 2025-02-28).
        # Under lag=7, first_observed_through is 2025-02-21.
        cutoff = date(2025, 2, 21)
        train_ok = [
            history.build_row("611", date(2025, 2, 20), 8, 7),
            history.build_row("611", date(2025, 2, 21), 8, 7),
        ]
        val_ok = [
            history.build_row("611", date(2025, 3, 1), 8, 7),
            history.build_row("611", date(2025, 3, 2), 8, 7),
        ]
        validate_training_cut(train_ok, val_ok, cutoff, dst_days=history.dst_days)

        # A row from the unpublished lag gap (target_day = 2025-02-22 > cutoff 2025-02-21)
        # must be rejected if included in train_rows!
        unpublished_train = history.build_row("611", date(2025, 2, 22), 8, 7)
        with self.assertRaisesRegex(ValidationError, "unpublished training target"):
            validate_training_cut(train_ok + [unpublished_train], val_ok, cutoff, dst_days=history.dst_days)

        # A validation row with target_day <= cutoff must be rejected.
        with self.assertRaisesRegex(ValidationError, "does not strictly follow training cutoff"):
            validate_training_cut(train_ok, [train_ok[-1]], cutoff, dst_days=history.dst_days)

    def test_missing_vs_zero_and_dst_target_exclusion(self):
        obs = []
        for stamp, _, _, _, excluded in calendar_hours(date(2025, 1, 1), date(2025, 3, 12)):
            if excluded:
                obs.append(Observation("611", stamp, None, "excluded_dst_day"))
            elif stamp.date() == date(2025, 2, 10) and stamp.hour == 8:
                obs.append(Observation("611", stamp, 0.0, "observed"))
            elif stamp.date() == date(2025, 2, 17) and stamp.hour == 8:
                obs.append(Observation("611", stamp, None, "missing"))
            else:
                obs.append(Observation("611", stamp, 50.0, "observed"))

        history = FeatureHistory(obs)
        # For Monday 2025-02-17 hour 8 (missing target), target is None, eligible_target is False,
        # and under lag=1 (cutoff 2025-02-15), Monday 2025-02-10's observed 0.0 is preserved as 0.0.
        missing_target_row = history.build_row("611", date(2025, 2, 17), 8, 1)
        self.assertIsNone(missing_target_row["target"])
        self.assertFalse(missing_target_row["eligible_target"])
        self.assertEqual(missing_target_row["same_weekday_lag1"], 0.0)
        self.assertTrue(missing_target_row["feature_ready"])

        # For Tuesday 2025-02-18 hour 8 under lag=1 (cutoff 2025-02-16) vs Wednesday 2025-02-19 under lag=1 (cutoff 2025-02-17):
        # On 2025-02-17, hour 8 is missing, so 2025-02-17 has only 23 observed hours and cannot be used for cutoff_day_total_24h;
        # it must fall back to 2025-02-16 (all 24 hours observed).
        wed_row = history.build_row("611", date(2025, 2, 19), 8, 1)
        self.assertEqual(wed_row["observed_through"], "2025-02-17")
        self.assertEqual(wed_row["cutoff_full_day_date"], "2025-02-16")
        self.assertEqual(wed_row["cutoff_day_total_24h"], 24 * 50.0)

        # DST transition target day 2025-03-09 emits no historical features and is neither ready nor eligible.
        dst_row = history.build_row("611", date(2025, 3, 9), 8, 1)
        self.assertEqual(dst_row["target_status"], "excluded_dst_day")
        self.assertFalse(dst_row["eligible_target"])
        self.assertFalse(dst_row["feature_ready"])
        self.assertIsNone(dst_row["same_weekday_lag1"])
        self.assertIsNone(dst_row["feature_newest_source_date"])

    def test_real_feature_store_pipeline_and_holdout_guard(self):
        c = config()
        source = [
            row(f"row-{i}-{h}", f"{day}T{h:02d}:00:00.000", ridership=str(10 + h), transfers="0")
            for i, day in enumerate(date(2025, 1, 1) + timedelta(days=n) for n in range(59))
            for h in range(24)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "snapshot"
            panel = root / "panel"
            features_dir = root / "features"
            download(Selection(date(2025, 1, 1), date(2025, 3, 1), ("611",)), snapshot, fetch=FakeSource(source))
            aggregate(snapshot, panel)
            report = build_feature_store(panel, features_dir, c)
            self.assertEqual(report["leakage_violations"], 0)
            self.assertEqual(report["canary_perturbation_checks_passed"], 3)
            self.assertEqual(report["feature_columns"], list(FEATURE_COLUMNS))
            for entry in report["by_fold_and_lag"]:
                self.assertEqual(entry["cutoff_leakage_violations"], 0)
                self.assertEqual(entry["unpublished_gap_rows_excluded"], entry["lag_days"] * 24)
                self.assertEqual(entry["validation_scheduled_rows"], 28 * 24)
                self.assertEqual(entry["validation_scoreable_rows"], 28 * 24)

            with duckdb.connect() as db:
                count = db.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(features_dir / "features.parquet")]
                ).fetchone()[0]
                self.assertEqual(count, report["total_feature_rows"])

            with self.assertRaises(FileExistsError):
                build_feature_store(panel, features_dir, c)

            # Tampering with panel selection into calibration/holdout is refused.
            audit_path = panel / "audit.json"
            audit = json.loads(audit_path.read_text())
            audit["selection"]["end_exclusive"] = "2025-04-01"
            audit_path.write_text(json.dumps(audit))
            with self.assertRaisesRegex(ValidationError, "calibration/holdout"):
                build_feature_store(panel, root / "features-bad", c)


if __name__ == "__main__":
    unittest.main()
