"""Unit and integration tests for past-residual interval calibration and slice error walkthroughs."""
from datetime import date, timedelta
import json
from pathlib import Path
import unittest

from calibrate import (
    build_slice_error_walkthrough,
    calibrate_past_residual_intervals,
    empirical_quantile,
    summarize_calibration,
    winkler_score,
)
from ingest import ValidationError


class TestPastResidualCalibrationAndSlices(unittest.TestCase):
    def setUp(self):
        self.config = json.loads(Path("study_config.json").read_text())

    def test_empirical_quantile_and_winkler_score_properties(self):
        vals = [-100.0, -50.0, 0.0, 50.0, 100.0]
        self.assertAlmostEqual(empirical_quantile(vals, 0.50), 0.0)
        self.assertAlmostEqual(empirical_quantile(vals, 0.0), -100.0)
        self.assertAlmostEqual(empirical_quantile(vals, 1.0), 100.0)
        self.assertAlmostEqual(empirical_quantile(vals, 0.25), -50.0)

        # Inside interval -> score equals width
        self.assertAlmostEqual(winkler_score(150.0, 100.0, 200.0, 0.80), 100.0)
        # Below lower bound by 10 at alpha=0.20 -> width (100) + (2/0.20)*10 = 200
        self.assertAlmostEqual(winkler_score(90.0, 100.0, 200.0, 0.80), 200.0)
        # Above upper bound by 15 at alpha=0.20 -> width (100) + (2/0.20)*15 = 250
        self.assertAlmostEqual(winkler_score(215.0, 100.0, 200.0, 0.80), 250.0)

    def test_chronological_calibration_enforces_cutoff_and_refuses_holdout(self):
        records = []
        # Populate synthetic scoreable predictions for Fold 1 (2025-03-01) and Fold 2 (2025-06-01)
        for fold_start, num_days, offset_err in (
            ("2025-03-01", 30, 40.0),
            ("2025-06-01", 30, 35.0),
        ):
            start_d = date.fromisoformat(fold_start)
            for d_idx in range(num_days):
                t_day = start_d + timedelta(days=d_idx)
                issued = t_day - timedelta(days=1)
                cutoff = issued - timedelta(days=1)
                sign = -1.0 if (d_idx % 2 == 0) else 1.0
                records.append(
                    {
                        "fold_start": fold_start,
                        "issued_on": issued.isoformat(),
                        "target_day": t_day.isoformat(),
                        "observed_through": cutoff.isoformat(),
                        "station_complex_id": "611",
                        "hour": 8,
                        "lag_days": 1,
                        "method": "pooled_boosted_trees",
                        "actual": max(0.0, 1000.0 + sign * offset_err),
                        "prediction": 1000.0,
                        "target_status": "observed",
                        "scoreable": True,
                        "day_type": "weekday",
                        "hour_band": "peak",
                    }
                )

        calibrated, checks = calibrate_past_residual_intervals(records, self.config)
        self.assertEqual(len(calibrated), 30)
        self.assertGreater(checks, 0)
        for rec in calibrated:
            self.assertEqual(rec["fold_start"], "2025-06-01")
            self.assertTrue(rec["intervals"]["p80"]["covered"])
            self.assertTrue(rec["intervals"]["p90"]["covered"])
            self.assertGreaterEqual(
                rec["intervals"]["p90"]["width"], rec["intervals"]["p80"]["width"]
            )

        summary = summarize_calibration(calibrated)
        self.assertEqual(len(summary["overall"]), 1)
        self.assertEqual(summary["overall"][0]["scored_hours"], 30)
        self.assertEqual(
            summary["overall"][0]["levels"]["p80"]["empirical_coverage"], 1.0
        )

        walk = build_slice_error_walkthrough(records, self.config)
        self.assertEqual(len(walk["joint_station_daytype_hourband_slices"]), 1)
        self.assertAlmostEqual(
            walk["joint_station_daytype_hourband_slices"][0]["mae"], 37.5
        )

        # Verify October 2026 holdout date is strictly refused
        bad_records = list(records) + [
            {
                **records[0],
                "target_day": "2026-10-01",
            }
        ]
        with self.assertRaises(ValidationError):
            calibrate_past_residual_intervals(bad_records, self.config)
        with self.assertRaises(ValidationError):
            build_slice_error_walkthrough(bad_records, self.config)


if __name__ == "__main__":
    unittest.main()
