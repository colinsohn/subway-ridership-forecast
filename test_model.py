from datetime import date, timedelta
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from aggregate import aggregate
from features import FeatureHistory
from ingest import Selection, ValidationError, download
from model import (
    ALL_METHODS,
    MODEL_NAME,
    PooledBoostedTrees,
    evaluate_development_models,
    run_model_evaluation,
)
from test_backtest import config
from test_features import complete_panel_observations
from test_ingest import FakeSource, row


class ModelTests(unittest.TestCase):
    def test_pooled_boosted_trees_fit_predict_and_dst_handling(self):
        obs = complete_panel_observations(date(2025, 1, 1), date(2025, 3, 25))
        history = FeatureHistory(obs)
        train_rows = [
            history.build_row("611", date(2025, 1, 15) + timedelta(days=d), h, 1)
            for d in range(35)
            for h in range(24)
        ]
        train_rows = [r for r in train_rows if r["eligible_target"] and r["feature_ready"]]
        model = PooledBoostedTrees(stations=("611",), n_estimators=10, min_samples_leaf=16)
        model.fit(train_rows)

        sample = history.build_row("611", date(2025, 3, 17), 8, 1)
        pred = model.predict_row(sample)
        self.assertIsNotNone(pred)
        self.assertGreaterEqual(pred, 0.0)
        self.assertAlmostEqual(sum(model.feature_importance_.values()), 1.0, places=4)

        # Excluded DST day returns None prediction.
        dst_sample = history.build_row("611", date(2025, 3, 9), 8, 1)
        self.assertIsNone(model.predict_row(dst_sample))

        with self.assertRaises(ValidationError):
            PooledBoostedTrees(stations=("611",)).fit([])

    def test_end_to_end_model_evaluation_and_holdout_guard(self):
        c = config()
        c["publication_lag_days"] = [1, 3, 7]
        source = [
            row(
                f"row-{i}-{h}",
                f"{day}T{h:02d}:00:00.000",
                ridership=str(100 + 15 * day.weekday() + 10 * h + (i % 5)),
                transfers="0",
            )
            for i, day in enumerate(date(2025, 1, 1) + timedelta(days=n) for n in range(59))
            for h in range(24)
        ]
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "snapshot"
            panel = root / "panel"
            out = root / "model_eval"
            download(Selection(date(2025, 1, 1), date(2025, 3, 1), ("611",)), snapshot, fetch=FakeSource(source))
            aggregate(snapshot, panel)
            report = run_model_evaluation(panel, out, c)
            self.assertEqual(report["chronology_violations"], 0)
            self.assertEqual(report["methods_compared"], list(ALL_METHODS))
            self.assertEqual(len(report["overall"]), 3 * 3)  # 3 lags * 3 methods
            for item in report["overall"]:
                self.assertEqual(item["scheduled"], 28 * 24)
                self.assertEqual(item["scored"], 28 * 24)
                self.assertIsNotNone(item["mae"])
                self.assertIsNotNone(item["rmse"])
                self.assertIsNotNone(item["wape"])

            with duckdb.connect() as db:
                n_preds = db.execute(
                    "SELECT count(*) FROM read_parquet(?)", [str(out / "predictions.parquet")]
                ).fetchone()[0]
                self.assertEqual(n_preds, 28 * 24 * 3 * 3)

            with self.assertRaises(FileExistsError):
                run_model_evaluation(panel, out, c)


if __name__ == "__main__":
    unittest.main()
