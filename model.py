"""Train and evaluate the pooled boosted-tree model against seasonal baselines on development folds."""
import argparse
from datetime import date, timedelta
import json
import math
from pathlib import Path
import shutil
import tempfile

import duckdb

from aggregate import sql_path
from backtest import METHODS as BASELINE_METHODS, PEAK_HOURS, chronological_folds, metrics, next_month, validate_config
from features import (
    FEATURE_COLUMNS,
    FeatureHistory,
    build_feature_store,
    validate_feature_row,
    validate_training_cut,
)
from forecast_window import next_day_window
from ingest import ValidationError, sha256

MODEL_NAME = "pooled_boosted_trees"
ALL_METHODS = ("seasonal_latest", "seasonal_mean4", MODEL_NAME)


def _num(val, fallback=0.0):
    return float(val) if val is not None and math.isfinite(val) else float(fallback)


def extract_feature_vector(row, stations):
    """Extract deterministic numeric feature vector from a validated cutoff-bounded feature row."""
    mean4 = _num(row["same_weekday_mean4"], _num(row["recent_same_hour_mean7"], 0.0))
    lag1 = _num(row["same_weekday_lag1"], mean4)
    lag2 = _num(row["same_weekday_lag2"], mean4)
    lag3 = _num(row["same_weekday_lag3"], mean4)
    lag4 = _num(row["same_weekday_lag4"], mean4)
    min4 = _num(row["same_weekday_min4"], mean4)
    max4 = _num(row["same_weekday_max4"], mean4)
    std4 = _num(row["same_weekday_std4"], 0.0)
    trend = _num(row["same_weekday_trend"], 0.0)
    age = _num(row["same_weekday_age_days"], float(row["lag_days"] + 7))
    rec_latest = _num(row["recent_same_hour_latest"], mean4)
    rec_mean7 = _num(row["recent_same_hour_mean7"], mean4)
    rec_adj = _num(row["recent_same_hour_adjacent_mean"], rec_mean7)
    day_tot = _num(row["cutoff_day_total_24h"], mean4 * 24.0)

    vec = [
        float(row["day_of_week"]),
        float(row["is_weekend"]),
        float(row["is_peak_hour"]),
        float(row["month"]),
        float(row["hour_sin"]),
        float(row["hour_cos"]),
        float(row["dow_sin"]),
        float(row["dow_cos"]),
        lag1,
        lag2,
        lag3,
        lag4,
        float(row["same_weekday_count4"]),
        mean4,
        min4,
        max4,
        std4,
        trend,
        age,
        rec_latest,
        rec_mean7,
        float(row["recent_same_hour_count7"]),
        rec_adj,
        day_tot,
        lag1 - mean4,
        rec_mean7 - mean4,
        rec_latest - rec_mean7,
        rec_adj - mean4,
    ]
    for s in stations:
        vec.append(1.0 if row["station_complex_id"] == s else 0.0)
    return vec


def feature_names(stations):
    return list(FEATURE_COLUMNS) + [
        "lag1_minus_mean4",
        "recent_mean7_minus_mean4",
        "recent_latest_minus_recent_mean7",
        "recent_adjacent_minus_mean4",
    ] + [f"station_{s}" for s in stations]


class PooledBoostedTrees:
    """Deterministic pooled histogram gradient-boosted regression trees over a seasonal anchor."""

    def __init__(
        self,
        stations,
        n_estimators=20,
        max_depth=2,
        learning_rate=0.10,
        min_samples_leaf=48,
        l2_reg=25.0,
        n_bins=12,
    ):
        self.stations = tuple(sorted(stations))
        self.n_estimators = n_estimators
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.min_samples_leaf = min_samples_leaf
        self.l2_reg = l2_reg
        self.n_bins = n_bins
        self.alpha = 0.5
        self.bias = 0.0
        self.thresholds = []
        self.trees = []
        self.feature_importance_ = {}

    def _anchor(self, row):
        lag1 = row["same_weekday_lag1"]
        mean4 = row["same_weekday_mean4"]
        if lag1 is not None and mean4 is not None:
            return self.alpha * float(lag1) + (1.0 - self.alpha) * float(mean4)
        if mean4 is not None:
            return float(mean4)
        if lag1 is not None:
            return float(lag1)
        if row["recent_same_hour_mean7"] is not None:
            return float(row["recent_same_hour_mean7"])
        return 0.0

    def fit(self, train_rows):
        if not train_rows:
            raise ValidationError("cannot fit PooledBoostedTrees on empty training set")
        n = len(train_rows)
        targets = [float(r["target"]) for r in train_rows]

        # 1. Select optimal seasonal anchor blend alpha in [0, 1] on eligible training rows only.
        best_alpha, best_mae = 0.5, float("inf")
        for step in range(21):
            candidate = step * 0.05
            err = 0.0
            for r, y in zip(train_rows, targets):
                l1 = float(r["same_weekday_lag1"])
                m4 = float(r["same_weekday_mean4"])
                pred = candidate * l1 + (1.0 - candidate) * m4
                err += abs(y - pred)
            if err < best_mae:
                best_mae = err
                best_alpha = candidate
        self.alpha = round(best_alpha, 4)

        preds = [self._anchor(r) for r in train_rows]
        residuals = [y - p for y, p in zip(targets, preds)]
        # Median/trimmed residual bias adjustment
        sorted_res = sorted(residuals)
        self.bias = sorted_res[n // 2] * 0.25
        preds = [p + self.bias for p in preds]

        vectors = [extract_feature_vector(r, self.stations) for r in train_rows]
        n_features = len(vectors[0])
        names = feature_names(self.stations)
        gains = [0.0] * n_features

        # 2. Precompute quantile thresholds per feature for O(N) histogram binning.
        self.thresholds = []
        binned_cols = []
        for f_idx in range(n_features):
            col = [v[f_idx] for v in vectors]
            s_col = sorted(col)
            thresh = []
            for b in range(1, self.n_bins):
                q_val = s_col[min(n - 1, int(b * n / self.n_bins))]
                if not thresh or q_val > thresh[-1]:
                    thresh.append(q_val)
            self.thresholds.append(thresh)
            k = len(thresh)
            if k == 0:
                binned_cols.append([0] * n)
            else:
                b_col = [0] * n
                for i, val in enumerate(col):
                    lo, hi = 0, k
                    while lo < hi:
                        mid = (lo + hi) // 2
                        if val <= thresh[mid]:
                            hi = mid
                        else:
                            lo = mid + 1
                    b_col[i] = lo
                binned_cols.append(b_col)

        # 3. Fit boosted regression trees using Huber-clipped gradients (robust to MAE & RMSE).
        self.trees = []
        all_indices = list(range(n))
        for _ in range(self.n_estimators):
            abs_err = sorted(abs(y - p) for y, p in zip(targets, preds))
            delta = max(25.0, abs_err[min(n - 1, int(0.75 * n))])
            grads = []
            for y, p in zip(targets, preds):
                diff = y - p
                if diff > delta:
                    grads.append(delta)
                elif diff < -delta:
                    grads.append(-delta)
                else:
                    grads.append(diff)

            tree = self._build_tree(all_indices, grads, binned_cols, gains, depth=0)
            self.trees.append(tree)
            for i in range(n):
                preds[i] += self.learning_rate * self._eval_binned(tree, binned_cols, i)

        total_gain = sum(gains)
        self.feature_importance_ = {
            names[i]: (round(gains[i] / total_gain, 6) if total_gain > 0 else 0.0)
            for i in range(n_features)
        }
        return self

    def _leaf_value(self, grad_sum, count):
        return grad_sum / (count + self.l2_reg)

    def _build_tree(self, indices, grads, binned_cols, gains, depth):
        count = len(indices)
        grad_sum = sum(grads[i] for i in indices)
        leaf_val = self._leaf_value(grad_sum, count)
        if depth >= self.max_depth or count < 2 * self.min_samples_leaf:
            return {"leaf": True, "value": leaf_val}

        parent_score = (grad_sum * grad_sum) / (count + self.l2_reg)
        best_gain = 0.0
        best_feat = -1
        best_bin = -1

        n_features = len(self.thresholds)
        for f_idx in range(n_features):
            n_thresh = len(self.thresholds[f_idx])
            if n_thresh == 0:
                continue
            nb = n_thresh + 1
            bin_sums = [0.0] * nb
            bin_counts = [0] * nb
            b_col = binned_cols[f_idx]
            for i in indices:
                b = b_col[i]
                bin_sums[b] += grads[i]
                bin_counts[b] += 1

            left_sum = 0.0
            left_count = 0
            for b in range(n_thresh):
                left_sum += bin_sums[b]
                left_count += bin_counts[b]
                right_count = count - left_count
                if left_count < self.min_samples_leaf or right_count < self.min_samples_leaf:
                    continue
                right_sum = grad_sum - left_sum
                score = (left_sum * left_sum) / (left_count + self.l2_reg) + (
                    right_sum * right_sum
                ) / (right_count + self.l2_reg)
                gain = score - parent_score
                if gain > best_gain:
                    best_gain = gain
                    best_feat = f_idx
                    best_bin = b

        if best_feat < 0 or best_gain <= 1e-6:
            return {"leaf": True, "value": leaf_val}

        gains[best_feat] += best_gain
        b_col = binned_cols[best_feat]
        left_idx = [i for i in indices if b_col[i] <= best_bin]
        right_idx = [i for i in indices if b_col[i] > best_bin]
        return {
            "leaf": False,
            "feature": best_feat,
            "bin": best_bin,
            "threshold": self.thresholds[best_feat][best_bin],
            "left": self._build_tree(left_idx, grads, binned_cols, gains, depth + 1),
            "right": self._build_tree(right_idx, grads, binned_cols, gains, depth + 1),
        }

    def _eval_binned(self, node, binned_cols, row_idx):
        while not node["leaf"]:
            if binned_cols[node["feature"]][row_idx] <= node["bin"]:
                node = node["left"]
            else:
                node = node["right"]
        return node["value"]

    def predict_row(self, row):
        if not row["feature_ready"] or row["target_status"] == "excluded_dst_day":
            return None
        vec = extract_feature_vector(row, self.stations)
        pred = self._anchor(row) + self.bias
        for tree in self.trees:
            node = tree
            while not node["leaf"]:
                if vec[node["feature"]] <= node["threshold"]:
                    node = node["left"]
                else:
                    node = node["right"]
            pred += self.learning_rate * node["value"]
        return max(0.0, pred)


def evaluate_development_models(observations, config):
    """Run chronological fold-by-fold training and comparison against both seasonal baselines."""
    dates, months = validate_config(config)
    bounds = (dates["development_start"], dates["development_end_exclusive"])
    if any(not bounds[0] <= obs.stamp.date() < bounds[1] for obs in observations):
        raise ValidationError("non-development observation refused")

    history = FeatureHistory(observations)
    stations = tuple(sorted(config["station_complex_ids"]))

    # Pre-build all eligible historical feature rows per lag once.
    rows_by_lag = {}
    for lag in config["publication_lag_days"]:
        first_target = dates["development_start"] + timedelta(days=lag + 1)
        lag_rows = []
        day = first_target
        while day < dates["development_end_exclusive"]:
            for station in config["station_complex_ids"]:
                for hour in range(24):
                    lag_rows.append(
                        history.build_row(station, day, hour, lag, bounds=bounds)
                    )
            day += timedelta(days=1)
        rows_by_lag[lag] = lag_rows

    fold_diagnostics = []
    prediction_records = []

    for month in months:
        m_start = month.isoformat()
        m_end_date = next_month(month)
        m_end = m_end_date.isoformat()
        first_issued = month - timedelta(days=1)

        for lag in config["publication_lag_days"]:
            fold_cutoff = next_day_window(first_issued, lag).observed_through
            all_rows = rows_by_lag[lag]

            train_rows = [
                r
                for r in all_rows
                if date.fromisoformat(r["target_day"]) <= fold_cutoff
                and r["eligible_target"]
                and r["feature_ready"]
            ]
            val_rows = [
                r
                for r in all_rows
                if month <= date.fromisoformat(r["target_day"]) < m_end_date
            ]

            # Strictly verify zero unpublished training labels or leaked feature dates.
            validate_training_cut(
                train_rows, val_rows, fold_cutoff, dst_days=history.dst_days, bounds=bounds
            )

            model = PooledBoostedTrees(stations=stations)
            model.fit(train_rows)

            top_features = sorted(
                model.feature_importance_.items(), key=lambda kv: kv[1], reverse=True
            )[:6]
            fold_diagnostics.append({
                "fold_start": m_start,
                "validation_end_exclusive": m_end,
                "lag_days": lag,
                "fold_start_observed_through": fold_cutoff.isoformat(),
                "train_rows": len(train_rows),
                "validation_rows": len(val_rows),
                "seasonal_anchor_alpha_on_lag1": model.alpha,
                "residual_bias_adjustment": round(model.bias, 4),
                "top_features_by_gain": [
                    {"feature": name, "relative_gain": gain}
                    for name, gain in top_features
                ],
            })

            for r in val_rows:
                validate_feature_row(r, dst_days=history.dst_days, bounds=bounds)
                is_dst = r["target_status"] == "excluded_dst_day"
                p_latest = None if is_dst else r["same_weekday_lag1"]
                p_mean4 = None if is_dst else r["same_weekday_mean4"]
                p_gbdt = None if is_dst else model.predict_row(r)

                for method, pred in (
                    ("seasonal_latest", p_latest),
                    ("seasonal_mean4", p_mean4),
                    (MODEL_NAME, p_gbdt),
                ):
                    prediction_records.append({
                        "fold_start": m_start,
                        "issued_on": r["issued_on"],
                        "target_day": r["target_day"],
                        "observed_through": r["observed_through"],
                        "station_complex_id": r["station_complex_id"],
                        "hour": r["hour"],
                        "lag_days": lag,
                        "method": method,
                        "actual": r["target"],
                        "prediction": pred,
                        "target_status": r["target_status"],
                        "reference_count": r["same_weekday_count4"],
                        "reference_oldest": r["same_weekday_oldest_date"],
                        "reference_newest": r["feature_newest_source_date"]
                        if method == MODEL_NAME
                        else r["same_weekday_newest_date"],
                        "scoreable": (r["target"] is not None and pred is not None),
                        "day_type": "weekend" if r["is_weekend"] == 1 else "weekday",
                        "hour_band": "peak" if r["is_peak_hour"] == 1 else "off_peak",
                    })

    return prediction_records, fold_diagnostics


MODEL_PREDICTION_TYPES = {
    "fold_start": "DATE",
    "issued_on": "DATE",
    "target_day": "DATE",
    "observed_through": "DATE",
    "station_complex_id": "VARCHAR",
    "hour": "INTEGER",
    "lag_days": "INTEGER",
    "method": "VARCHAR",
    "actual": "DOUBLE",
    "prediction": "DOUBLE",
    "target_status": "VARCHAR",
    "reference_count": "INTEGER",
    "reference_oldest": "DATE",
    "reference_newest": "DATE",
    "scoreable": "BOOLEAN",
    "day_type": "VARCHAR",
    "hour_band": "VARCHAR",
}


def run_model_evaluation(panel_dir, destination, config):
    """Fit and score the pooled boosted-tree model across development folds and verify chronology."""
    from backtest import load_panel

    destination = Path(destination)
    if destination.exists():
        raise FileExistsError("model evaluation output already exists")
    observations, audit = load_panel(panel_dir, config)
    if (
        audit["selection"]["start"] != config["development_start"]
        or audit["selection"]["end_exclusive"] != config["development_end_exclusive"]
    ):
        raise ValidationError("full declared development panel required for model evaluation")

    predictions, fold_diagnostics = evaluate_development_models(observations, config)

    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".model-", dir=destination.parent))
    try:
        with (stage / "model_predictions.jsonl").open("w") as stream:
            for row in predictions:
                stream.write(json.dumps(row, allow_nan=False) + "\n")

        with duckdb.connect() as db:
            columns = "{" + ",".join(f"'{name}':'{kind}'" for name, kind in MODEL_PREDICTION_TYPES.items()) + "}"
            db.execute(
                f"CREATE TABLE predictions AS SELECT * FROM read_json(?, format='newline_delimited', columns={columns})",
                [str(stage / "model_predictions.jsonl")],
            )
            violations = db.execute(
                """
                SELECT count(*) FROM predictions
                WHERE reference_newest > observed_through
                   OR observed_through >= issued_on
                   OR issued_on >= target_day
                   OR target_day >= ?
                """,
                [config["development_end_exclusive"]],
            ).fetchone()[0]
            if violations:
                raise ValidationError(f"model chronology audit failed with {violations} violations")

            # Verify identical scored counts across all three methods at every lag.
            mismatch = db.execute(
                """
                SELECT count(*) FROM (
                    SELECT lag_days, count(DISTINCT scored) AS distinct_counts
                    FROM (
                        SELECT lag_days, method, count(*) FILTER (WHERE scoreable) AS scored
                        FROM predictions
                        GROUP BY lag_days, method
                    )
                    GROUP BY lag_days
                    HAVING count(DISTINCT scored) <> 1
                )
                """
            ).fetchone()[0]
            if mismatch:
                raise ValidationError("scored case count mismatch between baselines and pooled model")

            report = {
                "format_version": 1,
                "model_name": MODEL_NAME,
                "methods_compared": list(ALL_METHODS),
                "study_config": config,
                "folds": chronological_folds(config),
                "fold_model_diagnostics": fold_diagnostics,
                "input_source_rows_sha256": audit["source_rows_sha256"],
                "input_source_version": audit["source_version"],
                "panel_parquet_sha256": audit["artifacts"]["station_hours.parquet"],
                "chronology_violations": violations,
                "overall": metrics(db, ["lag_days", "method"]),
                "by_fold": metrics(db, ["fold_start", "lag_days", "method"]),
                "by_station": metrics(db, ["station_complex_id", "lag_days", "method"]),
                "by_day_type": metrics(db, ["day_type", "lag_days", "method"]),
                "by_hour_band": metrics(db, ["hour_band", "lag_days", "method"]),
                "peak_hours": list(PEAK_HOURS),
                "duckdb_version": duckdb.__version__,
                "limitations": [
                    "Development validation only; September calibration and October final holdout remain untouched.",
                    "Each fold fits strictly on eligible observations with target_day <= fold_start_observed_through.",
                    "Three Manhattan hubs with simulated publication lags; not a system-wide or as-published claim.",
                ],
            }
            db.execute("COPY predictions TO " + sql_path(stage / "predictions.parquet") + " (FORMAT PARQUET)")

        (stage / "model_predictions.jsonl").unlink()
        report["predictions_parquet_sha256"] = sha256(stage / "predictions.parquet")
        (stage / "metrics.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        if destination.exists():
            raise FileExistsError("model evaluation output appeared during run")
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
    report = run_model_evaluation(args.panel, args.output, json.loads(args.config.read_text()))
    print(
        json.dumps(
            {
                "chronology_violations": report["chronology_violations"],
                "overall": report["overall"],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
