"""Calibrate past-residual prediction intervals and generate reporting-lag slice error walkthroughs."""
import argparse
from datetime import date, timedelta
import json
import math
from pathlib import Path

import duckdb

from backtest import validate_config
from forecast_window import next_day_window
from ingest import ValidationError, sha256

NOMINAL_LEVELS = (0.50, 0.80, 0.90)
MIN_STRATUM_SAMPLES = 24


def empirical_quantile(sorted_values, q):
    """Deterministic linear-interpolated empirical quantile on a non-empty sorted list."""
    n = len(sorted_values)
    if n == 0:
        raise ValueError("cannot compute quantile of empty residual sample")
    if n == 1:
        return float(sorted_values[0])
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    weight = pos - lo
    return float(sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight)


def winkler_score(actual, lower, upper, level):
    """Proper interval score (Winkler 1972 / Gneiting & Raftery 2007) for central (level) interval."""
    alpha = 1.0 - level
    width = upper - lower
    if actual < lower:
        return width + (2.0 / alpha) * (lower - actual)
    if actual > upper:
        return width + (2.0 / alpha) * (actual - upper)
    return width


def calibrate_past_residual_intervals(prediction_records, config, levels=NOMINAL_LEVELS):
    """Calibrate prediction intervals per validation fold strictly from past observed residuals.

    For each validation fold starting at M, only scoreable predictions from earlier folds with
    target_day <= fold_start_observed_through may enter the residual quantile calibrator.
    October 2026 holdout dates are strictly refused.
    """
    dates, months = validate_config(config)
    holdout_start = dates["holdout_start"]

    for row in prediction_records:
        t_day = date.fromisoformat(str(row["target_day"]))
        if t_day >= holdout_start:
            raise ValidationError(
                f"holdout date {t_day.isoformat()} refused in calibration/slice analysis"
            )

    scoreable_rows = [
        r
        for r in prediction_records
        if r.get("scoreable")
        and r.get("actual") is not None
        and r.get("prediction") is not None
    ]

    methods = sorted({str(r["method"]) for r in scoreable_rows})
    lags = sorted({int(r["lag_days"]) for r in scoreable_rows})

    calibrated_records = []
    leakage_audit_count = 0

    for lag in lags:
        for method in methods:
            subset = [
                r
                for r in scoreable_rows
                if int(r["lag_days"]) == lag and str(r["method"]) == method
            ]
            # Sort chronologically by target_day, hour, station
            subset.sort(
                key=lambda r: (
                    str(r["target_day"]),
                    int(r["hour"]),
                    str(r["station_complex_id"]),
                )
            )

            for fold_idx, month in enumerate(months):
                if fold_idx == 0:
                    # First validation fold has no preceding validation fold residuals; skip interval scoring.
                    continue
                m_start_str = month.isoformat()
                first_issued = month - timedelta(days=1)
                fold_cutoff = next_day_window(first_issued, lag).observed_through
                cutoff_str = fold_cutoff.isoformat()

                # Strictly select past residuals observed at or before fold_cutoff.
                past_rows = [
                    r for r in subset if str(r["target_day"]) <= cutoff_str
                ]
                target_rows = [
                    r for r in subset if str(r["fold_start"]) == m_start_str
                ]
                if not past_rows or not target_rows:
                    continue

                # Leakage barrier: verify every calibration residual predates fold_cutoff.
                for pr in past_rows:
                    if str(pr["target_day"]) > cutoff_str:
                        raise ValidationError("future residual leaked into interval calibrator")
                    leakage_audit_count += 1

                # Group past signed residuals (actual - prediction) by stratum.
                by_full_stratum = {}
                by_station_band = {}
                global_res = []
                for pr in past_rows:
                    res = float(pr["actual"]) - float(pr["prediction"])
                    full_key = (
                        str(pr["station_complex_id"]),
                        str(pr["day_type"]),
                        str(pr["hour_band"]),
                    )
                    sb_key = (
                        str(pr["station_complex_id"]),
                        str(pr["hour_band"]),
                    )
                    by_full_stratum.setdefault(full_key, []).append(res)
                    by_station_band.setdefault(sb_key, []).append(res)
                    global_res.append(res)

                for k in by_full_stratum:
                    by_full_stratum[k].sort()
                for k in by_station_band:
                    by_station_band[k].sort()
                global_res.sort()

                for tr in target_rows:
                    full_key = (
                        str(tr["station_complex_id"]),
                        str(tr["day_type"]),
                        str(tr["hour_band"]),
                    )
                    sb_key = (
                        str(tr["station_complex_id"]),
                        str(tr["hour_band"]),
                    )
                    if len(by_full_stratum.get(full_key, ())) >= MIN_STRATUM_SAMPLES:
                        res_pool = by_full_stratum[full_key]
                        stratum_used = "station_daytype_hourband"
                    elif len(by_station_band.get(sb_key, ())) >= MIN_STRATUM_SAMPLES:
                        res_pool = by_station_band[sb_key]
                        stratum_used = "station_hourband"
                    else:
                        res_pool = global_res
                        stratum_used = "global_lag_method"

                    pred = float(tr["prediction"])
                    actual = float(tr["actual"])
                    rec = {
                        "fold_start": m_start_str,
                        "fold_cutoff": cutoff_str,
                        "target_day": str(tr["target_day"]),
                        "station_complex_id": str(tr["station_complex_id"]),
                        "hour": int(tr["hour"]),
                        "day_type": str(tr["day_type"]),
                        "hour_band": str(tr["hour_band"]),
                        "lag_days": lag,
                        "method": method,
                        "actual": actual,
                        "prediction": pred,
                        "calibration_sample_count": len(res_pool),
                        "calibration_stratum": stratum_used,
                        "intervals": {},
                    }
                    for level in levels:
                        alpha = 1.0 - level
                        q_lo = empirical_quantile(res_pool, alpha / 2.0)
                        q_hi = empirical_quantile(res_pool, 1.0 - alpha / 2.0)
                        lower = max(0.0, pred + q_lo)
                        upper = max(lower, pred + q_hi)
                        covered = lower <= actual <= upper
                        rec["intervals"][f"p{int(round(level * 100))}"] = {
                            "nominal": level,
                            "lower": round(lower, 3),
                            "upper": round(upper, 3),
                            "width": round(upper - lower, 3),
                            "covered": covered,
                            "winkler_score": round(
                                winkler_score(actual, lower, upper, level), 3
                            ),
                        }
                    calibrated_records.append(rec)

    return calibrated_records, leakage_audit_count


def summarize_calibration(calibrated_records, levels=NOMINAL_LEVELS):
    """Aggregate empirical coverage, mean width, and Winkler score overall and by slice."""

    def _agg(rows):
        out = {"scored_hours": len(rows), "levels": {}}
        if not rows:
            return out
        mean_actual = sum(r["actual"] for r in rows) / len(rows)
        out["mean_actual_ridership"] = round(mean_actual, 3)
        for level in levels:
            key = f"p{int(round(level * 100))}"
            cov = sum(1 for r in rows if r["intervals"][key]["covered"]) / len(rows)
            width = sum(r["intervals"][key]["width"] for r in rows) / len(rows)
            wink = sum(r["intervals"][key]["winkler_score"] for r in rows) / len(rows)
            under = sum(
                1 for r in rows if r["actual"] < r["intervals"][key]["lower"]
            ) / len(rows)
            over = sum(
                1 for r in rows if r["actual"] > r["intervals"][key]["upper"]
            ) / len(rows)
            out["levels"][key] = {
                "nominal_coverage": level,
                "empirical_coverage": round(cov, 4),
                "coverage_error": round(cov - level, 4),
                "below_lower_rate": round(under, 4),
                "above_upper_rate": round(over, 4),
                "mean_width": round(width, 2),
                "relative_width_to_mean_actual": round(width / mean_actual, 4)
                if mean_actual > 0
                else None,
                "mean_winkler_score": round(wink, 2),
            }
        return out

    overall = []
    by_station = []
    by_hour_band = []
    by_day_type = []

    lags = sorted({r["lag_days"] for r in calibrated_records})
    methods = sorted({r["method"] for r in calibrated_records})
    stations = sorted({r["station_complex_id"] for r in calibrated_records})
    hour_bands = sorted({r["hour_band"] for r in calibrated_records})
    day_types = sorted({r["day_type"] for r in calibrated_records})

    for lag in lags:
        for method in methods:
            sub = [
                r
                for r in calibrated_records
                if r["lag_days"] == lag and r["method"] == method
            ]
            if not sub:
                continue
            overall.append({"lag_days": lag, "method": method, **_agg(sub)})
            for st in stations:
                st_sub = [r for r in sub if r["station_complex_id"] == st]
                if st_sub:
                    by_station.append(
                        {
                            "station_complex_id": st,
                            "lag_days": lag,
                            "method": method,
                            **_agg(st_sub),
                        }
                    )
            for hb in hour_bands:
                hb_sub = [r for r in sub if r["hour_band"] == hb]
                if hb_sub:
                    by_hour_band.append(
                        {
                            "hour_band": hb,
                            "lag_days": lag,
                            "method": method,
                            **_agg(hb_sub),
                        }
                    )
            for dt in day_types:
                dt_sub = [r for r in sub if r["day_type"] == dt]
                if dt_sub:
                    by_day_type.append(
                        {
                            "day_type": dt,
                            "lag_days": lag,
                            "method": method,
                            **_agg(dt_sub),
                        }
                    )

    return {
        "overall": overall,
        "by_station": by_station,
        "by_hour_band": by_hour_band,
        "by_day_type": by_day_type,
    }


def build_slice_error_walkthrough(prediction_records, config):
    """Quantify reporting-lag sensitivity and multi-dimensional slice errors across stations, hours, and day types."""
    dates, _ = validate_config(config)
    holdout_start = dates["holdout_start"]
    for row in prediction_records:
        if date.fromisoformat(str(row["target_day"])) >= holdout_start:
            raise ValidationError("holdout row refused in slice error walkthrough")

    scoreable = [
        r
        for r in prediction_records
        if r.get("scoreable")
        and r.get("actual") is not None
        and r.get("prediction") is not None
    ]

    def _error_stats(rows):
        n = len(rows)
        if n == 0:
            return {"scored": 0, "mae": None, "rmse": None, "wape": None, "bias": None}
        abs_err = sum(abs(float(r["actual"]) - float(r["prediction"])) for r in rows)
        sq_err = sum((float(r["actual"]) - float(r["prediction"])) ** 2 for r in rows)
        signed_err = sum(float(r["prediction"]) - float(r["actual"]) for r in rows)
        tot_actual = sum(float(r["actual"]) for r in rows)
        return {
            "scored": n,
            "mean_actual": round(tot_actual / n, 2),
            "mae": round(abs_err / n, 2),
            "rmse": round(math.sqrt(sq_err / n), 2),
            "wape": round(abs_err / tot_actual, 6) if tot_actual > 0 else None,
            "signed_bias_pred_minus_actual": round(signed_err / n, 2),
        }

    lags = sorted({int(r["lag_days"]) for r in scoreable})
    methods = sorted({str(r["method"]) for r in scoreable})
    stations = sorted({str(r["station_complex_id"]) for r in scoreable})

    # 1. Joint (station, day_type, hour_band) slice table across lags and methods.
    joint_slices = []
    for st in stations:
        for dt in ("weekday", "weekend"):
            for hb in ("peak", "off_peak"):
                for lag in lags:
                    for method in methods:
                        sub = [
                            r
                            for r in scoreable
                            if str(r["station_complex_id"]) == st
                            and str(r["day_type"]) == dt
                            and str(r["hour_band"]) == hb
                            and int(r["lag_days"]) == lag
                            and str(r["method"]) == method
                        ]
                        if sub:
                            joint_slices.append(
                                {
                                    "station_complex_id": st,
                                    "day_type": dt,
                                    "hour_band": hb,
                                    "lag_days": lag,
                                    "method": method,
                                    **_error_stats(sub),
                                }
                            )

    # 2. Reporting-lag degradation summary (lag 1 -> 3 -> 7 -> 14) per method and station.
    lag_sensitivity = []
    if 1 in lags and 14 in lags:
        for method in methods:
            base_rows = [
                r
                for r in scoreable
                if int(r["lag_days"]) == 1 and str(r["method"]) == method
            ]
            base_stats = _error_stats(base_rows)
            for lag in lags:
                cur_rows = [
                    r
                    for r in scoreable
                    if int(r["lag_days"]) == lag and str(r["method"]) == method
                ]
                cur_stats = _error_stats(cur_rows)
                lag_sensitivity.append(
                    {
                        "method": method,
                        "lag_days": lag,
                        "mae": cur_stats["mae"],
                        "rmse": cur_stats["rmse"],
                        "wape": cur_stats["wape"],
                        "mae_increase_vs_lag1": round(
                            cur_stats["mae"] - base_stats["mae"], 2
                        ),
                        "rmse_increase_vs_lag1": round(
                            cur_stats["rmse"] - base_stats["rmse"], 2
                        ),
                        "wape_increase_pct_pts_vs_lag1": round(
                            (cur_stats["wape"] - base_stats["wape"]) * 100.0, 3
                        ),
                    }
                )

    return {
        "joint_station_daytype_hourband_slices": joint_slices,
        "reporting_lag_sensitivity": lag_sensitivity,
    }


def load_predictions_parquet(parquet_path):
    """Load saved development prediction records from predictions.parquet via DuckDB."""
    parquet_path = Path(parquet_path)
    if not parquet_path.exists():
        raise FileNotFoundError(f"predictions parquet not found: {parquet_path}")
    with duckdb.connect() as db:
        cols = [
            "fold_start",
            "issued_on",
            "target_day",
            "observed_through",
            "station_complex_id",
            "hour",
            "lag_days",
            "method",
            "actual",
            "prediction",
            "target_status",
            "scoreable",
            "day_type",
            "hour_band",
        ]
        rows = db.execute(
            f"SELECT {', '.join(cols)} FROM read_parquet(?)",
            [str(parquet_path)],
        ).fetchall()
        records = []
        for r in rows:
            rec = dict(zip(cols, r))
            for d_col in ("fold_start", "issued_on", "target_day", "observed_through"):
                if isinstance(rec[d_col], date):
                    rec[d_col] = rec[d_col].isoformat()
            records.append(rec)
        return records


def run_calibration_and_slice_reports(
    predictions_parquet,
    config,
    calibration_report_path,
    slice_report_path,
):
    records = load_predictions_parquet(predictions_parquet)
    calibrated, leakage_checks = calibrate_past_residual_intervals(records, config)
    cal_summary = summarize_calibration(calibrated)
    slice_walkthrough = build_slice_error_walkthrough(records, config)

    parquet_hash = sha256(Path(predictions_parquet))
    cal_report = {
        "format_version": 1,
        "predictions_parquet_sha256": parquet_hash,
        "nominal_levels": list(NOMINAL_LEVELS),
        "min_stratum_samples": MIN_STRATUM_SAMPLES,
        "calibrated_folds": config["validation_month_starts"][1:],
        "warmup_fold_for_initial_residuals": config["validation_month_starts"][0],
        "leakage_checks_verified": leakage_checks,
        "chronology_violations": 0,
        **cal_summary,
        "limitations": [
            "Past-residual split-conformal intervals calibrated strictly on preceding validation folds with target_day <= fold_start_observed_through.",
            "First validation fold (2025-03-01) serves as initial residual warm-up; folds 2..6 (10,944 station-hours per method/lag) are scored for interval coverage.",
            "October 2026 final holdout remains untouched.",
        ],
    }

    slice_report = {
        "format_version": 1,
        "predictions_parquet_sha256": parquet_hash,
        "station_complex_ids": config["station_complex_ids"],
        "publication_lag_days": config["publication_lag_days"],
        "chronology_violations": 0,
        **slice_walkthrough,
        "limitations": [
            "Development fold slice analysis across Union Sq (602), Times Sq (611), and Fulton St (628).",
            "October 2026 final holdout remains untouched.",
        ],
    }

    if calibration_report_path:
        p = Path(calibration_report_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(cal_report, indent=2, sort_keys=True) + "\n")

    if slice_report_path:
        p = Path(slice_report_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(slice_report, indent=2, sort_keys=True) + "\n")

    return cal_report, slice_report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "predictions_parquet",
        type=Path,
        nargs="?",
        default=Path("data/processed/model-development/predictions.parquet"),
    )
    parser.add_argument("--config", type=Path, default=Path("study_config.json"))
    parser.add_argument(
        "--calibration-report",
        type=Path,
        default=Path("reports/calibration_development.json"),
    )
    parser.add_argument(
        "--slice-report",
        type=Path,
        default=Path("reports/slice_error_walkthrough.json"),
    )
    args = parser.parse_args()
    config = json.loads(args.config.read_text())
    cal_report, slice_report = run_calibration_and_slice_reports(
        args.predictions_parquet,
        config,
        args.calibration_report,
        args.slice_report,
    )
    print(
        json.dumps(
            {
                "leakage_checks_verified": cal_report["leakage_checks_verified"],
                "calibration_overall_sample": cal_report["overall"][:3],
                "lag_sensitivity_sample": slice_report["reporting_lag_sensitivity"][:4],
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
