"""Chronological development folds and reporting-lag-aware seasonal baselines."""
import argparse
from bisect import bisect_right
from dataclasses import dataclass
from datetime import date, datetime, timedelta
import json
import math
from pathlib import Path
import shutil
import tempfile

import duckdb

from aggregate import calendar_hours, sql_path
from forecast_window import next_day_window
from ingest import ValidationError, sha256

PEAK_HOURS = (7, 8, 9, 16, 17, 18)
METHODS = ("seasonal_latest", "seasonal_mean4")


def next_month(day):
    return date(day.year + (day.month == 12), day.month % 12 + 1, 1)


def validate_config(config):
    dates = {key: date.fromisoformat(config[key]) for key in (
        "development_start", "development_end_exclusive", "calibration_start",
        "calibration_end_exclusive", "holdout_start", "holdout_end_exclusive")}
    if (config.get("format_version") != 1 or config.get("timezone") != "America/New_York"
        or config.get("dst_policy") != "exclude_entire_transition_day"
        or config.get("missing_policy") != "no_zero_imputation"):
        raise ValidationError("unsupported study configuration or target policy")
    if not (dates["development_start"] < dates["development_end_exclusive"] == dates["calibration_start"]
            < dates["calibration_end_exclusive"] == dates["holdout_start"] < dates["holdout_end_exclusive"]):
        raise ValidationError("study periods must be ordered, contiguous, and nonempty")
    stations, lags = config["station_complex_ids"], config["publication_lag_days"]
    if not stations or len(set(stations)) != len(stations) or any(not isinstance(s, str) or not s.isdigit() for s in stations):
        raise ValidationError("station IDs must be unique numeric strings")
    if not lags or len(set(lags)) != len(lags) or any(type(lag) is not int or lag < 1 for lag in lags):
        raise ValidationError("publication lags must be unique positive integer days")
    months = [date.fromisoformat(d) for d in config["validation_month_starts"]]
    if not months or months != sorted(set(months)) or any(
        m.day != 1 or m < dates["development_start"] or next_month(m) > dates["development_end_exclusive"] for m in months):
        raise ValidationError("validation months must be ordered unique months entirely inside development")
    return dates, months


def chronological_folds(config):
    dates, months = validate_config(config)
    return [{"validation_start": month.isoformat(), "validation_end_exclusive": next_month(month).isoformat(),
             "history_start": dates["development_start"].isoformat(),
             "first_issued_on": (month - timedelta(days=1)).isoformat(),
             "first_observed_through": {str(lag): next_day_window(month-timedelta(days=1), lag).observed_through.isoformat()
                                        for lag in config["publication_lag_days"]},
             "policy": "walk forward daily; only observations at/before each issuance cutoff"} for month in months]


@dataclass(frozen=True)
class Observation:
    station: str
    stamp: datetime  # naive New York local hour label
    target: object  # float or None (missing/DST)
    status: str


class SeasonalHistory:
    def __init__(self, observations):
        self.values, self.dates = {}, {}
        seen = set()
        for obs in observations:
            key = (obs.station, obs.stamp)
            if key in seen:
                raise ValidationError("duplicate station-hour in panel")
            seen.add(key)
            if obs.target is None:
                continue
            if obs.status != "observed" or not math.isfinite(obs.target) or obs.target < 0:
                raise ValidationError("invalid or excluded observation used as target")
            self.values.setdefault((obs.station, obs.stamp.hour), []).append((obs.stamp.date(), obs.target))
        for key, rows in self.values.items():
            rows.sort()
            self.dates[key] = [day for day, _ in rows]

    def references(self, station, hour, window):
        """Return at most four same-weekday observations, newest first."""
        rows = self.values.get((station, hour), [])
        stop = bisect_right(self.dates.get((station, hour), []), window.observed_through)
        references = []
        for day, value in reversed(rows[:stop]):
            if day.weekday() == window.target_day.weekday():
                if not window.can_use_observation(day) or day >= window.target_day:
                    raise ValidationError("unavailable observation reached seasonal predictor")
                references.append((day, value))
                if len(references) == 4:
                    break
        return references


def forecast(history, station, hour, issued_on, lag):
    window = next_day_window(issued_on, lag)
    refs = history.references(station, hour, window)
    return {"seasonal_latest": (refs[0][1], refs[:1]) if refs else (None, []),
            "seasonal_mean4": (sum(v for _, v in refs)/len(refs), refs) if refs else (None, [])}


def load_panel(directory, config):
    """Verify provenance and refuse inputs containing calibration/holdout dates."""
    dates, _ = validate_config(config)
    directory = Path(directory)
    audit = json.loads((directory/"audit.json").read_text())
    if audit.get("format_version") != 1 or audit.get("timezone") != config["timezone"]:
        raise ValidationError("unsupported panel audit")
    for filename in ("station_hours.duckdb", "station_hours.parquet"):
        if sha256(directory/filename) != audit["artifacts"][filename]:
            raise ValidationError("panel checksum mismatch: " + filename)
    selection = audit["selection"]
    start, end = date.fromisoformat(selection["start"]), date.fromisoformat(selection["end_exclusive"])
    if not dates["development_start"] <= start < end <= dates["development_end_exclusive"]:
        raise ValidationError("input selection extends outside development (calibration/holdout forbidden)")
    if sorted(selection["station_complex_ids"]) != sorted(config["station_complex_ids"]):
        raise ValidationError("panel station selection differs from study")
    with duckdb.connect() as db:
        raw = db.execute("SELECT station_complex_id,local_timestamp,target,status FROM read_parquet(?) ORDER BY 1,2",
                         [str(directory/"station_hours.parquet")]).fetchall()
    expected = {(station, row[0]): row[4] for station in config["station_complex_ids"] for row in calendar_hours(start,end)}
    if len(raw) != len(expected):
        raise ValidationError("panel does not contain the complete declared station-hour grid")
    seen, observations = set(), []
    for station, stamp, value, status in raw:
        key = (station, stamp)
        if key not in expected or key in seen:
            raise ValidationError("unexpected/duplicate station-hour key")
        seen.add(key)
        if status not in ("observed", "missing", "excluded_dst_day") or (status == "excluded_dst_day") != expected[key]:
            raise ValidationError("panel DST or status policy differs from study")
        if (value is not None) != (status == "observed"):
            raise ValidationError("missing/DST targets must be null and observed targets nonnull")
        if value is not None and (not math.isfinite(value) or value < 0):
            raise ValidationError("invalid panel target")
        observations.append(Observation(station, stamp, value, status))
    return observations, audit


def prediction_rows(observations, config):
    dates, months = validate_config(config)
    if any(not dates["development_start"] <= obs.stamp.date() < dates["development_end_exclusive"] for obs in observations):
        raise ValidationError("non-development observation refused")
    history = SeasonalHistory(observations)
    actuals = {(obs.station, obs.stamp.date(), obs.stamp.hour): obs for obs in observations}
    for month in months:
        day = month
        while day < next_month(month):
            issued = day-timedelta(days=1)
            for lag in config["publication_lag_days"]:
                window = next_day_window(issued,lag)
                for station in config["station_complex_ids"]:
                    for hour in range(24):
                        actual = actuals.get((station,day,hour))
                        if actual is None:
                            raise ValidationError("validation grid incomplete; download missing period before scoring")
                        predicted = ({method: (None, []) for method in METHODS} if actual.status == "excluded_dst_day"
                                     else forecast(history, station, hour, issued,lag))
                        for method, (value, refs) in predicted.items():
                            yield {"fold_start": month.isoformat(), "issued_on": issued.isoformat(),
                                "target_day": day.isoformat(), "observed_through": window.observed_through.isoformat(),
                                "station_complex_id": station, "hour": hour, "lag_days": lag, "method": method,
                                "actual": actual.target, "prediction": value, "target_status": actual.status,
                                "reference_count": len(refs),
                                "reference_oldest": refs[-1][0].isoformat() if refs else None,
                                "reference_newest": refs[0][0].isoformat() if refs else None,
                                "scoreable": actual.target is not None and value is not None,
                                "day_type": "weekend" if day.weekday() >= 5 else "weekday",
                                "hour_band": "peak" if hour in PEAK_HOURS else "off_peak"}
            day += timedelta(days=1)


PREDICTION_TYPES = {"fold_start":"DATE", "issued_on":"DATE", "target_day":"DATE", "observed_through":"DATE",
    "station_complex_id":"VARCHAR", "hour":"INTEGER", "lag_days":"INTEGER", "method":"VARCHAR",
    "actual":"DOUBLE", "prediction":"DOUBLE", "target_status":"VARCHAR", "reference_count":"INTEGER",
    "reference_oldest":"DATE", "reference_newest":"DATE", "scoreable":"BOOLEAN", "day_type":"VARCHAR", "hour_band":"VARCHAR"}


def metrics(db, dimensions):
    keys = ",".join(dimensions)
    query = f"""SELECT {keys},count(*) AS scheduled,count(actual) AS observed_targets,
        count(prediction) AS predictions_available,count(*) FILTER(WHERE scoreable) AS scored,
        count(*) FILTER(WHERE target_status='missing') AS missing_targets,
        count(*) FILTER(WHERE target_status='excluded_dst_day') AS dst_excluded_targets,
        avg(abs(prediction-actual)) FILTER(WHERE scoreable) AS mae,
        sqrt(avg(pow(prediction-actual,2)) FILTER(WHERE scoreable)) AS rmse,
        sum(abs(prediction-actual)) FILTER(WHERE scoreable) /
            nullif(sum(actual) FILTER(WHERE scoreable),0) AS wape
        FROM predictions GROUP BY {keys} ORDER BY {keys}"""
    cursor = db.execute(query)
    names = [item[0] for item in cursor.description]
    return [dict(zip(names,(value.isoformat() if isinstance(value,date) else value for value in row))) for row in cursor.fetchall()]


def run(directory, destination, config):
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError("backtest output already exists")
    observations, audit = load_panel(directory,config)
    # Require the declared history, not a cherry-picked recent subset.
    if audit["selection"]["start"] != config["development_start"] or audit["selection"]["end_exclusive"] != config["development_end_exclusive"]:
        raise ValidationError("full declared development panel required for this run")
    destination.parent.mkdir(parents=True,exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".backtest-",dir=destination.parent))
    try:
        with (stage/"predictions.jsonl").open("w") as stream:
            for row in prediction_rows(observations,config):
                stream.write(json.dumps(row,allow_nan=False)+"\n")
        with duckdb.connect() as db:
            columns = "{" + ",".join(f"'{name}':'{kind}'" for name,kind in PREDICTION_TYPES.items()) + "}"
            db.execute(f"CREATE TABLE predictions AS SELECT * FROM read_json(?,format='newline_delimited',columns={columns})", [str(stage/"predictions.jsonl")])
            violations = db.execute("""SELECT count(*) FROM predictions WHERE reference_newest>observed_through
                OR observed_through>=issued_on OR issued_on>=target_day OR target_day>=?""", [config["development_end_exclusive"]]).fetchone()[0]
            if violations:
                raise ValidationError("chronology audit failed")
            report = {"format_version":1, "study_config":config, "folds":chronological_folds(config),
                "input_source_rows_sha256":audit["source_rows_sha256"], "input_source_version":audit["source_version"],
                "panel_parquet_sha256":audit["artifacts"]["station_hours.parquet"], "chronology_violations":violations,
                "overall":metrics(db,["lag_days","method"]), "by_fold":metrics(db,["fold_start","lag_days","method"]),
                "by_station":metrics(db,["station_complex_id","lag_days","method"]),
                "by_day_type":metrics(db,["day_type","lag_days","method"]),
                "by_hour_band":metrics(db,["hour_band","lag_days","method"]),
                "peak_hours":list(PEAK_HOURS), "duckdb_version":duckdb.__version__,
                "limitations":["Development validation only; calibration and final holdout remain untouched.",
                    "Current revised records with simulated publication delays, not as-published historical releases.",
                    "Three Manhattan hubs; findings do not establish system-wide performance.",
                    "Missing targets/references are not imputed; report counts alongside error metrics.",
                    "All transition-day targets/references excluded; fare-partition completeness is not certified.",
                    "seasonal_mean4 averages up to four eligible observed same-weekday/hour references; fewer are allowed."]}
            db.execute("COPY predictions TO " + sql_path(stage/"predictions.parquet") + " (FORMAT PARQUET)")
        (stage/"predictions.jsonl").unlink()
        report["predictions_parquet_sha256"] = sha256(stage/"predictions.parquet")
        (stage/"metrics.json").write_text(json.dumps(report,indent=2,sort_keys=True,allow_nan=False)+"\n")
        if destination.exists():
            raise FileExistsError("backtest output appeared during run")
        stage.rename(destination)
        return report
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("panel",type=Path)
    parser.add_argument("--config",type=Path,default=Path("study_config.json"))
    parser.add_argument("--output",type=Path,required=True)
    args = parser.parse_args()
    report = run(args.panel,args.output,json.loads(args.config.read_text()))
    print(json.dumps({"overall":report["overall"],"chronology_violations":report["chronology_violations"]},indent=2))


if __name__ == "__main__":
    main()
