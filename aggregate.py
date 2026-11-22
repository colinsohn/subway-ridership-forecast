"""Build a station/local-hour panel without imputing missing observations."""
import argparse
from datetime import date, datetime, time, timedelta, timezone
from importlib.resources import files
import json
from pathlib import Path
import shutil
import tempfile
from zoneinfo import ZoneInfo

import duckdb
import tzdata

from ingest import ValidationError, sha256, verify_snapshot


def new_york_zone():
    # Read the pinned package explicitly instead of using host-dependent tzdata.
    resource = files("tzdata.zoneinfo").joinpath("America").joinpath("New_York")
    with resource.open("rb") as stream:
        return ZoneInfo.from_file(stream, key="America/New_York")


def local_hour_kind(stamp, zone):
    """Classify a naive local hour by its number of valid UTC round trips."""
    instants = set()
    for fold in (0, 1):
        utc = stamp.replace(tzinfo=zone, fold=fold).astimezone(timezone.utc)
        if utc.astimezone(zone).replace(tzinfo=None) == stamp:
            instants.add(utc)
    return {0: "nonexistent", 1: "ordinary", 2: "ambiguous"}[len(instants)]


def calendar_hours(start, end):
    zone = new_york_zone()
    records, day = [], start
    while day < end:
        hours = [datetime.combine(day, time(hour=h)) for h in range(24)]
        kinds = [local_hour_kind(stamp, zone) for stamp in hours]
        transition = any(kind != "ordinary" for kind in kinds)
        records.extend((stamp, day, stamp.hour, kind, transition) for stamp, kind in zip(hours, kinds))
        day += timedelta(days=1)
    return records


def sql_path(path):
    # DuckDB ATTACH/COPY paths cannot use bind parameters in this version.
    return "'" + str(Path(path).resolve()).replace("'", "''") + "'"


def aggregate(snapshot, destination):
    snapshot, destination = Path(snapshot), Path(destination)
    if destination.exists():
        raise FileExistsError("output exists; use a new aggregation directory")
    manifest = verify_snapshot(snapshot)
    selection = manifest["selection"]
    start, end = date.fromisoformat(selection["start"]), date.fromisoformat(selection["end_exclusive"])
    calendar = calendar_hours(start, end)
    stations = selection["station_complex_ids"]
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".aggregate-", dir=destination.parent))
    try:
        with duckdb.connect(str(stage / "station_hours.duckdb")) as db:
            db.execute("ATTACH " + sql_path(snapshot / "snapshot.duckdb") + " AS src (READ_ONLY)")
            db.execute("CREATE TEMP VIEW raw AS SELECT * FROM src.ridership_rows")
            db.execute("CREATE TABLE calendar (local_timestamp TIMESTAMP, service_date DATE, hour INTEGER, dst_kind VARCHAR, excluded_dst_day BOOLEAN)")
            db.executemany("INSERT INTO calendar VALUES (?,?,?,?,?)", calendar)
            db.execute("CREATE TABLE stations (station_complex_id VARCHAR PRIMARY KEY)")
            db.executemany("INSERT INTO stations VALUES (?)", [(s,) for s in stations])
            # A fare/payment pair is a partition, not another independent target.
            # Unknown or contradictory labels need review before summing.
            mismatches = db.execute("""SELECT count(*) FROM raw WHERE lower(payment_method) NOT IN ('omny','metrocard')
                OR NOT starts_with(lower(fare_class_category), lower(payment_method) || ' - ')""").fetchone()[0]
            if mismatches:
                raise ValidationError(f"{mismatches} fare/payment rows have unknown or contradictory labels")
            db.execute("""CREATE TEMP TABLE duplicate_keys AS SELECT transit_timestamp, station_complex_id,
                payment_method, fare_class_category, count(*) AS n FROM raw
                GROUP BY ALL HAVING count(*) > 1""")
            ordinary_duplicates = db.execute("""SELECT count(*) FROM duplicate_keys d JOIN calendar c
                ON d.transit_timestamp=c.local_timestamp WHERE NOT c.excluded_dst_day""").fetchone()[0]
            if ordinary_duplicates:
                raise ValidationError(f"{ordinary_duplicates} duplicate fare/payment keys on non-DST dates; aggregation refused")
            db.execute("""CREATE TEMP TABLE observed AS SELECT station_complex_id,
                transit_timestamp AS local_timestamp, sum(ridership) AS ridership_observed,
                sum(transfers) AS transfers_observed, count(*) AS source_rows,
                count(DISTINCT payment_method) AS payment_methods,
                count(DISTINCT (payment_method, fare_class_category)) AS fare_pairs
                FROM raw GROUP BY ALL""")
            db.execute("""CREATE TABLE station_hours AS SELECT s.station_complex_id,
                c.local_timestamp, c.service_date, c.hour, c.dst_kind, c.excluded_dst_day,
                o.ridership_observed, o.transfers_observed,
                coalesce(o.source_rows,0) AS source_rows,
                coalesce(o.payment_methods,0) AS payment_methods,
                coalesce(o.fare_pairs,0) AS fare_pairs,
                CASE WHEN NOT c.excluded_dst_day AND o.source_rows IS NOT NULL
                     THEN o.ridership_observed ELSE NULL END AS target,
                CASE WHEN c.excluded_dst_day THEN 'excluded_dst_day'
                     WHEN o.source_rows IS NULL THEN 'missing' ELSE 'observed' END AS status
                FROM stations s CROSS JOIN calendar c
                LEFT JOIN observed o ON o.station_complex_id=s.station_complex_id
                    AND o.local_timestamp=c.local_timestamp
                ORDER BY s.station_complex_id,c.local_timestamp""")
            # Verify conservation on the raw local-hour labels, including DST
            # rows retained for auditing. Transfers are never added to entries.
            raw_total = db.execute("SELECT sum(ridership) FROM raw").fetchone()[0]
            grouped_total = db.execute("SELECT sum(ridership_observed) FROM station_hours").fetchone()[0]
            if abs(raw_total - grouped_total) > max(1e-6, abs(raw_total)*1e-12):
                raise ValidationError("aggregation did not conserve source ridership")
            coverage = []
            for station in stations:
                expected, observed, missing, dst, all_observed, minimum, maximum = db.execute("""SELECT
                    count(*) FILTER (WHERE NOT excluded_dst_day),
                    count(*) FILTER (WHERE status='observed'),
                    count(*) FILTER (WHERE status='missing'),
                    count(*) FILTER (WHERE excluded_dst_day),
                    count(*) FILTER (WHERE source_rows>0),
                    min(fare_pairs) FILTER (WHERE source_rows>0),
                    max(fare_pairs) FILTER (WHERE source_rows>0)
                    FROM station_hours WHERE station_complex_id=?""", [station]).fetchone()
                coverage.append({"station_complex_id": station, "eligible_wall_hours": expected,
                    "observed_eligible_hours": observed, "missing_eligible_hours": missing,
                    "excluded_dst_wall_hours": dst, "observed_wall_hours_including_dst": all_observed,
                    "eligible_coverage": observed/expected if expected else None,
                    "observed_fare_pairs_min": minimum, "observed_fare_pairs_max": maximum})
            # Pair-absence diagnostics use only within-snapshot reference pairs;
            # they neither establish incomplete data nor become model features.
            fare_pairs = [dict(zip(("station_complex_id", "payment_method", "fare_class_category", "rows", "ridership"), row))
                for row in db.execute("""SELECT station_complex_id,payment_method,fare_class_category,
                    count(*),sum(ridership) FROM raw GROUP BY ALL ORDER BY 1,2,3""").fetchall()]
            pair_gaps = db.execute("""WITH pairs AS (SELECT DISTINCT station_complex_id,payment_method,fare_class_category FROM raw),
                hours AS (SELECT station_complex_id,local_timestamp FROM observed)
                SELECT count(*) FROM hours h JOIN pairs p USING(station_complex_id)
                WHERE NOT EXISTS (SELECT 1 FROM raw r WHERE r.station_complex_id=h.station_complex_id
                    AND r.transit_timestamp=h.local_timestamp AND r.payment_method=p.payment_method
                    AND r.fare_class_category=p.fare_class_category)""").fetchone()[0]
            dst_observations = [dict(zip(("station_complex_id", "local_timestamp", "dst_kind", "source_rows", "ridership_observed"),
                (r[0], r[1].isoformat(), *r[2:]))) for r in db.execute("""SELECT station_complex_id,local_timestamp,
                    dst_kind,source_rows,ridership_observed FROM station_hours WHERE dst_kind<>'ordinary' ORDER BY 1,2""").fetchall()]
            duplicates = db.execute("SELECT count(*) FROM duplicate_keys").fetchone()[0]
            grid_count = db.execute("SELECT count(*) FROM station_hours").fetchone()[0]
            db.execute("COPY station_hours TO " + sql_path(stage / "station_hours.parquet") + " (FORMAT PARQUET)")
            db.execute("DETACH src")
        report = {"format_version": 1, "selection": selection, "source_rows_sha256": manifest["rows_sha256"],
            "source_version": manifest["source_version"], "input_rows": manifest["expected_source_rows"],
            "calendar_wall_hour_labels": len(calendar), "station_hour_grid_rows": grid_count,
            "ridership_total_before": raw_total, "ridership_total_after": grouped_total,
            "ordinary_duplicate_key_groups": ordinary_duplicates, "dst_duplicate_key_groups": duplicates,
            "fare_payment_mismatch_rows": mismatches, "coverage_by_station": coverage,
            "fare_payment_partitions": fare_pairs, "absent_pair_slots_at_observed_hours": pair_gaps,
            "dst_observations": dst_observations, "timezone": "America/New_York", "tzdata_version": tzdata.__version__,
            "policy": {"service_date": "local calendar date, midnight boundary",
                "target": "sum ridership across observed fare/payment rows; transfers not added",
                "missing": "NULL target; no zero imputation",
                "dst": "retain raw local-hour sums for audit; exclude entire transition day from targets",
                "fare_pair_absence": "diagnostic only; absence may mean zero transactions or missing data; completeness is not certified"},
            "limitations": ["Observed hour coverage is not proof that every fare partition was reported.",
                "Fall-back source timestamps cannot identify the two physical occurrences of the repeated hour.",
                "Source data are currently revised records, not historical publication snapshots.",
                "No forecasting model or accuracy evaluation is included."],
            "artifacts": {name: sha256(stage/name) for name in ("station_hours.duckdb", "station_hours.parquet")}}
        (stage / "audit.json").write_text(json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n")
        if destination.exists():
            raise FileExistsError("output appeared during aggregation")
        stage.rename(destination)
        return report
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = aggregate(args.snapshot, args.output)
    print(json.dumps({"station_hour_grid_rows": report["station_hour_grid_rows"],
                      "coverage_by_station": report["coverage_by_station"],
                      "dst_observations": report["dst_observations"]}, indent=2))


if __name__ == "__main__":
    main()
