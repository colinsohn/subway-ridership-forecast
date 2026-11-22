from datetime import date, datetime
import json
from pathlib import Path
import tempfile
import unittest

import duckdb

from aggregate import aggregate, calendar_hours, local_hour_kind, new_york_zone
from ingest import Selection, ValidationError, download, verify_snapshot
from test_ingest import FakeSource, row


class AggregateTests(unittest.TestCase):
    def snapshot(self, tmp, rows, start=date(2025, 1, 6), end=date(2025, 1, 7), stations=("611",)):
        path = Path(tmp) / "source's snapshot"
        download(Selection(start, end, stations), path, fetch=FakeSource(rows))
        return path

    def test_sum_partitions_missing_zero_and_unobserved_station(self):
        rows = [row(ridership="10", transfers="2"),
                row("row-2", payment_method="metrocard", fare_class_category="MetroCard - Full Fare", ridership="5", transfers="1"),
                row("row-3", "2025-01-06T01:00:00.000", ridership="0", transfers="0")]
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self.snapshot(tmp, rows, stations=("611", "602"))
            original = verify_snapshot(snapshot)
            output = Path(tmp)/"panel's output"
            report = aggregate(snapshot, output)
            self.assertEqual(verify_snapshot(snapshot), original)  # input is immutable
            self.assertEqual(report["ridership_total_before"], 15)
            self.assertEqual(report["ridership_total_after"], 15)
            self.assertEqual(report["station_hour_grid_rows"], 48)
            coverage = {s["station_complex_id"]: s for s in report["coverage_by_station"]}
            self.assertEqual(coverage["611"]["missing_eligible_hours"], 22)
            self.assertEqual(coverage["602"]["missing_eligible_hours"], 24)
            self.assertGreater(report["absent_pair_slots_at_observed_hours"], 0)
            with duckdb.connect(str(output/"station_hours.duckdb"), read_only=True) as db:
                actual = db.execute("SELECT hour,target,transfers_observed,status,source_rows FROM station_hours WHERE station_complex_id='611' AND hour<3 ORDER BY hour").fetchall()
                self.assertEqual(actual, [(0,15.0,3.0,'observed',2),(1,0.0,0.0,'observed',1),(2,None,None,'missing',0)])
                self.assertEqual(db.execute("SELECT count(*) FROM read_parquet(?)", [str(output/"station_hours.parquet")]).fetchone()[0], 48)
            self.assertEqual(json.loads((output/"audit.json").read_text()), report)
            with self.assertRaises(FileExistsError):
                aggregate(snapshot, output)

    def test_non_dst_duplicates_and_fare_mismatch_refused(self):
        for rows in [[row(), row("row-2")], [row(fare_class_category="MetroCard - Full Fare")]]:
            with self.subTest(rows=rows), tempfile.TemporaryDirectory() as tmp:
                snapshot = self.snapshot(tmp, rows)
                output = Path(tmp)/"panel"
                with self.assertRaises(ValidationError):
                    aggregate(snapshot, output)
                self.assertFalse(output.exists())
                self.assertFalse(any(p.name.startswith('.aggregate-') for p in Path(tmp).iterdir()))

    def test_pinned_dst_calendar(self):
        zone = new_york_zone()
        self.assertEqual(local_hour_kind(datetime(2025,3,9,2), zone), 'nonexistent')
        self.assertEqual(local_hour_kind(datetime(2025,11,2,1), zone), 'ambiguous')
        self.assertEqual(local_hour_kind(datetime(2025,1,1,1), zone), 'ordinary')
        records = calendar_hours(date(2025,3,8), date(2025,3,11))
        self.assertEqual(len(records), 72)
        self.assertEqual(sum(r[4] for r in records), 24)
        self.assertEqual(sum(r[3]=='nonexistent' for r in records), 1)

    def test_dst_rows_preserved_for_audit_and_entire_day_excluded(self):
        for day, hour, kind in [(date(2025,3,9),2,'nonexistent'),(date(2025,11,2),1,'ambiguous')]:
            # Include an impossible/repeated source label deliberately. The
            # source lacks offsets; the audit retains it, never invents UTCs.
            stamp = f'{day}T{hour:02d}:00:00.000'
            rows = [row("one", stamp), row("two", stamp)]
            with self.subTest(day=day), tempfile.TemporaryDirectory() as tmp:
                snapshot = self.snapshot(tmp, rows, day, date.fromordinal(day.toordinal()+1))
                output = Path(tmp)/"panel"
                report = aggregate(snapshot, output)
                coverage = report['coverage_by_station'][0]
                self.assertEqual(coverage['eligible_wall_hours'],0)
                self.assertIsNone(coverage['eligible_coverage'])
                self.assertEqual(coverage['excluded_dst_wall_hours'],24)
                self.assertEqual(report['dst_duplicate_key_groups'],1)
                self.assertEqual(report['dst_observations'][0]['dst_kind'],kind)
                with duckdb.connect(str(output/"station_hours.duckdb"),read_only=True) as db:
                    self.assertEqual(db.execute('SELECT count(target),sum(ridership_observed) FROM station_hours').fetchone(),(0,21.0))

    def test_midnight_boundary_is_calendar_date(self):
        rows = [row('first','2025-01-06T23:00:00.000'),row('second','2025-01-07T00:00:00.000')]
        with tempfile.TemporaryDirectory() as tmp:
            snapshot = self.snapshot(tmp,rows,end=date(2025,1,8))
            output=Path(tmp)/'panel';aggregate(snapshot,output)
            with duckdb.connect(str(output/'station_hours.duckdb'),read_only=True) as db:
                self.assertEqual(db.execute("SELECT service_date,hour FROM station_hours WHERE source_rows>0 ORDER BY local_timestamp").fetchall(),[(date(2025,1,6),23),(date(2025,1,7),0)])

    def test_corrupt_snapshot_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            snapshot=self.snapshot(tmp,[row()])
            with (snapshot/'rows.jsonl').open('a') as stream:stream.write('tampered\n')
            with self.assertRaisesRegex(ValidationError,'checksum'):
                aggregate(snapshot,Path(tmp)/'panel')


if __name__ == '__main__':
    unittest.main()
