import copy
from datetime import date
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse

import duckdb
import ingest


def row(source_id="row-1", stamp="2025-01-06T00:00:00.000", **changes):
    value = {"source_row_id": source_id, "transit_timestamp": stamp,
        "transit_mode": "subway", "station_complex_id": "611", "station_complex": "Demo station",
        "borough": "Manhattan", "payment_method": "omny", "fare_class_category": "OMNY - Full Fare",
        "ridership": "10.5", "transfers": "2.0"}
    value.update(changes)
    return value


def metadata():
    types = {**{f: "text" for f in ingest.TEXT_FIELDS}, "transit_timestamp": "calendar_date",
             "ridership": "number", "transfers": "number"}
    return {"id": ingest.DATASET_ID, "rowsUpdatedAt": 100,
            "columns": [{"fieldName": f, "dataTypeName": kind} for f, kind in types.items()]}


class FakeSource:
    def __init__(self, rows, changed=False, fail_page=False):
        self.rows, self.changed, self.fail_page = rows, changed, fail_page
        self.urls, self.metadata_calls = [], 0

    def __call__(self, url):
        self.urls.append(url)
        if url == ingest.METADATA_URL:
            self.metadata_calls += 1
            value = metadata()
            if self.changed and self.metadata_calls > 1:
                value["rowsUpdatedAt"] += 1
            return value
        q = parse_qs(urlparse(url).query)
        if q["$select"] == ["count(*) as rows"]:
            return [{"rows": str(len(self.rows))}]
        if self.fail_page:
            raise OSError("interrupted download")
        offset, limit = int(q["$offset"][0]), int(q["$limit"][0])
        return copy.deepcopy(self.rows[offset:offset + limit])


class IngestTests(unittest.TestCase):
    def setUp(self):
        self.selection = ingest.Selection(date(2025, 1, 6), date(2025, 1, 7), ("611",))

    def test_selection_and_query_bounds(self):
        self.assertIn("< '2025-01-07T00:00:00'", self.selection.where())
        for stations in [(), ("611", "611"), ("611'); DROP TABLE t;--",)]:
            with self.assertRaises(ingest.ValidationError):
                ingest.Selection(self.selection.start, self.selection.end, stations)
        with self.assertRaises(ingest.ValidationError):
            ingest.Selection(self.selection.end, self.selection.start, ("611",))

    def test_paging_database_and_offline_integrity(self):
        source = FakeSource([row(), row("row-2", payment_method="metrocard"), row("row-3", ridership="0", transfers="0")])
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "snapshot"
            manifest = ingest.download(self.selection, output, page_size=2, fetch=source)
            self.assertEqual(manifest["pages"], [{"offset": 0, "rows": 2}, {"offset": 2, "rows": 1}])
            self.assertEqual(manifest["validation"]["rows"], 3)
            self.assertEqual(manifest["validation"]["ridership_sum"], 21.0)
            self.assertEqual(manifest["validation"]["natural_key_duplicate_groups"], 1)
            for url in source.urls:
                if "$offset" in parse_qs(urlparse(url).query):
                    self.assertEqual(parse_qs(urlparse(url).query)["$order"], [":id"])
            with duckdb.connect(str(output / "snapshot.duckdb"), read_only=True) as db:
                self.assertEqual(db.execute("SELECT DISTINCT station_complex_id FROM ridership_rows").fetchall(), [("611",)])
            self.assertEqual(ingest.verify_snapshot(output), manifest)
            for filename in ["snapshot.duckdb", "source_metadata.json"]:
                path = output / filename
                original = path.read_bytes()
                path.write_bytes(original + b"tampered")
                with self.assertRaisesRegex(ingest.ValidationError, "checksum"):
                    ingest.verify_snapshot(output)
                path.write_bytes(original)
            with self.assertRaises(FileExistsError):
                ingest.download(self.selection, output, fetch=source)
            with (output / "rows.jsonl").open("a") as stream:
                stream.write(json.dumps(row("tampered")) + "\n")
            with self.assertRaisesRegex(ingest.ValidationError, "checksum"):
                ingest.verify_snapshot(output)

    def test_invalid_rows(self):
        invalid = [row(transit_timestamp="bad"), row(transit_timestamp="2025-01-06T00:30:00"),
            row(transit_timestamp="2025-01-06T00:00:00+00:00"), row(transit_timestamp="2025-01-07T00:00:00"),
            row(station_complex_id="602"), row(transit_mode="tram"), row(ridership="NaN"),
            row(ridership="Infinity"), row(ridership="-1"), row(transfers="11"), row(ridership="no"),
            row(payment_method=""), row(ridership=None), row(ridership=1), row(transfers="-1")]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ingest.ValidationError):
                ingest.validate_row(value, self.selection)
        ingest.validate_row(row(ridership="0", transfers="0"), self.selection)
        ingest.validate_row(row(ridership="1.25", transfers="0.1"), self.selection)

    def test_incomplete_unstable_duplicate_and_failed_downloads_leave_no_output(self):
        sources = [FakeSource([], False), FakeSource([row()], changed=True),
                   FakeSource([row(), row()]), FakeSource([row()], fail_page=True)]
        for source in sources:
            with self.subTest(source=source), tempfile.TemporaryDirectory() as tmp:
                output = Path(tmp) / "snapshot"
                with self.assertRaises((ingest.ValidationError, OSError)):
                    ingest.download(self.selection, output, page_size=1, fetch=source)
                self.assertFalse(output.exists())
                self.assertEqual(list(Path(tmp).iterdir()), [])
        source = FakeSource([row(), row("row-2")])
        def short_response(url):
            if "$offset" in parse_qs(urlparse(url).query):
                return []
            return source(url)
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ingest.ValidationError):
                ingest.download(self.selection, Path(tmp)/"snapshot", fetch=short_response)

    def test_source_schema_and_row_cap(self):
        for change in [lambda m: m.pop("rowsUpdatedAt"), lambda m: m.update(id="wrong"),
                       lambda m: m["columns"].pop()]:
            value = metadata(); change(value)
            with self.assertRaises(ingest.ValidationError):
                ingest.source_version(value)
        with tempfile.TemporaryDirectory() as tmp:
            for page_size, max_rows in [(0, 10), (50001, 10), (10, 0), (10, 1)]:
                with self.assertRaises(ingest.ValidationError):
                    ingest.download(self.selection, Path(tmp)/"snapshot", page_size, max_rows,
                                    fetch=FakeSource([row(), row("row-2")]))

    def test_transient_retry_and_permanent_errors(self):
        transient = HTTPError(ingest.ENDPOINT, 429, "throttled", {}, None)
        with patch("ingest.urlopen", side_effect=[transient] * 3) as call, patch("ingest.time.sleep") as sleep:
            with self.assertRaises(HTTPError):
                ingest.request_json(ingest.ENDPOINT)
            self.assertEqual(call.call_count, 3)
            self.assertEqual(sleep.call_count, 2)
        permanent = HTTPError(ingest.ENDPOINT, 400, "bad query", {}, None)
        with patch("ingest.urlopen", side_effect=permanent) as call:
            with self.assertRaises(HTTPError):
                ingest.request_json(ingest.ENDPOINT)
            self.assertEqual(call.call_count, 1)


if __name__ == "__main__":
    unittest.main()
