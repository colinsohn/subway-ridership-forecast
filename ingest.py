"""Download a bounded MTA snapshot and validate it with Python and DuckDB.

No credentials are required. Output is staged and published as a directory only
when all pages, source-version guards, row checks, and database checks pass.
"""
import argparse
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
from pathlib import Path
import re
import shutil
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

DATASET_ID = "5wq4-mkjj"
ENDPOINT = f"https://data.ny.gov/resource/{DATASET_ID}.json"
METADATA_URL = f"https://data.ny.gov/api/views/{DATASET_ID}.json"
TEXT_FIELDS = (
    "transit_mode", "station_complex_id", "station_complex", "borough",
    "payment_method", "fare_class_category",
)
FIELDS = ("source_row_id", "transit_timestamp", *TEXT_FIELDS, "ridership", "transfers")
SELECT = ":id as source_row_id," + ",".join(FIELDS[1:])


class ValidationError(ValueError):
    """A snapshot cannot be trusted as a complete, valid requested selection."""


@dataclass(frozen=True)
class Selection:
    start: date
    end: date  # exclusive; timestamps are local New York wall times
    stations: tuple

    def __post_init__(self):
        if self.start >= self.end:
            raise ValidationError("start must precede end (exclusive)")
        if not self.stations or any(not re.fullmatch(r"[0-9]+", s) for s in self.stations):
            raise ValidationError("select one or more numeric station complex IDs")
        if len(set(self.stations)) != len(self.stations):
            raise ValidationError("duplicate station IDs")

    def where(self):
        ids = ",".join("'" + s + "'" for s in sorted(self.stations))
        return (f"transit_mode = 'subway' AND transit_timestamp >= '{self.start}T00:00:00' "
                f"AND transit_timestamp < '{self.end}T00:00:00' AND station_complex_id in ({ids})")


def request_json(url):
    """Bounded retry of throttling, temporary HTTP errors, and network failures."""
    for attempt in range(3):
        try:
            req = Request(url, headers={"User-Agent": "subway-ridership-forecast/0.1", "Accept": "application/json"})
            with urlopen(req, timeout=60) as response:
                return json.load(response)
        except HTTPError as exc:
            if exc.code not in (429, 500, 502, 503, 504) or attempt == 2:
                raise
        except (URLError, TimeoutError):
            if attempt == 2:
                raise
        time.sleep(2 ** attempt)


def canonical(value):
    return (json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False) + "\n").encode()


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def source_version(metadata):
    if metadata.get("id") != DATASET_ID or not isinstance(metadata.get("rowsUpdatedAt"), int):
        raise ValidationError("missing dataset identity or rowsUpdatedAt revision guard")
    columns = {c["fieldName"]: c["dataTypeName"] for c in metadata.get("columns", [])}
    expected = {**{f: "text" for f in TEXT_FIELDS}, "transit_timestamp": "calendar_date",
                "ridership": "number", "transfers": "number"}
    if any(columns.get(f) != kind for f, kind in expected.items()):
        raise ValidationError("required source columns or types changed")
    return {"dataset_id": metadata["id"], "rows_updated_at": metadata["rowsUpdatedAt"],
            "required_column_types": expected}


def validate_row(row, selection):
    if not isinstance(row, dict) or any(f not in row or not isinstance(row[f], str) or not row[f].strip() for f in FIELDS):
        raise ValidationError("missing, empty, or non-string source field")
    try:
        stamp = datetime.fromisoformat(row["transit_timestamp"])
    except ValueError as exc:
        raise ValidationError("invalid local timestamp") from exc
    if stamp.tzinfo is not None or stamp.minute or stamp.second or stamp.microsecond:
        raise ValidationError("expected a naive local timestamp on an hour boundary")
    if not selection.start <= stamp.date() < selection.end:
        raise ValidationError("row outside requested date range")
    if row["transit_mode"] != "subway" or row["station_complex_id"] not in selection.stations:
        raise ValidationError("row outside requested mode/stations")
    try:
        riders, transfers = (Decimal(row[f]) for f in ("ridership", "transfers"))
        if not all(x.is_finite() and math.isfinite(float(x)) for x in (riders, transfers)):
            raise ValidationError("nonfinite count")
        if not 0 <= transfers <= riders:
            raise ValidationError("counts must satisfy 0 <= transfers <= ridership")
    except InvalidOperation as exc:
        raise ValidationError("invalid count") from exc


def validate_jsonl(path, selection):
    seen = set()
    with Path(path).open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            validate_row(row, selection)
            if row["source_row_id"] in seen:
                raise ValidationError("duplicate source row ID across pages")
            seen.add(row["source_row_id"])
    if not seen:
        raise ValidationError("empty selection; no usable snapshot")
    return len(seen)


def build_database(jsonl, database):
    """Explicit types preserve station identifiers; no JSON type guessing."""
    with duckdb.connect(str(database)) as db:
        columns = "{" + ",".join(f"'{f}':'VARCHAR'" for f in FIELDS) + "}"
        db.execute(f"""CREATE TABLE ridership_rows AS SELECT
            source_row_id, CAST(transit_timestamp AS TIMESTAMP) AS transit_timestamp,
            transit_mode, station_complex_id, station_complex, borough,
            payment_method, fare_class_category,
            CAST(ridership AS DOUBLE) AS ridership, CAST(transfers AS DOUBLE) AS transfers
            FROM read_json(?, format='newline_delimited', columns={columns})""", [str(jsonl)])
        rows, stations, first, last, riders = db.execute("""SELECT count(*), count(DISTINCT station_complex_id),
            min(transit_timestamp), max(transit_timestamp), sum(ridership) FROM ridership_rows""").fetchone()
        # Natural-key duplicates are reported for the subsequent station/hour
        # audit, never silently collapsed or called duplicated source rows.
        duplicate_groups = db.execute("""SELECT count(*) FROM (
            SELECT 1 FROM ridership_rows GROUP BY transit_timestamp, transit_mode,
            station_complex_id, payment_method, fare_class_category HAVING count(*) > 1)""").fetchone()[0]
        return {"rows": rows, "stations_observed": stations, "first_local_hour": first.isoformat(),
                "last_local_hour": last.isoformat(), "ridership_sum": riders,
                "natural_key_duplicate_groups": duplicate_groups, "duckdb_version": duckdb.__version__}


def download(selection, destination, page_size=5000, max_rows=250000, fetch=request_json):
    destination = Path(destination)
    if destination.exists():
        raise FileExistsError("destination already exists; use a new snapshot directory")
    if not 1 <= page_size <= 50000 or max_rows < 1:
        raise ValidationError("page size must be 1..50000 and max rows positive")
    before = fetch(METADATA_URL)
    version = source_version(before)
    count_url = ENDPOINT + "?" + urlencode({"$select": "count(*) as rows", "$where": selection.where()})
    counts = fetch(count_url)
    if not isinstance(counts, list) or len(counts) != 1:
        raise ValidationError("invalid source count response")
    expected = int(counts[0]["rows"])
    if not 0 < expected <= max_rows:
        raise ValidationError(f"selection has {expected} rows; expected 1..{max_rows}")
    destination.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=".ingest-", dir=destination.parent))
    started = datetime.now(timezone.utc).isoformat()
    try:
        raw = stage / "rows.jsonl"
        pages, offset = [], 0
        with raw.open("wb") as stream:
            while offset < expected:
                url = ENDPOINT + "?" + urlencode({"$select": SELECT, "$where": selection.where(),
                    "$order": ":id", "$limit": str(page_size), "$offset": str(offset)})
                page = fetch(url)
                if not isinstance(page, list) or not page or len(page) > page_size:
                    raise ValidationError("empty or invalid page before expected count")
                for row in page:
                    validate_row(row, selection)
                    stream.write(canonical({f: row[f] for f in FIELDS}))
                pages.append({"offset": offset, "rows": len(page)})
                offset += len(page)
        if offset != expected:
            raise ValidationError("downloaded count differs from source count")
        after = fetch(METADATA_URL)
        if source_version(after) != version:
            raise ValidationError("source changed during download; retry a new snapshot")
        observed = validate_jsonl(raw, selection)
        if observed != expected:
            raise ValidationError("validated count differs from source count")
        (stage / "source_metadata.json").write_bytes(canonical(before))
        summary = build_database(raw, stage / "snapshot.duckdb")
        if summary["rows"] != expected:
            raise ValidationError("DuckDB count differs from source count")
        manifest = {
            "format_version": 1, "source_url": ENDPOINT, "metadata_url": METADATA_URL,
            "selection": {"start": selection.start.isoformat(), "end_exclusive": selection.end.isoformat(),
                          "station_complex_ids": sorted(selection.stations), "mode": "subway"},
            "query": {"select": SELECT, "where": selection.where(), "order": ":id", "page_size": page_size},
            "started_at_utc": started, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "source_version": version, "source_metadata_sha256": sha256(stage / "source_metadata.json"),
            "rows_sha256": sha256(raw), "database_sha256": sha256(stage / "snapshot.duckdb"),
            "expected_source_rows": expected, "pages": pages,
            "validation": summary,
            "limitations": ["Current revised historical data, not historical publication snapshots.",
                "rowsUpdatedAt checked before/after; API pagination is not a transactional snapshot.",
                "Timestamps are New York local wall hours; DST ambiguity and missing hours remain for the aggregation audit.",
                "Transfers are already included in ridership; do not add them.",
                "Validation does not establish complete hourly coverage or forecasting accuracy."]}
        (stage / "manifest.json").write_bytes(canonical(manifest))
        # A new destination is required; failures never replace a prior snapshot.
        if destination.exists():
            raise FileExistsError("destination appeared during download")
        stage.rename(destination)
        return manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def verify_snapshot(directory):
    """Check stored source identity, byte integrity, and rows without networking."""
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest["format_version"] != 1:
        raise ValidationError("unsupported manifest version")
    for filename, field in [("rows.jsonl", "rows_sha256"), ("source_metadata.json", "source_metadata_sha256"),
                            ("snapshot.duckdb", "database_sha256")]:
        if sha256(directory / filename) != manifest[field]:
            raise ValidationError(f"checksum mismatch: {filename}")
    if source_version(json.loads((directory / "source_metadata.json").read_text())) != manifest["source_version"]:
        raise ValidationError("source metadata does not match manifest")
    s = manifest["selection"]
    selection = Selection(date.fromisoformat(s["start"]), date.fromisoformat(s["end_exclusive"]), tuple(s["station_complex_ids"]))
    count = validate_jsonl(directory / "rows.jsonl", selection)
    if count != manifest["expected_source_rows"]:
        raise ValidationError("snapshot row count mismatch")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    fetch = sub.add_parser("download")
    fetch.add_argument("--start", type=date.fromisoformat, required=True)
    fetch.add_argument("--end", type=date.fromisoformat, required=True, help="exclusive local service date")
    fetch.add_argument("--stations", nargs="+", required=True)
    fetch.add_argument("--output", type=Path, required=True)
    fetch.add_argument("--page-size", type=int, default=5000)
    fetch.add_argument("--max-rows", type=int, default=250000)
    verify = sub.add_parser("verify")
    verify.add_argument("directory", type=Path)
    args = parser.parse_args()
    if args.command == "download":
        manifest = download(Selection(args.start, args.end, tuple(args.stations)), args.output, args.page_size, args.max_rows)
    else:
        manifest = verify_snapshot(args.directory)
    print(json.dumps(manifest["validation"], indent=2))


if __name__ == "__main__":
    main()
