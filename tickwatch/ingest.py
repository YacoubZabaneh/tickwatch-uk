"""Refresh the local snapshot from Elanco or a local JSON array."""

import argparse
import json
import logging
import re
import sqlite3
import time
from collections import Counter
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import httpx
import ijson

from .store import SCHEMA, connect, database_path

SOURCE = "https://dev-task.elancoapps.com/data/tick-sightings"
LOGGER = logging.getLogger(__name__)

# Elanco supplies city names only. Extend this lookup as new cities appear.
CITY_REGIONS = {
    "Birmingham": "West Midlands",
    "Bristol": "South West",
    "Cardiff": "Wales",
    "Edinburgh": "Scotland",
    "Glasgow": "Scotland",
    "Leeds": "Yorkshire and The Humber",
    "Leicester": "East Midlands",
    "Liverpool": "North West",
    "London": "London",
    "Manchester": "North West",
    "Newcastle": "North East",
    "Nottingham": "East Midlands",
    "Sheffield": "Yorkshire and The Humber",
    "Southampton": "South East",
}
CITIES = {city.casefold(): city for city in CITY_REGIONS}
TIMESTAMP = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?")
INSERT = """
INSERT INTO sightings VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO NOTHING
"""


class ImportFailure(Exception):
    """The source could not be imported as a usable snapshot."""


def clean_text(value):
    if not isinstance(value, str):
        return None
    return " ".join(value.split()) or None


def normalise(record):
    if not isinstance(record, dict):
        return None, "record_is_not_an_object"
    source_id = record.get("id")
    if not isinstance(source_id, str) or not source_id.strip():
        return None, "missing_id"
    city = clean_text(record.get("location"))
    if city is None:
        return None, "missing_city"
    raw_date = record.get("date")
    if not isinstance(raw_date, str) or not TIMESTAMP.fullmatch(raw_date.strip()):
        return None, "invalid_date"
    try:
        observed_at = datetime.fromisoformat(raw_date.strip()).isoformat(timespec="microseconds")
    except ValueError:
        return None, "invalid_date"

    city_key = city.casefold()
    city = CITIES.get(city_key, city.title())
    region = CITY_REGIONS.get(city, "Unknown")
    return (
        source_id.strip(), observed_at, city, city_key, region, region.casefold(),
        clean_text(record.get("species")), clean_text(record.get("latinName")),
    ), None


def stream_records(chunks):
    """Validate the top-level array and yield its elements incrementally."""
    records = ijson.sendable_list()
    parser = ijson.items_coro(records, "item")
    started = False
    try:
        for chunk in chunks:
            if not started:
                first = chunk.lstrip(b" \t\r\n")
                if not first:
                    continue
                if not first.startswith(b"["):
                    raise ImportFailure("Expected a top-level JSON array")
                started = True
            parser.send(chunk)
            yield from records
            records.clear()
        parser.close()
        yield from records
    except ijson.JSONError as error:
        raise ImportFailure("Source returned incomplete or invalid JSON") from error


def replace_snapshot(path, records, source):
    counts = Counter()
    rejected = Counter()
    with connect(path) as database:
        database.execute("PRAGMA journal_mode=WAL")
        database.executescript(SCHEMA)
        database.execute("BEGIN IMMEDIATE")
        database.execute("DELETE FROM sightings")
        for record in records:
            counts["received"] += 1
            row, reason = normalise(record)
            if reason:
                rejected[reason] += 1
                continue
            if database.execute(INSERT, row).rowcount == 0:
                previous = tuple(database.execute(
                    "SELECT * FROM sightings WHERE id = ?", (row[0],)
                ).fetchone())
                counts["duplicates"] += 1
                counts["conflicting_duplicates"] += previous != row
                continue
            counts["stored"] += 1
            counts["unknown_region"] += row[4] == "Unknown"
            counts["incomplete_species"] += row[6] is None or row[7] is None

        if counts["stored"] == 0:
            raise ImportFailure("Source contained no usable sightings; previous snapshot preserved")
        summary = {
            "source": source,
            "refreshed_at": datetime.now(UTC).isoformat(),
            **{key: counts[key] for key in (
                "received", "stored", "duplicates", "conflicting_duplicates",
                "unknown_region", "incomplete_species",
            )},
            "rejected": sum(rejected.values()),
            "rejection_reasons": dict(rejected),
        }
        database.execute(
            "INSERT OR REPLACE INTO dataset VALUES (1, ?)", (json.dumps(summary),)
        )
    return summary


def import_file(database, file):
    with file.open("rb") as stream:
        chunks = iter(lambda: stream.read(65536), b"")
        return replace_snapshot(database, stream_records(chunks), f"file:{file.name}")


def retry_delay(value, attempt):
    if value:
        try:
            return max(0, int(value))
        except ValueError:
            try:
                return max(0, (parsedate_to_datetime(value) - datetime.now(UTC)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return 2 ** attempt


def import_live(database, url=SOURCE, *, transport=None):
    with httpx.Client(timeout=15, transport=transport) as client:
        for attempt in range(3):
            delay = 2 ** attempt
            try:
                with client.stream("GET", url, headers={"Accept": "application/json"}) as response:
                    response.raise_for_status()
                    records = stream_records(response.iter_bytes(chunk_size=65536))
                    return replace_snapshot(database, records, url)
            except httpx.HTTPStatusError as error:
                status = error.response.status_code
                if status not in (408, 429, 500, 502, 503, 504):
                    raise ImportFailure(f"Upstream returned HTTP {status}") from error
                failure = f"Upstream returned HTTP {status}"
                delay = retry_delay(error.response.headers.get("Retry-After"), attempt)
            except (httpx.RequestError, ImportFailure) as error:
                failure = str(error) or type(error).__name__
            if attempt == 2 or delay > 60:
                raise ImportFailure(f"{failure}; refresh aborted")
            LOGGER.warning("%s. Retrying in %.1fs", failure, delay)
            time.sleep(delay)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", type=Path, help="Import a local JSON array for an offline demo")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        path = database_path()
        summary = import_file(path, args.file) if args.file else import_live(path)
    except (ImportFailure, OSError, sqlite3.Error) as error:
        LOGGER.error("Import failed: %s", error)
        return 1
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
