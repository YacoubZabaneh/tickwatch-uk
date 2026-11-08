"""SQLite schema and connection lifecycle shared by the importer and API."""

import os
import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS sightings (
    id TEXT PRIMARY KEY NOT NULL,
    observed_at TEXT NOT NULL,
    city TEXT NOT NULL,
    city_key TEXT NOT NULL,
    region TEXT NOT NULL,
    region_key TEXT NOT NULL,
    species TEXT,
    latin_name TEXT
);
CREATE INDEX IF NOT EXISTS sightings_date ON sightings(observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS sightings_city_date
    ON sightings(city_key, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS sightings_region_date
    ON sightings(region_key, observed_at DESC, id DESC);
CREATE TABLE IF NOT EXISTS dataset (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    summary TEXT NOT NULL
);
"""


def database_path() -> Path:
    return Path(os.environ.get("TICKWATCH_DB", "var/tickwatch.db"))


@contextmanager
def connect(path: Path, *, read_only: bool = False):
    if read_only:
        database = path.resolve().as_uri() + "?mode=ro"
    else:
        path.parent.mkdir(parents=True, exist_ok=True)
        database = str(path)
    connection = sqlite3.connect(database, uri=read_only, timeout=10)
    connection.row_factory = sqlite3.Row
    try:
        with connection:
            yield connection
    finally:
        connection.close()
