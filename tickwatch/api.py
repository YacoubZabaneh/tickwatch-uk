"""Read-only API for searching sightings and exploring reported activity."""

import json
import logging
import sqlite3
from contextlib import contextmanager
from datetime import date
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, RedirectResponse
from pydantic import BaseModel

from .store import connect, database_path

app = FastAPI(
    title="TickWatch UK",
    version="1.0.0",
    description="Search and aggregate sightings from Elanco's educational dataset. "
    "Counts describe submitted reports and cannot establish tick or disease risk.",
)
LOGGER = logging.getLogger(__name__)


class Filters(BaseModel):
    start: date | None = None
    end: date | None = None
    city: str | None = None
    region: str | None = None


class Sighting(BaseModel):
    id: str
    observed_at: str
    city: str
    region: str
    species: str | None
    latin_name: str | None


class SightingPage(BaseModel):
    total: int
    limit: int
    offset: int
    next_offset: int | None
    items: list[Sighting]


class RegionCount(BaseModel):
    region: str
    count: int


class PeriodCount(BaseModel):
    period_start: date
    count: int


class Trends(BaseModel):
    interval: Literal["week", "month"]
    items: list[PeriodCount]


class DatasetSummary(BaseModel):
    source: str
    refreshed_at: str
    received: int
    stored: int
    duplicates: int
    conflicting_duplicates: int
    unknown_region: int
    incomplete_species: int
    rejected: int
    rejection_reasons: dict[str, int]
    first_sighting: str
    last_sighting: str


def get_filters(
    start: Annotated[date | None, Query(description="First included calendar date")] = None,
    end: Annotated[date | None, Query(description="Last included calendar date")] = None,
    city: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
    region: Annotated[str | None, Query(min_length=1, max_length=100)] = None,
):
    if start is not None and end is not None and start > end:
        raise HTTPException(422, "start must be on or before end")
    if any(value is not None and not value.strip() for value in (city, region)):
        raise HTTPException(422, "city and region must contain non-whitespace characters")
    return Filters(start=start, end=end, city=city, region=region)


def where(filters):
    clauses = []
    params = []
    if filters.start:
        clauses.append("observed_at >= ?")
        params.append(filters.start.isoformat())
    if filters.end:
        clauses.append("observed_at <= ?")
        params.append(f"{filters.end.isoformat()}T23:59:59.999999")
    for value, column in ((filters.city, "city_key"), (filters.region, "region_key")):
        if value is not None:
            clauses.append(f"{column} = ?")
            params.append(" ".join(value.split()).casefold())
    return (" WHERE " + " AND ".join(clauses) if clauses else ""), params


@app.exception_handler(sqlite3.Error)
async def handle_database_error(request: Request, error: sqlite3.Error):
    LOGGER.error("SQLite query failed on %s: %s", request.url.path, error)
    return JSONResponse(status_code=503, content={"detail": "Local dataset is temporarily unavailable"})


@contextmanager
def snapshot():
    path = database_path()
    if not path.is_file():
        raise HTTPException(503, "No dataset loaded. Run python -m tickwatch.ingest")
    with connect(path, read_only=True) as database:
        database.execute("BEGIN")
        row = database.execute("SELECT summary FROM dataset WHERE singleton = 1").fetchone()
        if row is None:
            raise HTTPException(503, "No successful dataset import yet")
        yield database, json.loads(row["summary"])


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", tags=["Operations"])
def health():
    """Readiness check for the local snapshot."""
    with snapshot():
        return {"status": "ready"}


@app.get("/v1/dataset", response_model=DatasetSummary, tags=["Operations"])
def dataset():
    """Latest successful import and data-quality counts."""
    with snapshot() as (database, summary):
        first, last = database.execute(
            "SELECT (SELECT observed_at FROM sightings ORDER BY observed_at LIMIT 1), "
            "(SELECT observed_at FROM sightings ORDER BY observed_at DESC LIMIT 1)"
        ).fetchone()
        return {**summary, "first_sighting": first, "last_sighting": last}


@app.get("/v1/sightings", response_model=SightingPage, tags=["Sightings"])
def sightings(
    filters: Annotated[Filters, Depends(get_filters)],
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0, le=2_147_483_647)] = 0,
):
    """Return matching sightings, newest first, with deterministic pagination."""
    condition, params = where(filters)
    with snapshot() as (database, _):
        total = database.execute("SELECT COUNT(*) FROM sightings" + condition, params).fetchone()[0]
        rows = database.execute(
            "SELECT id, observed_at, city, region, species, latin_name FROM sightings"
            + condition + " ORDER BY observed_at DESC, id DESC LIMIT ? OFFSET ?",
            [*params, limit, offset],
        ).fetchall()
        return {
            "total": total, "limit": limit, "offset": offset,
            "next_offset": offset + limit if offset + limit < total else None,
            "items": [dict(row) for row in rows],
        }


@app.get("/v1/reports/regions", response_model=list[RegionCount], tags=["Reports"])
def regions(filters: Annotated[Filters, Depends(get_filters)]):
    """Count matching reports by assigned UK region."""
    condition, params = where(filters)
    with snapshot() as (database, _):
        return [dict(row) for row in database.execute(
            "SELECT region, COUNT(*) AS count FROM sightings" + condition
            + " GROUP BY region ORDER BY count DESC, region", params,
        )]


@app.get("/v1/reports/trends", response_model=Trends, tags=["Reports"])
def trends(
    filters: Annotated[Filters, Depends(get_filters)],
    interval: Literal["week", "month"] = "month",
):
    """Count matching reports in observed periods. Weeks start on Monday."""
    condition, params = where(filters)
    period = (
        "date(substr(observed_at, 1, 10), '-6 days', 'weekday 1')"
        if interval == "week" else "substr(observed_at, 1, 7) || '-01'"
    )
    with snapshot() as (database, _):
        rows = database.execute(
            f"SELECT {period} AS period_start, COUNT(*) AS count FROM sightings"
            + condition + " GROUP BY period_start ORDER BY period_start", params,
        )
        return {"interval": interval, "items": [dict(row) for row in rows]}
