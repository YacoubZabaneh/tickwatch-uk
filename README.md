# TickWatch UK

Elanco Backend Placement Technical Task

**Candidate:** Yacoub Zabaneh

TickWatch UK imports Elanco's tick sighting records into SQLite and exposes a versioned REST API for searching, filtering and reporting. The data is from Elanco's educational placement API. Counts describe reports in that dataset and should not be interpreted as tick prevalence or disease risk.

[Watch the project walkthrough](walkthrough.mp4)

## Run locally

Requires Python 3.12 and internet access for installing dependencies. Run from the repository root.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m tickwatch.ingest
python -m uvicorn tickwatch.api:app
```

On Windows PowerShell, use `py -3.12 -m venv .venv` and activate with `.\.venv\Scripts\Activate.ps1`. You can also run the remaining commands with `.\.venv\Scripts\python.exe` directly.

Open [interactive API documentation](http://127.0.0.1:8000/docs). The machine-readable schema is at [/openapi.json](http://127.0.0.1:8000/openapi.json). Swagger UI loads its assets from a CDN.

For an offline demo after installing dependencies, replace the import command with:

```bash
python -m tickwatch.ingest --file examples/sightings.json
```

The fixture uses fictional sightings and includes a duplicate, missing species metadata, a city outside the lookup and an invalid date. Imports replace the active snapshot. Set `TICKWATCH_DB` to a separate path if you want to keep a fixture and a live snapshot side by side. The default database is `var/tickwatch.db`. Use the same value for the importer and the server.

## API

| Endpoint | Purpose |
| --- | --- |
| `GET /health` | Local snapshot readiness. Returns 503 until a usable snapshot exists. |
| `GET /v1/dataset` | Source, last successful refresh, observation date range and data-quality counts. |
| `GET /v1/sightings` | Newest-first search. Supports `limit` (1–200), `offset` and `next_offset`. |
| `GET /v1/reports/regions` | Counts by assigned region, highest count first. |
| `GET /v1/reports/trends` | Counts by calendar month or Monday-start week. Use `interval=month` or `interval=week`. |

The search and both report endpoints accept `start`, `end`, `city` and `region`. Date filters use inclusive calendar dates (`YYYY-MM-DD`). Location filters match complete names, ignoring case and extra whitespace. Filters combine with AND. Trends return observed periods only. A missing period can be filled with a zero **reports** count by the client within a selected reporting window. If filters cut through a week or month, its count includes only the selected days.

```bash
curl 'http://127.0.0.1:8000/v1/sightings?city=Bristol&start=2024-01-01&end=2024-12-31&limit=2'
curl 'http://127.0.0.1:8000/v1/reports/regions'
curl 'http://127.0.0.1:8000/v1/reports/trends?interval=week&city=Bristol&start=2024-01-01&end=2024-03-31'
```

Use `curl.exe` for these commands on Windows if `curl` is a PowerShell alias.

## Architecture and data decisions

**Source → importer → SQLite snapshot → FastAPI.** The [Elanco documentation](https://dev-task.elancoapps.com/) describes a public read-only API. Its [all-sightings endpoint](https://dev-task.elancoapps.com/data/tick-sightings) returns a JSON array with no documented pagination or incremental cursor. The importer streams it in 64 KiB chunks and validates records one at a time. FastAPI provides request validation, typed response schemas and interactive documentation. SQLite keeps installation simple and performs filtering and aggregation with indexed SQL.

- **Identity.** The upstream ID is the primary key. The first valid version of an ID wins within a snapshot. Repeats are counted, and repeats with differing normalised fields also increment a conflict counter. Distinct IDs are retained even if they share a date, city and species because they may be separate reports.
- **Incomplete records.** A missing ID, missing city or invalid date makes a record unusable for this API. Such rows are counted by rejection reason. Missing species fields are returned as JSON `null`. Known cities use canonical names and a lookup into UK regions or devolved nations. Unmapped cities remain searchable and are counted under `Unknown`. Newcastle in the source is interpreted as Newcastle upon Tyne.
- **Dates.** The source omits offsets, so the API keeps the reported wall-clock time and groups by its reported calendar date. `refreshed_at` is a separate UTC timestamp for the import itself. A recent import can still contain historical observations.
- **Refresh failures.** The importer uses a 15-second HTTP operation timeout and up to three attempts for transient HTTP or network failures and unusable upstream payloads. It honours `Retry-After` up to a 60-second wait and aborts if the upstream requests a longer delay. Otherwise it backs off between attempts. Import failures exit with a nonzero status. Schedule one importer at a time if automating refreshes.
- **Snapshot consistency.** Rows and import metrics commit in one transaction only after the array has been parsed and contains usable data. SQLite's write-ahead log lets API readers continue querying the previous snapshot while the import runs. Each search response reads its count and page in a single transaction. If the snapshot changes between client requests, restart offset pagination to avoid page drift.
- **Scale.** Streaming avoids holding the full feed in memory. Date, city/date and region/date indexes support common filters. Aggregates execute in SQL, and paginated searches return at most 200 rows. A full refresh still costs one read of the source and one database rewrite. Very deep offsets and unfiltered aggregates will cost more as the dataset grows.

## Verification and next steps

```bash
python -m unittest discover -v
```

Tests use isolated SQLite databases and a mocked upstream for failure scenarios. GitHub Actions runs the suite on pushes and pull requests. The live source was also verified with 1,000 imported records across 14 cities, including 77 Bristol records, spanning 2012–2024.

For a larger deployment, introduce incremental ingestion if the source offers a watermark, use cursor-based pagination tied to a dataset version and monitor refresh failures and coverage changes. Before building an AI/ML extension, validate whether there are enough comparable reports and contextual variables. A useful first experiment would compare seasonal baselines with held-out periods to flag unusual **report volume**. Estimating disease risk would require appropriate surveillance and exposure data.
