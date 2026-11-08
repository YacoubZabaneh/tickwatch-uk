import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import httpx
from fastapi.testclient import TestClient

from tickwatch.api import app
from tickwatch.ingest import ImportFailure, import_file, import_live

FIXTURE = Path(__file__).resolve().parents[1] / "examples" / "sightings.json"


class BackendTests(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.database = Path(directory.name) / "test db.sqlite"
        env = patch.dict(os.environ, {"TICKWATCH_DB": str(self.database)})
        env.start()
        self.addCleanup(env.stop)
        self.client = TestClient(app)
        self.addCleanup(self.client.close)

    def test_no_dataset_returns_503_then_becomes_ready(self):
        self.assertEqual(self.client.get("/health").status_code, 503)
        self.assertEqual(self.client.get("/v1/sightings").status_code, 503)
        import_file(self.database, FIXTURE)
        self.assertEqual(self.client.get("/health").json(), {"status": "ready"})

    def test_duplicate_missing_fields_and_unknown_region(self):
        import_file(self.database, FIXTURE)
        import_file(self.database, FIXTURE)
        stats = self.client.get("/v1/dataset").json()
        self.assertEqual((stats["received"], stats["stored"], stats["duplicates"], stats["rejected"]), (7, 5, 1, 1))
        self.assertEqual(stats["incomplete_species"], 2)
        self.assertEqual(stats["unknown_region"], 1)
        self.assertEqual(stats["rejection_reasons"], {"invalid_date": 1})

    def test_search_combines_filters_and_includes_end_of_day(self):
        result = self.client.get("/v1/sightings")
        self.assertEqual(result.status_code, 503)
        import_file(self.database, FIXTURE)
        response = self.client.get("/v1/sightings", params={
            "city": " bRiStOl ", "region": "South   West",
            "start": "2024-06-03", "end": "2024-06-09", "limit": 1,
        }).json()
        self.assertEqual(response["total"], 2)
        self.assertEqual(response["next_offset"], 1)
        self.assertEqual(response["items"][0]["id"], "demo-2")
        self.assertIsNone(response["items"][0]["latin_name"])
        page = self.client.get("/v1/sightings?city=Bristol&limit=1&offset=1").json()
        self.assertEqual(page["items"][0]["id"], "demo-1")
        self.assertIsNone(page["next_offset"])

    def test_invalid_queries_and_safe_sql_parameters(self):
        import_file(self.database, FIXTURE)
        for query in ("start=2024-06-10&end=2024-06-03", "start=not-a-date", "limit=201", "offset=-1", "city=%20"):
            with self.subTest(query=query):
                self.assertEqual(self.client.get(f"/v1/sightings?{query}").status_code, 422)
        response = self.client.get("/v1/sightings", params={"city": "x' OR 1=1 --"})
        self.assertEqual(response.json()["total"], 0)

    def test_reports_use_same_filters_and_monday_buckets(self):
        import_file(self.database, FIXTURE)
        self.assertEqual(self.client.get("/v1/reports/regions?end=2024-06-30").json(), [
            {"region": "South West", "count": 2}, {"region": "Wales", "count": 1},
        ])
        self.assertEqual(self.client.get("/v1/reports/trends?interval=week&end=2024-06-30").json(), {
            "interval": "week",
            "items": [{"period_start": "2024-06-03", "count": 2}, {"period_start": "2024-06-10", "count": 1}],
        })
        self.assertEqual(self.client.get("/v1/reports/trends").json()["items"], [
            {"period_start": "2024-06-01", "count": 3}, {"period_start": "2024-07-01", "count": 2},
        ])

    def test_truncated_stream_rolls_back_changes(self):
        original = import_file(self.database, FIXTURE)
        malformed = self.database.with_name("truncated.json")
        valid = {"id": "replacement", "date": "2024-08-01T12:30:00", "location": "Bristol"}
        malformed.write_text("[" + json.dumps(valid) + ",", encoding="utf-8")
        with self.assertRaises(ImportFailure):
            import_file(self.database, malformed)
        self.assertEqual(self.client.get("/v1/dataset").json()["refreshed_at"], original["refreshed_at"])
        self.assertEqual(self.client.get("/v1/sightings").json()["total"], 5)

    def test_wrong_shape_empty_and_unusable_snapshots_are_rejected(self):
        import_file(self.database, FIXTURE)
        invalid = self.database.with_name("invalid.json")
        for body in ("{}", "[]", "[{}]"):
            with self.subTest(body=body):
                invalid.write_text(body, encoding="utf-8")
                with self.assertRaises(ImportFailure):
                    import_file(self.database, invalid)
        self.assertEqual(self.client.get("/v1/sightings").json()["total"], 5)

    def test_rate_limit_retry_then_success(self):
        calls = iter([httpx.Response(429, headers={"Retry-After": "2"}), httpx.Response(200, content=FIXTURE.read_bytes())])
        with patch("tickwatch.ingest.time.sleep") as sleep:
            summary = import_live(self.database, transport=httpx.MockTransport(lambda _: next(calls)))
        sleep.assert_called_once_with(2)
        self.assertEqual(summary["stored"], 5)

    def test_exhausted_upstream_retries_leave_snapshot_readable(self):
        import_file(self.database, FIXTURE)
        calls = []
        def unavailable(request):
            calls.append(request)
            return httpx.Response(503)
        with patch("tickwatch.ingest.time.sleep"):
            with self.assertRaisesRegex(ImportFailure, "HTTP 503"):
                import_live(self.database, transport=httpx.MockTransport(unavailable))
        self.assertEqual(len(calls), 3)
        self.assertEqual(self.client.get("/v1/sightings").json()["total"], 5)


if __name__ == "__main__":
    unittest.main()
