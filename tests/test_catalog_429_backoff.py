"""Regression tests for Uber Eats HTTP 200 with application-level 429."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from catalog_complete import capture_catalog


class Response:
    def __init__(self, code, payload):
        self.status_code = code
        self.payload = payload
        self.content = json.dumps(payload).encode("utf-8")

    def json(self):
        return self.payload


def ok_catalog(items=None):
    return Response(200, {
        "status": "success",
        "data": {
            "catalog": [{"payload": {"standardItemsPayload": {
                "catalogItems": items or [{"uuid": "sku-1", "title": "Item", "price": 1000}]
            }}}],
            "pagingInfo": {"hasMore": False},
        },
    })


def application_throttle():
    return Response(200, {"status": "failure",
                          "data": {"code": 429, "message": "status code error",
                                   "meta": {"error": "too many requests"}}})


def save(root, path, response):
    p = root / path
    p.parent.mkdir(parents=True, exist_ok=True)
    if p.exists():
        raise FileExistsError(path)
    p.write_bytes(response.content)


class Catalog429BackoffTests(unittest.TestCase):
    BASE = {"aisles": {"store": [{"catalogSectionUUID": "section-1"}]}}

    def test_http200_application_429_is_cooldown_then_success(self):
        class Session:
            calls = 0
            def post(self, url, json, headers, timeout):
                self.calls += 1
                return application_throttle() if self.calls == 1 else ok_catalog()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session = Session()
            with patch("catalog_complete.global_limiter.wait"), \\
                 patch("catalog_complete.global_limiter.trigger_backoff") as backoff:
                out = capture_catalog(session, "store", self.BASE, root, {}, save)
            self.assertTrue(out["catalog_complete"])
            self.assertEqual(out["catalog_item_occurrences"], 1)
            self.assertEqual(session.calls, 2)
            self.assertEqual(len(list(root.rglob("*.body"))), 2)
            self.assertEqual(backoff.call_count, 1)
            self.assertIn("application 429", backoff.call_args.args[1])

    def test_persistent_application_429_still_fails_closed(self):
        class Session:
            calls = 0
            def post(self, url, json, headers, timeout):
                self.calls += 1
                return application_throttle()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session = Session()
            with patch("catalog_complete.global_limiter.wait"), \\
                 patch("catalog_complete.global_limiter.trigger_backoff") as backoff:
                with self.assertRaisesRegex(RuntimeError, "application 429"):
                    capture_catalog(session, "store", self.BASE, root, {}, save)
            self.assertEqual(session.calls, 6)
            self.assertEqual(backoff.call_count, 6)
            self.assertEqual(len(list(root.rglob("*.body"))), 6)

    def test_http429_is_cooldown_then_success(self):
        class Session:
            calls = 0
            def post(self, url, json, headers, timeout):
                self.calls += 1
                return Response(429, {"error": "too many requests"}) if self.calls == 1 else ok_catalog()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session = Session()
            with patch("catalog_complete.global_limiter.wait"), \\
                 patch("catalog_complete.global_limiter.trigger_backoff") as backoff:
                out = capture_catalog(session, "store", self.BASE, root, {}, save)
            self.assertTrue(out["catalog_complete"])
            self.assertEqual(session.calls, 2)
            self.assertIn("HTTP 429", backoff.call_args.args[1])

    def test_throttled_alternate_category_does_not_mark_empty_complete(self):
        class Session:
            calls = 0
            def post(self, url, json, headers, timeout):
                self.calls += 1
                if self.calls == 1:
                    return Response(200, {"status": "success",
                                          "data": {"catalog": [],
                                                   "pagingInfo": {"hasMore": False}}})
                if self.calls == 2:
                    return application_throttle()
                return ok_catalog()

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            session = Session()
            with patch("catalog_complete.global_limiter.wait"), \\
                 patch("catalog_complete.global_limiter.trigger_backoff") as backoff:
                out = capture_catalog(session, "store", self.BASE, root, {}, save)
            self.assertTrue(out["catalog_complete"])
            self.assertEqual(out["catalog_item_occurrences"], 1)
            self.assertEqual(session.calls, 3)
            self.assertEqual(len(list(root.rglob("*.body"))), 3)
            self.assertEqual(backoff.call_count, 1)


if __name__ == "__main__":
    unittest.main()
