"""The archive must be byte-identical to the HTTP response and ETL read-only."""
import hashlib
import io
import json
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from raw_capture import capture_response, documents_from_hf_raw, fetch_store_raw


class ImmutableRawTests(unittest.TestCase):
    def test_original_response_body_is_preserved_exactly(self):
        original = b'{ "data": { "price": 6000, "name": "\\u5e97" }, "extra": [1, 2]}\n'
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            response = SimpleNamespace(content=original)
            path = capture_response(root, "feed/page-0001-attempt-01-status-200.body", response)
            self.assertEqual(Path(path).read_bytes(), original)
            with self.assertRaises(FileExistsError):
                capture_response(root, "feed/page-0001-attempt-01-status-200.body", response)

    def test_db_conversion_does_not_mutate_archived_response(self):
        original = b'{ "status" : "success", "data": {"title":"Test", "catalogSectionsMap":{}, "price":6000}}\n'
        filename = "stores/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa-attempt-01-status-200.body"
        with tempfile.TemporaryDirectory() as temp:
            archive_path = Path(temp) / "raw.tar.gz"
            with tarfile.open(archive_path, "w:gz") as output:
                info = tarfile.TarInfo(filename)
                info.size = len(original)
                output.addfile(info, io.BytesIO(original))
            record = {
                "complete": True,
                "batch_id": "20261030160101",
                "responses": {filename: hashlib.sha256(original).hexdigest()},
                "stores": [{
                    "store_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
                    "store_url": "https://www.ubereats.com/tw/store/test/id",
                    "response_path": filename,
                    "captured_at": "2026-10-30T16:01:01+08:00",
                    "success": True,
                }]
            }
            fake_document = {"name":"converted"}
            with tarfile.open(archive_path, "r:gz") as src:
                with patch("menu_core.convert_api_data_to_schema", return_value=fake_document), \
                     patch("snapshot_validation.validate_document"):
                    products = documents_from_hf_raw(src, record)
                self.assertEqual(
                    src.extractfile(filename).read(), original)
            self.assertEqual(products[0]["name"], "converted")
            self.assertEqual(products[0]["store_uuid"], record["stores"][0]["store_uuid"])

    def test_bad_source_hash_aborts_db_conversion(self):
        original = b'{"data":{"title":"Example"}}'
        filename = "stores/raw.body"
        with tempfile.TemporaryDirectory() as temp:
            archive_path = Path(temp) / "raw.tar.gz"
            with tarfile.open(archive_path, "w:gz") as output:
                info = tarfile.TarInfo(filename)
                info.size = len(original)
                output.addfile(info, io.BytesIO(original))
            with tarfile.open(archive_path, "r:gz") as src:
                with self.assertRaises(ValueError):
                    documents_from_hf_raw(src, {
                        "complete":True, "responses":{filename:"bad"},
                        "stores":[], "batch_id":"20261030160101",
                    })


class StoreRetryScopeTests(unittest.TestCase):
    STORE = {"store_uuid": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
             "store_url": "https://www.ubereats.com/tw/store/test/aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"}

    @staticmethod
    def response(body, status=200):
        return SimpleNamespace(status_code=status,
                               content=json.dumps(body).encode("utf-8"),
                               json=lambda: body)

    def run_capture(self, replies, offsets=None):
        session = SimpleNamespace()
        calls = []

        def post(url, json, headers, timeout):
            calls.append((url.rsplit("/", 1)[-1],
                          (json.get("pagingInfo") or {}).get("offset")))
            reply = replies.pop(0)
            if isinstance(reply, Exception):
                raise reply
            return reply

        session.post = post
        session.close = lambda: None
        with tempfile.TemporaryDirectory() as temp:
            with patch("raw_capture.requests.Session", return_value=session), \
                 patch("raw_capture.attach_delivery_location", return_value=True), \
                 patch("raw_capture.global_limiter.wait"), \
                 patch("raw_capture.global_limiter.record_success"), \
                 patch("raw_capture.global_limiter.trigger_backoff") as backoff, \
                 patch("raw_capture.time.sleep"):
                result = fetch_store_raw(self.STORE, Path(temp))
                files = list(Path(temp).rglob("*.body"))
                if offsets is not None:
                    self.assertEqual([offset for api, offset in calls
                                      if api == "getCatalogPresentationV2"], offsets)
                return result, calls, len(files), backoff.call_count

    def base(self):
        return self.response({"status": "success", "data": {
            "title": "Test store",
            "aisles": {"store": [{"catalogSectionUUID": "parent"}]},
        }})

    def page(self, more=False, offset=0):
        return self.response({"status": "success", "data": {
            "catalog": [{"payload": {"standardItemsPayload": {
                "catalogItems": [{"uuid": "sku-1", "title": "Item", "price": 1000}]
            }}}],
            "pagingInfo": {"hasMore": more, "offset": offset},
        }})

    def test_structural_partition_failure_never_restarts_store(self):
        result, calls, count, _ = self.run_capture(
            [self.base(), self.page(True, 1020), self.base()], [0])
        self.assertFalse(result["success"])
        self.assertIn("native subsection UUIDs unavailable", result["error"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(count, 3)
        self.assertNotIn("catalog_complete", result)

    def test_missing_child_ids_refresh_metadata_without_recrawling_parent(self):
        renewed = self.response({"status": "success", "data": {
            "title": "Test store",
            "aisles": {"store": [{"catalogSectionUUID": "parent"}]},
            "sections": [{"uuid": "parent", "subsectionUuids": ["child"]}],
        }})
        result, calls, count, _ = self.run_capture(
            [self.base(), self.page(True, 1020), renewed, self.page()], [0, 0])
        self.assertTrue(result["success"])
        self.assertTrue(result["catalog_complete"])
        self.assertEqual(len(calls), 4)
        self.assertEqual(count, 4)
        self.assertEqual(result["catalog_section_reports"][0]["mode"],
                         "native_children_before_deep_offset")

    def test_metadata_refresh_forbidden_is_not_a_verified_partial(self):
        result, calls, count, _ = self.run_capture(
            [self.base(), self.page(True, 1020), self.response({}, 403)], [0])
        self.assertFalse(result["success"])
        self.assertIn("Native metadata refresh HTTP 403", result["error"])
        from catalog_complete import is_recoverable_catalog_failure
        self.assertFalse(is_recoverable_catalog_failure(result["error"]))
        self.assertEqual(len(calls), 3)
        self.assertEqual(count, 3)

    def test_transient_later_page_retries_only_that_page(self):
        from requests import Timeout
        result, calls, count, _ = self.run_capture(
            [self.base(), self.page(True, 60), Timeout("temporary"), self.page()],
            [0, 60, 60])
        self.assertTrue(result["success"])
        self.assertTrue(result["catalog_complete"])
        self.assertEqual(len(calls), 4)
        self.assertEqual(count, 3)

    def test_exhausted_later_page_never_restarts_store(self):
        from requests import Timeout
        result, calls, count, _ = self.run_capture(
            [self.base(), self.page(True, 60)] + [Timeout("persistent")] * 3,
            [0, 60, 60, 60])
        self.assertFalse(result["success"])
        self.assertIn("offset 60 failed", result["error"])
        self.assertEqual(len(calls), 5)
        self.assertEqual(count, 2)
        self.assertNotIn("catalog_complete", result)

    def test_metadata_timeout_can_retry_before_paging(self):
        from requests import Timeout
        result, calls, count, _ = self.run_capture(
            [Timeout("metadata"), self.base(), self.page()], [0])
        self.assertTrue(result["success"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(count, 2)

    def test_metadata_429_still_backs_off_and_retries(self):
        result, calls, count, backoffs = self.run_capture(
            [self.response({"error": "rate limited"}, 429), self.base(), self.page()],
            [0])
        self.assertTrue(result["success"])
        self.assertEqual(len(calls), 3)
        self.assertEqual(count, 3)
        self.assertEqual(backoffs, 1)


if __name__ == "__main__":
    unittest.main()
