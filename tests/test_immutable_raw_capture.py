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

from raw_capture import capture_response, documents_from_hf_raw


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


if __name__ == "__main__":
    unittest.main()
