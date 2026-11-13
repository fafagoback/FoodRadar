"""Regression tests for fail-closed single-store HF Raw catalog quarantine.

No HTTP, HF, GitHub Actions or Turso writes are made by these tests.
"""
import copy
import hashlib
import io
import json
import sys
import tarfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from catalog_complete import EmptyCatalogPagesError, merge_catalog_pages
from pipeline import validate_raw_replay_coverage
from raw_capture import documents_from_hf_raw


GOOD = "11111111-1111-4111-8111-111111111111"
EMPTY = "22222222-2222-4222-8222-222222222222"


def raw_fixture(empty_page=None):
    """Two originally successful shops: one item-bearing, one empty V2 catalog."""
    def page(items):
        return {"data": {
            "catalog": [{"payload": {"standardItemsPayload": {
                "catalogItems": items}}}] if items else None,
            "pagingInfo": {"hasMore": False},
        }}

    payloads = {
        "raw/good.body": {"data": {
            "name": "Good", "catalogSectionsMap": {},
        }},
        "raw/good-page.body": page([{"uuid": "sku-1", "title": "Coffee", "price": 9900}]),
        "raw/empty.body": {"data": {
            "name": "Empty", "catalogSectionsMap": {},
        }},
        "raw/empty-page.body": empty_page if empty_page is not None else page([]),
    }
    contents = {name: json.dumps(data).encode()
                for name, data in payloads.items()}
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:gz") as archive:
        for name, data in contents.items():
            member = tarfile.TarInfo(name)
            member.size = len(data)
            archive.addfile(member, io.BytesIO(data))
    raw = output.getvalue()

    def store(uid, name):
        return {
            "store_uuid": uid,
            "store_url": "https://example.org/store/" + name,
            "response_path": "raw/" + name + ".body",
            "catalog_responses": [{"path": "raw/" + name + "-page.body"}],
            "catalog_complete": True,
            "success": True,
            "inactive": False,
            "captured_at": "2026-11-12T15:00:21+08:00",
        }
    manifest = {
        "batch_id": "20261112150021",
        "complete": True,
        "stores": [store(GOOD, "good"), store(EMPTY, "empty")],
        "responses": {name: hashlib.sha256(data).hexdigest()
                      for name, data in contents.items()},
    }
    return raw, manifest


class HFEmptyMenuQuarantine(unittest.TestCase):
    def decode(self, content, manifest, excluded=None):
        with tarfile.open(fileobj=io.BytesIO(content), mode="r:gz") as archive:
            with patch("menu_core.convert_api_data_to_schema",
                       return_value={"name": "verified"}), \\
                 patch("snapshot_validation.validate_document"):
                if excluded is None:
                    return documents_from_hf_raw(archive, manifest)
                return documents_from_hf_raw(
                    archive, manifest, quarantined_store_uuids=excluded)

    def test_typed_empty_menu_error_does_not_modify_original_native(self):
        base = {"catalogSectionsMap": {"original": []}}
        page = {"data": {"catalog": None, "pagingInfo": {"hasMore": False}}}
        with self.assertRaises(EmptyCatalogPagesError):
            merge_catalog_pages(base, [page])
        self.assertEqual(base, {"catalogSectionsMap": {"original": []}})

    def test_one_invalid_store_keeps_other_verified_store(self):
        raw, manifest = raw_fixture()
        excluded = []
        docs = self.decode(raw, manifest, excluded)
        self.assertEqual([row["store_uuid"] for row in docs], [GOOD])
        self.assertEqual(excluded, [EMPTY])
        self.assertEqual(raw, raw_fixture()[0])  # Immutable HF bytes untouched.

    def test_quarantine_cannot_be_silently_discarded_by_old_callers(self):
        raw, manifest = raw_fixture()
        with self.assertRaisesRegex(ValueError, "explicit per-store quarantine"):
            self.decode(raw, manifest)

    def test_invalid_catalog_contract_still_aborts_replay(self):
        raw, manifest = raw_fixture({
            "data": {"catalog": "corrupt", "pagingInfo": {"hasMore": False}},
        })
        quarantined = []
        with self.assertRaisesRegex(ValueError, "unsupported type"):
            self.decode(raw, manifest, quarantined)
        self.assertEqual(quarantined, [])

    def test_modified_raw_body_still_aborts_before_quarantine(self):
        raw, manifest = raw_fixture()
        invalid = copy.deepcopy(manifest)
        invalid["responses"]["raw/empty-page.body"] = "0" * 64
        quarantined = []
        with self.assertRaisesRegex(ValueError, "checksum mismatch"):
            self.decode(raw, invalid, quarantined)
        self.assertEqual(quarantined, [])


class ReplayCoverageGuard(unittest.TestCase):
    def setUp(self):
        self.ids = [f"00000000-0000-4000-8000-{i:012x}" for i in range(1, 101)]
        self.records = [{"store_uuid": uid, "store_url": "https://example.org/"}
                        for uid in self.ids]

    def test_one_empty_menu_isolated_instead_of_blocking_healthy_stores(self):
        ids = self.ids
        got = validate_raw_replay_coverage(
            self.records, 100,
            [{"store_uuid": uid} for uid in ids[1:]], [ids[0]])
        self.assertEqual(got, {ids[0]})

    def test_five_percent_is_allowed_but_six_percent_aborts(self):
        ids = self.ids
        self.assertEqual(len(validate_raw_replay_coverage(
            self.records, 100,
            [{"store_uuid": uid} for uid in ids[5:]], ids[:5])), 5)
        with self.assertRaisesRegex(ValueError, "Excess empty-menu quarantine"):
            validate_raw_replay_coverage(
                self.records, 100,
                [{"store_uuid": uid} for uid in ids[6:]], ids[:6])

    def test_unknown_id_overlap_and_zero_verified_stores_fail_closed(self):
        ids = self.ids
        with self.assertRaisesRegex(ValueError, "UUID mismatch"):
            validate_raw_replay_coverage(
                self.records, 100,
                [{"store_uuid": uid} for uid in ids[1:]], ["foreign"])
        with self.assertRaisesRegex(ValueError, "UUID mismatch"):
            validate_raw_replay_coverage(
                self.records, 100, [{"store_uuid": uid} for uid in ids],
                [ids[0]])
        with self.assertRaisesRegex(ValueError, "UUID mismatch"):
            validate_raw_replay_coverage(
                self.records, 100, [], ids)


if __name__ == "__main__":
    unittest.main()
