"""Offline regression tests for the API-only promotion anomaly recheck."""
import hashlib
import io
import json
import sys
import tarfile
import tempfile
import unittest
from datetime import datetime, timezone, timedelta
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from promo_api_recheck import (
    read_capture_view, trigger_reason, compare_views, verify_suspicious_store
)
from raw_capture import documents_from_hf_raw

SID = "11111111-1111-4111-8111-111111111111"
GOOD = "22222222-2222-4222-8222-222222222222"
NOW = datetime(2026, 11, 16, 9, 30, tzinfo=timezone(timedelta(hours=8)))


def make_item(index, kind="NONE", price=13900):
    value = {"uuid": f"item-{index:04d}", "price": price,
             "title": f"Product {index}",
             "catalogItemAnalyticsData": {"promoType": kind}}
    if kind == "BOGO":
        value["itemPromotion"] = {
            "type": "buyXGetYItemPromotion",
            "buyXGetYItemPromotion": {"buyQuantity": 1, "getQuantity": 1}}
    return value


def write_capture(root, sid=SID, *, bogo=0, discounted=0, total=130):
    store_path = f"stores/{sid}-status-200.body"
    page_path = f"catalog/{sid}-page-000.body"
    items = [make_item(i, "BOGO" if i < bogo else
                       "DISCOUNTED_ITEM" if i < bogo + discounted else "NONE")
             for i in range(total)]
    base = {"data": {"title": "Verified store", "catalogSectionsMap": {}}}
    page = {"data": {"catalog": [
        {"payload": {"standardItemsPayload": {"catalogItems": items}}}
    ], "pagingInfo": {"hasMore": False}}}
    for name, value in ((store_path, base), (page_path, page)):
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value), encoding="utf8")
    return {"store_uuid": sid, "store_url": "https://example.org/store/test",
            "success": True, "inactive": False, "catalog_complete": True,
            "catalog_responses": [{"path": page_path}], "response_path": store_path,
            "captured_at": "2026-11-16T09:30:00+08:00"}


class SameRunAPIRecheck(unittest.TestCase):
    def test_stats_from_saved_real_shaped_api_bodies(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = write_capture(root, bogo=30, discounted=40)
            stats = read_capture_view(root, source)["stats"]
            self.assertEqual(stats["total_skus"], 130)
            self.assertEqual(stats["bogo"], 30)
            self.assertEqual(stats["direct_discount"], 40)

    def test_large_promotion_disappearance_triggers_a_recheck_only(self):
        reason = trigger_reason(
            {"total_skus": 130, "bogo": 40, "direct_discount": 30},
            {"total_skus": 130, "bogo": 0, "direct_discount": 0},
            "20261116063000", NOW)
        self.assertIn("drop", reason)
        self.assertIsNone(trigger_reason(
            {"total_skus": 130, "bogo": 40, "direct_discount": 30},
            {"total_skus": 130, "bogo": 40, "direct_discount": 30},
            "20261116063000", NOW))
        self.assertIsNone(trigger_reason(None, {
            "total_skus": 130, "bogo": 0, "direct_discount": 0}, now=NOW))

    def test_same_run_second_api_confirms_even_if_promo_remains_absent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root)
            calls = []
            def fetch_again(item, new_root):
                calls.append(item["store_uuid"])
                return write_capture(new_root)
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, original,
                {"total_skus": 130, "bogo": 40, "direct_discount": 30},
                "20261116063000", fetch_again=fetch_again, now=NOW)
            self.assertEqual(calls, [SID])
            self.assertEqual(result["promo_recheck"]["status"], "MATCHED")
            evidence = result["promo_recheck"]["second_response_path"]
            self.assertTrue((root / evidence).is_file())
            self.assertTrue(result["success"])

    def test_two_api_responses_disagree_is_not_silently_verified(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root)
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, original,
                {"total_skus": 130, "bogo": 40, "direct_discount": 30},
                "20261116063000",
                fetch_again=lambda _item, folder: write_capture(folder, bogo=40),
                now=NOW)
            self.assertEqual(result["promo_recheck"]["status"], "CONFLICT")
            self.assertEqual(result["promo_recheck"]["comparison"]["price_or_promo_mismatches"], 40)
            self.assertTrue(result["success"])

    def test_retry_failure_is_unavailable_not_proven_wrong(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root)
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, original,
                {"total_skus": 130, "bogo": 40, "direct_discount": 30},
                "20261116063000",
                fetch_again=lambda _item, folder: {
                    "store_uuid": SID, "success": False, "error": "HTTP 503"},
                now=NOW)
            self.assertEqual(result["promo_recheck"]["status"], "UNAVAILABLE")
            self.assertIn("HTTP 503", result["promo_recheck"]["error"])
            self.assertTrue(result["success"])
            self.assertEqual(result["promotion_stats"]["total_skus"], 130)

    def test_incomplete_second_catalog_is_not_counted_as_match(self):
        first = {"items": {f"item-{i}": (100, "NONE", None, None)
                           for i in range(120)}}
        second = {"items": {f"item-{i}": (100, "NONE", None, None)
                            for i in range(50)}}
        result = compare_views(first, second)
        self.assertEqual(result["status"], "CONFLICT")
        self.assertLess(result["common_coverage_pct"], 90)


class RawReplayQuarantinesInconclusiveAPI(unittest.TestCase):
    def test_inconclusive_store_preserves_previous_prices_but_not_raw_evidence(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            suspect = write_capture(root, SID)
            valid = write_capture(root, GOOD, bogo=20)
            retry = write_capture(root / "promo-recheck" / SID, SID)
            suspect["promo_recheck"] = {
                "status": "CONFLICT",
                "second_response_path": "promo-recheck/" + SID + "/" + retry["response_path"]}
            bodies = {}
            for path in root.rglob("*.body"):
                bodies[path.relative_to(root).as_posix()] = path.read_bytes()
            buff = io.BytesIO()
            with tarfile.open(fileobj=buff, mode="w:gz") as tar:
                for name, raw in bodies.items():
                    info = tarfile.TarInfo(name)
                    info.size = len(raw)
                    tar.addfile(info, io.BytesIO(raw))
            manifest = {
                "batch_id": "20261116093000", "complete": True,
                "stores": [suspect, valid],
                "responses": {name: hashlib.sha256(raw).hexdigest()
                              for name, raw in bodies.items()},
            }
            excluded = []
            with tarfile.open(fileobj=io.BytesIO(buff.getvalue()), mode="r:gz") as tar:
                with patch("menu_core.convert_api_data_to_schema",
                           return_value={"name": "Verified"}), \
                     patch("snapshot_validation.validate_document"):
                    docs = documents_from_hf_raw(
                        tar, manifest, quarantined_store_uuids=excluded)
                self.assertIn(suspect["promo_recheck"]["second_response_path"],
                              manifest["responses"])
            self.assertEqual(excluded, [SID])
            self.assertEqual([d["store_uuid"] for d in docs], [GOOD])


if __name__ == "__main__":
    unittest.main()
