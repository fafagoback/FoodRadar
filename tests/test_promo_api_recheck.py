"""Offline regression tests for the API-only promotion anomaly recheck."""
import hashlib
import io
import json
import os
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
    read_capture_view, trigger_reason, compare_views, verify_suspicious_store, _signature
)
from raw_capture import documents_from_hf_raw
from distributed_crawl import latest_raw_promotion_baseline

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

    def test_previous_inconclusive_api_forces_new_recheck(self):
        self.assertEqual(trigger_reason(
            {"_recheck_required": True, "total_skus": 130,
             "bogo": 40, "direct_discount": 20},
            {"total_skus": 130, "bogo": 40, "direct_discount": 20},
            "20261113063000", NOW),
            "previous_api_recheck_inconclusive")

    def test_hf_baseline_preserves_individual_sku_offer(self):
        with tempfile.TemporaryDirectory() as temp:
            latest = Path(temp) / "latest.json"
            latest.write_text(json.dumps({
                "format": "foodradar-immutable-http-body-v1",
                "batch_id": "20261116073000",
                "stores": [{
                    "store_uuid": SID, "store_url": "https://example.org/store/test",
                    "success": True,
                    "promotion_stats": {
                        "total_skus": 130, "bogo": 1, "direct_discount": 0},
                    "promotion_watch": {"item-0000": [13900, 2]},
                }],
            }), encoding="utf8")
            with (
                patch.dict(os.environ, {"HF_TOKEN": "offline-fixture"}),
                patch("huggingface_hub.hf_hub_download", return_value=str(latest)),
            ):
                baseline = latest_raw_promotion_baseline({
                    "hf_repo_id": "hub-google/FoodRadar", "hf_folder": "Raw"})
            self.assertEqual(baseline[SID]["promotion_watch"]["item-0000"],
                             [13900, 2])

    def test_inconclusive_recheck_keeps_last_trusted_sku_watch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = write_capture(root, total=130)
            old_watch = {"item-0000": [13900, 2]}
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, first,
                {"total_skus": 130, "bogo": 1, "direct_discount": 0},
                "20261116063000", now=NOW,
                fetch_again=lambda _item, _root: {
                    "store_uuid": SID, "success": False, "error": "HTTP 429"},
                previous_watch=old_watch)
            self.assertEqual(result["promo_recheck"]["status"], "UNAVAILABLE")
            self.assertEqual(result["promo_recheck"]["previous_watch"], old_watch)
            self.assertNotIn("item-0000", result["promotion_watch"])

    def test_after_retry_failure_next_batch_uses_original_trusted_sku_witness(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root, total=130, bogo=0)
            old_watch = {"item-0000": [13900, 2]}
            checked = verify_suspicious_store(
                {"store_uuid": SID}, root, original,
                {"total_skus": 130, "bogo": 1, "direct_discount": 0},
                "20261116063000", now=NOW, previous_watch=old_watch,
                fetch_again=lambda _store, _folder: {
                    "store_uuid": SID, "success": False, "error": "HTTP 503"})
            manifest = root / "latest.json"
            manifest.write_text(json.dumps({
                "format": "foodradar-immutable-http-body-v1",
                "batch_id": "20261116073000",
                "stores": [checked],
            }), encoding="utf8")
            with (
                patch.dict(os.environ, {"HF_TOKEN": "offline-fixture"}),
                patch("huggingface_hub.hf_hub_download",
                      return_value=str(manifest)),
            ):
                baseline = latest_raw_promotion_baseline({
                    "hf_repo_id": "hub-google/FoodRadar", "hf_folder": "Raw"})
            self.assertTrue(baseline[SID]["stats"]["_recheck_required"])
            self.assertEqual(baseline[SID]["promotion_watch"], old_watch)
            self.assertEqual(baseline[SID]["batch"], "20261116063000")

    def test_unconfirmed_promotions_do_not_replace_hf_baseline(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            latest = root / "latest.json"
            latest.write_text(json.dumps({
                "format": "foodradar-immutable-http-body-v1",
                "batch_id": "20261116073000",
                "stores": [{
                    "store_uuid": SID, "store_url": "https://example.org/store/test",
                    "success": True,
                    "promotion_stats": {
                        "total_skus": 130, "bogo": 0, "direct_discount": 0},
                    "promo_recheck": {
                        "status": "UNAVAILABLE",
                        "previous_stats": {
                            "total_skus": 130, "bogo": 40, "direct_discount": 30},
                        "previous_batch": "20261116063000",
                    },
                }],
            }), encoding="utf8")
            with (
                patch.dict(os.environ, {"HF_TOKEN": "offline-fixture"}),
                patch("huggingface_hub.hf_hub_download",
                      return_value=str(latest)),
            ):
                baseline = latest_raw_promotion_baseline({
                    "hf_repo_id": "hub-google/FoodRadar", "hf_folder": "Raw"})
            saved = baseline[SID]["stats"]
            self.assertEqual(saved["bogo"], 40)
            self.assertTrue(saved["_recheck_required"])
            self.assertNotEqual(saved["bogo"], 0)

    def test_one_promoted_sku_disappears_without_store_wide_alarm(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root, total=130, bogo=0)
            old = {"total_skus": 130, "bogo": 1, "direct_discount": 0}
            old_watch = {"item-0000": [13900, 2]}
            # A single BOGO ends, while 129 other items remain unchanged.
            # The old store-level threshold (>=20) would miss this.
            first = read_capture_view(root, original)
            self.assertEqual(first["stats"]["bogo"], 0)
            self.assertEqual(
                trigger_reason(old, first["stats"], "20261116063000", NOW,
                               previous_watch=old_watch,
                               current_items=first["items"]),
                "previously_promoted_sku_price_increase")
            calls = []
            def fetch_again(item, retry_root):
                calls.append(item)
                return write_capture(retry_root, total=130)
            checked = verify_suspicious_store(
                {"store_uuid": SID}, root, original, old,
                "20261116063000", fetch_again=fetch_again, now=NOW,
                previous_watch=old_watch)
            self.assertEqual(len(calls), 1)
            self.assertEqual(checked["promo_recheck"]["status"], "MATCHED")
            self.assertEqual(checked["promo_recheck"]["trigger"],
                             "previously_promoted_sku_price_increase")

    def test_one_promoted_sku_with_same_effective_price_needs_no_retry(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = write_capture(root, total=130, bogo=0)
            current = read_capture_view(root, source)
            self.assertIsNone(trigger_reason(
                {"total_skus": 130, "bogo": 1, "direct_discount": 0},
                current["stats"], "20261116063000", NOW,
                previous_watch={"item-0000": [13900, 1]},
                current_items=current["items"]))

    def test_verified_bogo_saved_as_compact_historical_watch(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = write_capture(root, total=130, bogo=1)
            result = read_capture_view(root, source)
            self.assertEqual(result["promotion_watch"]["item-0000"],
                             [13900, 2])
            self.assertEqual(len(result["promotion_watch"]), 1)

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
            self.assertEqual(len(result["promo_recheck"]["second_catalog_response_paths"]), 1)
            self.assertTrue((root / result["promo_recheck"]["second_catalog_response_paths"][0]).is_file())
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

    def test_changed_strikethrough_reference_price_cannot_match(self):
        def label(amount, strike):
            return {"payload": {"labelPayload": {"label": {
                "richTextElements": [{"text": {
                    "text": {"text": f"NT${amount}"},
                    "predefinedDecorations": ["STRIKE_THROUGH"] if strike else [],
                }}],
            }}}}
        a = make_item(1, "DISCOUNTED_ITEM", 13900)
        a["itemThumbnailElements"] = [label(199, True), label(139, False)]
        b = make_item(1, "DISCOUNTED_ITEM", 13900)
        b["itemThumbnailElements"] = [label(299, True), label(139, False)]
        first = {"items": {"item-0001": _signature(a)[1]}}
        second = {"items": {"item-0001": _signature(b)[1]}}
        self.assertNotEqual(first["items"], second["items"])
        self.assertEqual(compare_views(first, second)["status"], "CONFLICT")

    def test_one_missing_sku_cannot_pass_as_verified(self):
        first = {"items": {f"item-{i}": (100, "NONE", None, None)
                           for i in range(120)}}
        second = {"items": {f"item-{i}": (100, "NONE", None, None)
                            for i in range(119)}}
        result = compare_views(first, second)
        self.assertEqual(result["status"], "UNAVAILABLE")
        self.assertEqual(result["missing_in_second"], 1)
        self.assertGreater(result["common_coverage_pct"], 99)

    def test_unreadable_primary_api_is_not_silently_published(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            source = write_capture(root)
            (root / source["response_path"]).write_text(
                '{"data": null}', encoding="utf8")
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, source, None, now=NOW)
            self.assertEqual(result["promo_recheck"]["status"], "UNAVAILABLE")
            self.assertEqual(result["promo_recheck"]["trigger"],
                             "initial_native_parse_unavailable")

    def test_recheck_different_store_id_cannot_be_matched(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            original = write_capture(root)
            result = verify_suspicious_store(
                {"store_uuid": SID}, root, original,
                {"total_skus": 130, "bogo": 40, "direct_discount": 30},
                "20261116063000",
                fetch_again=lambda _item, folder: write_capture(folder, sid=GOOD),
                now=NOW)
            self.assertEqual(result["promo_recheck"]["status"], "UNAVAILABLE")
            self.assertIn("different store UUID", result["promo_recheck"]["error"])

    def test_incomplete_second_catalog_is_not_counted_as_match(self):
        first = {"items": {f"item-{i}": (100, "NONE", None, None)
                           for i in range(120)}}
        second = {"items": {f"item-{i}": (100, "NONE", None, None)
                            for i in range(50)}}
        result = compare_views(first, second)
        self.assertEqual(result["status"], "UNAVAILABLE")
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
