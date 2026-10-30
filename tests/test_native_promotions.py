"""Native promotions, unknown categories, original response archival."""
import sys
import unittest
import json
import tarfile
import tempfile
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from promotion_evidence import native_item_promo, native_promotion_field_names
from menu_core import convert_api_data_to_schema
from json_to_db import extract_promo_info


class NativePromotionsTests(unittest.TestCase):
    def test_bogo_from_structured_item_metadata(self):
        self.assertEqual(native_item_promo({"promotionInfo": {"promotionType": "BOGO"}}), ("買1送1", 2))
        self.assertEqual(native_item_promo({"promotions": [{"type": "BUY_2_GET_1_FREE"}]}), ("買2送1", 3))
        self.assertEqual(native_item_promo({"promotion": {
            "type": "BUY_GET_FREE", "buyQuantity": 2, "freeQuantity": 1}}), ("買2送1", 3))

    def test_unknown_type_does_not_invent_price_discount(self):
        self.assertIsNone(native_item_promo({"promotionInfo": {"promotionType": "NEW_BONUS_TYPE"}}))
        self.assertIsNone(native_item_promo({"title": "Offers", "price": 100}))
        self.assertIsNone(native_item_promo({"offerInfo": {"text": "Member specials"}}))
        self.assertIn("newPromotionInfo", native_promotion_field_names({"newPromotionInfo": {"mystery": 1}}))
        self.assertEqual(extract_promo_info("Offers", "Coffee", "")[0], "無")

    def test_converter_preserves_native_explicit_promotion(self):
        item = {"uuid": "product-1", "title": "test", "price": 5800,
                "catalogItemAnalyticsData": {"promoType": "BOGO"},
                "itemPromotion": {"type": "buyXGetYItemPromotion",
                    "buyXGetYItemPromotion": {"buyQuantity": 1, "getQuantity": 1}}}
        api = {"title": "Shop", "isOpen": True, "catalogSectionsMap": {
            "MAIN": [{"payload": {"standardItemsPayload": {
                "title": {"text": "一般商品"}, "catalogItems": [item]}}}]
        }}
        doc = convert_api_data_to_schema(api, "https://www.ubereats.com/tw/store/test/abc")
        converted = doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]
        self.assertEqual(converted["offers"]["price"], "58.00")
        self.assertEqual(converted["verified_price"]["kind"], "SAME_SKU_BXGY")
        self.assertEqual(converted["verified_price"]["received_quantity"], 2)
        self.assertEqual(converted["verified_price"]["payable_total_cents"], 5800)

    def test_raw_archival_and_category_badge_removed(self):
        crawler = (ROOT / "src/menu_core.py").read_text(encoding="utf-8")
        archive = (ROOT / "src/package_and_upload_menu_snapshot.py").read_text(encoding="utf-8")
        page = (ROOT / "web/app.js").read_text(encoding="utf-8")
        self.assertIn("json.dump(res_json, raw_file", crawler)
        self.assertIn("ApiRaw/", archive)
        self.assertNotIn("escapeHtml(item.category_name || '餐飲主食')", page)
        self.assertNotIn("escapeHtml(prod.category_name || '新上架')", page)
        self.assertNotIn("escapeHtml(p.category_name || '一般')", page)


    def test_real_archive_keeps_api_response_separate_from_schema(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            menu = root / "menus"
            native = root / "api_raw"
            output = root / "archive"
            menu.mkdir()
            native.mkdir()
            batch = "20261029143000"
            url = "https://www.ubereats.com/tw/store/sample/abc"
            doc = {
                "@type": "Restaurant", "@id": url, "name": "sample",
                "menu_status": "present",
                "hasMenu": {"hasMenuSection": [{
                    "@type": "MenuSection", "name": "Buy 1, get 1 free",
                    "hasMenuItem": [{
                        "@type": "MenuItem", "name": "coffee",
                        "offers": {"@type": "Offer", "price": "58.00"}}]}]}}
            raw = {"status": "success", "data": {
                "title": "sample", "catalogSectionsMap": {
                    "DATA": [{"payload": {"standardItemsPayload": {
                        "title": {"text": "Buy 1, get 1 free"},
                        "catalogItems": [{"title": "coffee", "price": 5800}]}}}]}}}
            filename = batch + "_sample.json"
            (menu / filename).write_text(json.dumps(doc), encoding="utf-8")
            (native / filename).write_text(json.dumps(raw), encoding="utf-8")
            (root / "stores.json").write_text(
                json.dumps([{"store_url": url}]), encoding="utf-8")
            subprocess.run([
                sys.executable, str(ROOT / "src/package_and_upload_menu_snapshot.py"),
                "--src-dir", str(menu), "--api-raw-dir", str(native),
                "--stores-file", str(root / "stores.json"),
                "--output-dir", str(output), "--batch-id", batch,
                "--expected-workers", "1", "--actual-workers", "1",
                "--minimum-stores", "1", "--repo-id", "test/repo", "--offline"],
                check=True, capture_output=True, text=True)
            with tarfile.open(output / ("taiwan_menus_" + batch + ".tar.gz"), "r:gz") as archive:
                names = archive.getnames()
                schema_names = [n for n in names if n.startswith("Json/")]
                native_names = [n for n in names if n.startswith("ApiRaw/")]
                self.assertEqual(len(schema_names), 1)
                self.assertEqual(len(native_names), 1)
                self.assertEqual(json.load(archive.extractfile(native_names[0])), raw)
                self.assertEqual(json.load(archive.extractfile(schema_names[0])), doc)


if __name__ == "__main__":
    unittest.main()
