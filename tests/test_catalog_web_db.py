"""End-to-end projection test: immutable native items -> serving DB -> Turso web card."""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
sys.path.insert(0, str(ROOT / "scripts"))

from catalog_web_fields import web_catalog_fields
from menu_core import convert_api_data_to_schema
from serving_state import apply_snapshot
from build_web_db import build_web_db

SID = "d2e50ee1-69da-5eff-95f0-eb0dbaeb9e42"
PID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


def native_item(available=True):
    return {
        "uuid": PID, "title": "Häagen-Dazs 草莓冰淇淋 473ml【哈根達斯】",
        "price": 25900, "imageUrl": "https://example.org/icecream.png",
        "isAvailable": available, "isSoldOut": not available,
        "sectionUuid": "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        "subsectionUuid": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
        "catalogItemAnalyticsData": {"promoType": "MULTI_SKU_FLAT"},
        "itemThumbnailElements": [{"payload": {"labelPayload": {"label": {
            "richTextElements": [
                {"type": "text", "text": {"text": {"text": "NT$259"}, "predefinedDecorations": None}},
                {"type": "text", "text": {"text": {"text": "NT$309"}, "predefinedDecorations": ["STRIKE_THROUGH"]}}
            ]}}}}],
        "itemLevelPromotion": {"type": "multiSKUFlatPromotion",
            "multiSKUFlatPromotion": {
                "promotionUuid": "ffffffff-ffff-4fff-8fff-ffffffffffff",
                "itemPromotionTag": {"text": "Save NT$60 on NT$399+"}}},
        "promoInfo": {"promotionUUID": "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"},
        "quickAddConfig": {"private_unneeded": True}
    }


def document(available=True):
    item = native_item(available)
    native = {
        "title": "Uber Eats 優市 北投店", "isOpen": True,
        "workingHoursTagline": "Open 24 hours",
        "location": {"address": "台北市北投區示範路1號", "city": "台北市"},
        "catalogSectionsMap": {"catalog": [
            {"catalogSectionUUID": "11111111-1111-4111-8111-111111111111",
             "payload": {"standardItemsPayload": {
                "title": {"text": "飲料冰品"}, "catalogItems": [item]}}},
            {"catalogSectionUUID": "22222222-2222-4222-8222-222222222222",
             "payload": {"standardItemsPayload": {
                "title": {"text": "哈根達斯活動"}, "catalogItems": [item]}}}
        ]}
    }
    doc = convert_api_data_to_schema(native, "https://www.ubereats.com/tw/store/test/store")
    doc["store_uuid"] = SID
    doc["captured_at"] = "2026-11-09T10:10:00+08:00"
    return doc


class CatalogWebDatabaseTests(unittest.TestCase):
    def test_compact_projection_does_not_copy_original_payload(self):
        web = web_catalog_fields(native_item())
        self.assertEqual(web["display_original_price_cents"], 30900)
        self.assertEqual(web["image_url"], "https://example.org/icecream.png")
        self.assertEqual(web["cart_promo_label"], "Save NT$60 on NT$399+")
        self.assertEqual(web["native_promo_type"], "MULTI_SKU_FLAT")
        self.assertNotIn("quickAddConfig", web)
        self.assertNotIn("itemThumbnailElements", web)
        self.assertNotIn("price", web)

    def test_two_categories_one_sku_and_web_card(self):
        with tempfile.TemporaryDirectory() as d:
            state = Path(d) / "serving.db"
            packed = Path(d) / "packed.db"
            conn = sqlite3.connect(state)
            conn.row_factory = sqlite3.Row
            apply_snapshot(conn, [document()], "20261109101000")
            records = conn.execute("SELECT product_uuid,catalog_web,price_evidence_status FROM products").fetchall()
            self.assertEqual(len(records), 1)
            meta = json.loads(records[0]["catalog_web"])
            self.assertEqual(len(meta["section_uuids"]), 2)
            self.assertEqual(meta["display_original_price_cents"], 30900)
            self.assertEqual(records[0]["price_evidence_status"], "VERIFIED_MENU")
            conn.close()
            build_web_db(state, packed)
            db = sqlite3.connect(packed)
            row = db.execute("SELECT card,platform_promo FROM web_products WHERE store_uuid=? AND product_uuid=?", (SID, PID)).fetchone()
            self.assertIsNotNone(row)
            card = json.loads(row[0])
            self.assertEqual(row[1], 1)
            self.assertEqual(card["image_url"], "https://example.org/icecream.png")
            self.assertEqual(card["display_original_price"], 309)
            self.assertEqual(card["price"], 259)
            self.assertEqual(card["cart_promo_label"], "Save NT$60 on NT$399+")
            self.assertEqual(len(card["category_uuids"]), 2)
            db.close()

    def test_soldout_cannot_be_published_as_current_deal(self):
        with tempfile.TemporaryDirectory() as d:
            conn = sqlite3.connect(Path(d) / "serving.db")
            conn.row_factory = sqlite3.Row
            apply_snapshot(conn, [document(available=False)], "20261109101000")
            conn.close()
            build_web_db(Path(d) / "serving.db", Path(d) / "packed.db")
            db = sqlite3.connect(Path(d) / "packed.db")
            row = db.execute("SELECT card,platform_promo,promo FROM web_products").fetchone()
            self.assertIsNotNone(row)
            card = json.loads(row[0])
            self.assertTrue(card["is_sold_out"])
            self.assertFalse(card["is_available"])
            self.assertEqual(row[1:], (0, 0))
            db.close()


if __name__ == "__main__":
    unittest.main()
