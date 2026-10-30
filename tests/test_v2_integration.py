"""End-to-end proof: getStoreV1 numeric quotes -> SQLite -> 7-day deals."""
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from verified_prices import quote_from_native_item
from serving_state import apply_snapshot
from verified_weekly import update_verified_weekly

SID = "v2-store"
PID = "same-sku"
BATCH1 = "20261029063000"
BATCH2 = "20261030063000"


def native(price, category=None, buy=None, free=None, old=None):
    data = {"uuid": PID, "title": "實際商品", "price": price,
            "catalogItemAnalyticsData": {"promoType": category}}
    if buy is not None:
        data["itemPromotion"] = {"type": "buyXGetYItemPromotion",
                                  "buyXGetYItemPromotion": {
                                      "buyQuantity": buy, "getQuantity": free}}
    if old is not None:
        def label(value, crossed=False):
            return {"type": "text", "text": {
                "text": {"text": f"NT${value}"},
                "predefinedDecorations": ["STRIKE_THROUGH"] if crossed else None}}
        data["itemThumbnailElements"] = [{"payload": {"labelPayload": {
            "label": {"richTextElements": [label(price/100), label(old, True)]}}}}]
    return data


def document(item, status=None):
    quote = quote_from_native_item(item) if status is None else None
    payload = {"identifier": PID, "name": item["title"],
               "offers": {"price": str(item["price"] / 100), "priceCurrency": "TWD"}}
    if quote is not None:
        payload["verified_price"] = quote
    if status == "UNVERIFIED":
        payload["promo_type"] = "買1送1"  # Deliberate legacy text; never trusted.
    return {"@id": "https://www.ubereats.com/tw/store/test/v2-store",
            "@type": "Restaurant", "name": "測試餐廳", "store_uuid": SID,
            "menu_status": "present", "isOpen": True,
            "openingHoursSpecification": [{"dayOfWeek": [
                "Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"],
                "opens": "00:00", "closes": "00:00"}],
            "hasMenu": {"hasMenuSection": [{
                "name": "一般商品", "hasMenuItem": [payload]}]}}


class V2Integration(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.addCleanup(self.conn.close)

    def apply(self, source, batch, status=None):
        apply_snapshot(self.conn, [document(source, status)], batch)
        update_verified_weekly(self.conn, batch)
        return self.conn.execute("SELECT * FROM products WHERE product_uuid=?", (PID,)).fetchone()

    def test_native_bogo_price_history_and_discount_rate(self):
        self.apply(native(6000), BATCH1)
        row = self.apply(native(6000, "BOGO", 1, 1), BATCH2)
        self.assertEqual((row["price"], row["effective_price"], row["quantity"]),
                         (60, 30, 2))
        self.assertEqual((row["verified_discount_pct"], row["seven_day_discount_pct"]), (50, 50))
        self.assertEqual(row["seven_day_is_price_deal"], 1)
        kinds = [x[0] for x in self.conn.execute(
            "SELECT promotion_type FROM product_price_observations ORDER BY batch_id")]
        self.assertEqual(kinds, ["NONE", "SAME_SKU_BXGY"])

    def test_direct_discount_uses_marked_price_not_badge_percent(self):
        self.apply(native(7900), BATCH1)
        row = self.apply(native(7100, "DISCOUNTED_ITEM", old=79), BATCH2)
        self.assertEqual(row["verified_discount_pct"], 10.126582278481013)
        self.assertEqual(row["seven_day_discount_pct"], 10.13)
        self.assertEqual(row["effective_price"], 71)
        self.assertEqual(row["seven_day_is_price_deal"], 0)

    def test_boga_never_enters_discount_table(self):
        self.apply(native(6000), BATCH1)
        row = self.apply(native(6800, "BOGA", 1, 0), BATCH2)
        self.assertEqual(row["price_evidence_status"], "UNVERIFIED")
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM product_price_observations").fetchone()[0], 1)

    def test_legacy_text_cannot_create_fake_bogo(self):
        row = self.apply(native(6000), BATCH1, status="UNVERIFIED")
        self.assertEqual(row["price_evidence_status"], "UNVERIFIED")
        self.assertEqual(row["quantity"], 1)
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM product_price_observations").fetchone()[0], 0)


if __name__ == "__main__":
    unittest.main()
