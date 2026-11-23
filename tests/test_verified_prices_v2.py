"""Native price proof and actual 2026-10-29 Uber getStoreV1 fixtures."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from verified_prices import quote_from_native_item, quote_from_converted_item, quote_numbers, ui_percent


def item(price, promo_type=None, promotion=None, thumbnail=None, **extra):
    result = {"uuid": "test-sku", "title": "測試商品", "price": price,
              "catalogItemAnalyticsData": {"promoType": promo_type}}
    if promotion is not None:
        result["itemPromotion"] = promotion
    if thumbnail is not None:
        result["itemThumbnailElements"] = [{"payload": {"labelPayload": {
            "label": {"richTextElements": thumbnail}}}}]
    result.update(extra)
    return result


def label(text, strike=False):
    return {"text": {"text": {"text": text},
            "predefinedDecorations": ["STRIKE_THROUGH"] if strike else None},
            "type": "text"}


class VerifiedPrices(unittest.TestCase):
    def test_real_costco_fractional_cent_native_prices(self):
        # Real 2026-11-02 Costco Beitou HF Raw values previously rejected as
        # INVALID_MENU_PRICE, hiding 15 validly priced products from the catalog.
        for raw_cents, expected in [
            (87455.70393999999, 87456),
            (26403.98209, 26404),
            (151897.212, 151897),
            (69423.5281, 69424),
            (26002.5, 26003),
        ]:
            with self.subTest(raw_cents=raw_cents):
                q = quote_from_native_item(item(raw_cents))
                self.assertEqual(q["status"], "VERIFIED_MENU")
                self.assertEqual(q["menu_price_cents"], expected)
                self.assertEqual(q["payable_total_cents"], expected)
                self.assertIsNotNone(quote_from_converted_item({"verified_price": q}))
        for invalid in [float("nan"), float("inf"), -0.5, 0, True, "87456"]:
            with self.subTest(invalid=invalid):
                q = quote_from_native_item(item(invalid))
                self.assertEqual(q["status"], "UNVERIFIED")

    def test_actual_bogo_coffee_60_2_for_1(self):
        q = quote_from_native_item(item(6000, "BOGO", {
            "type": "buyXGetYItemPromotion",
            "buyXGetYItemPromotion": {"buyQuantity": 1,
                                      "getQuantity": 1, "maxRedemptionCount": 100}}))
        self.assertEqual((q["original_total_cents"], q["payable_total_cents"],
                          q["received_quantity"]), (12000, 6000, 2))
        self.assertEqual(str(quote_numbers(q)["discount_rate"]), "50")
        self.assertEqual(str(quote_numbers(q)["effective_unit_price"]), "30")
        self.assertIsNotNone(quote_from_converted_item({"verified_price": q}))

    def test_actual_direct_markdown_79_to_71(self):
        q = quote_from_native_item(item(7100, "DISCOUNTED_ITEM",
            thumbnail=[label("NT$71 "), label("NT$79", True)]))
        self.assertEqual((q["original_total_cents"], q["payable_total_cents"]), (7900, 7100))
        self.assertEqual(str(ui_percent(quote_numbers(q)["discount_rate"])), "10.13")
        self.assertEqual(str(quote_numbers(q)["effective_unit_price"]), "71")

    def test_actual_boga_gift_bag_is_rejected(self):
        q = quote_from_native_item(item(6800, "BOGA", {
            "type": "buyXGetYItemPromotion",
            "buyXGetYItemPromotion": {"buyQuantity": 1, "getQuantity": 0}}))
        self.assertEqual(q["status"], "UNVERIFIED")
        self.assertIsNone(quote_from_converted_item({"verified_price": q}))

    def test_missing_bogo_numbers_never_default_to_one(self):
        q = quote_from_native_item(item(7000, "BOGO", {
            "type": "buyXGetYItemPromotion",
            "buyXGetYItemPromotion": {"buyQuantity": 1}}))
        self.assertEqual(q["status"], "UNVERIFIED")

    def test_unsupported_and_ambiguous_discounts_never_guess(self):
        self.assertEqual(quote_from_native_item(item(10000, "FREE_ITEM"))["status"], "UNVERIFIED")
        self.assertEqual(quote_from_native_item(item(10000, "MULTI_SKU_BXFY"))["status"], "UNVERIFIED")
        self.assertEqual(quote_from_native_item(item(10000, "DISCOUNTED_ITEM",
            thumbnail=[label("NT$100")]))["status"], "UNVERIFIED")
        self.assertEqual(quote_from_native_item(item(9900, "DISCOUNTED_ITEM",
            thumbnail=[label("NT$99"), label("NT$100", True)],
            itemLevelPromotion={"multiSKUFlatPromotion": {}}))["status"], "UNVERIFIED")

    def test_uncertified_historical_json_rejected(self):
        self.assertIsNone(quote_from_converted_item({
            "price": 60, "promo_type": "買1送1", "quantity": 2}))

    def test_plain_regular_price(self):
        q = quote_from_native_item(item(6500))
        self.assertEqual((q["kind"], q["original_total_cents"],
                          q["payable_total_cents"]), ("NONE", 6500, 6500))
        self.assertEqual(str(quote_numbers(q)["discount_rate"]), "0")
        self.assertEqual(quote_from_native_item(item(
            6500, title="買一送一活動"))["status"], "UNVERIFIED")


if __name__ == "__main__":
    unittest.main()
