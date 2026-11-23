"""Regression tests: platform prices are already the payable discounted price."""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from json_to_db import calculate_effective_price, extract_promo_info


class EffectivePriceTests(unittest.TestCase):
    def test_discount_labels_do_not_apply_twice(self):
        for label in ("66折特惠", "6折特惠", "85折特惠", "限時特惠"):
            with self.subTest(label=label):
                self.assertEqual(calculate_effective_price(168.30, label, 1), 168.30)

    def test_two_digit_sale_label_is_not_six_discount(self):
        promo, qty = extract_promo_info("優惠方案", "66折黑醬烤雞堡超值餐", "")
        self.assertEqual((promo, qty), ("無", 1))
        self.assertEqual(calculate_effective_price(168.30, promo, qty), 168.30)

    def test_legacy_text_is_never_allowed_to_reprice_a_product(self):
        self.assertEqual(calculate_effective_price(200, "買1送1", 2), 200)
        self.assertEqual(calculate_effective_price(90, "買2送1", 3), 90)
        self.assertEqual(calculate_effective_price(100, "第2件半價", 1), 100)
        self.assertEqual(calculate_effective_price(100, "加1元多1件", 2), 100)
        self.assertEqual(calculate_effective_price(80, "買A送B", 2), 80)

    def test_invalid_prices_are_excluded_before_comparison(self):
        for price in (None, 'NaN', float('nan'), float('inf'), float('-inf'), 'not a price', 0, -3):
            with self.subTest(price=str(price)):
                self.assertEqual(calculate_effective_price(price, '買1送1', 2), 0.0)

    def test_numeric_conversion_rejects_non_finite_values(self):
        from serving_state import _num
        for value in ('NaN', float('inf'), float('-inf')):
            with self.subTest(value=str(value)):
                self.assertEqual(_num(value, float, 0), 0)
                self.assertIsNone(_num(value, float, None))

    def test_regular_product_is_unchanged(self):
        self.assertEqual(calculate_effective_price(168.30, "無", 1), 168.30)


if __name__ == "__main__":
    unittest.main()
