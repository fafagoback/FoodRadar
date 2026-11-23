"""Real SQLite V2 projection tests for A-B-A suppression on the live data path."""
from datetime import datetime, timedelta, timezone
from fractions import Fraction
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from serving_state import apply_snapshot
from verified_weekly import update_verified_weekly
from price_oscillation import has_disputed_high_in_week
from test_v2_integration import native, document, PID

TW = timezone(timedelta(hours=8))


class OscillationTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.day = 0

    def add(self, value):
        batch = (datetime(2026, 11, 1, 6, 30, tzinfo=TW)
                 + timedelta(days=self.day)).strftime("%Y%m%d%H%M%S")
        self.day += 1
        apply_snapshot(self.db, [document(native(value * 100))], batch)
        update_verified_weekly(self.db, batch)
        return self.db.execute("SELECT * FROM products WHERE product_uuid=?", (PID,)).fetchone()

    def test_10_50_10_remains_suppressed_at_day_eight(self):
        for day, price in enumerate([10,50,10,10,10,10,10,10,10,10], 1):
            row = self.add(price)
            if day >= 3:
                self.assertEqual(row["seven_day_is_price_deal"], 0, day)
                self.assertEqual(row["seven_day_discount_pct"], 0, day)
            if day == 8:
                self.assertIsNone(row["seven_day_reference_price"])
        # Both 10 and 50 remain untouched in original verified observations.
        historical = list(self.db.execute(
            "SELECT payable_total_cents FROM product_price_observations ORDER BY observed_at"))
        self.assertEqual([x[0] for x in historical[:3]], [1000,5000,1000])

    def test_100_20_100_20_never_creates_second_false_drop(self):
        for day, price in enumerate([100,20,100,20], 1):
            row = self.add(price)
            if day == 2:
                self.assertEqual(row["seven_day_discount_pct"], 80)
                self.assertEqual(row["seven_day_is_price_deal"], 1)
            if day == 4:
                self.assertEqual(row["seven_day_is_price_deal"], 0)
                self.assertIsNone(row["seven_day_reference_price"])

    def test_sustained_real_discount_without_rebound_stays_visible(self):
        for price in [100,20,20,20,20,20,20]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 1)
        self.assertEqual(row["seven_day_discount_pct"], 80)

    def test_bounded_reversal_and_price_difference_independent(self):
        start = datetime(2026, 11, 1, 6, 30, tzinfo=TW)
        points = [(start + timedelta(days=i), Fraction(v)) for i,v in
                  enumerate([10,11,10])]
        self.assertTrue(has_disputed_high_in_week(points, points[-1][0]))
        distant = [(start, Fraction(10)),
                   (start+timedelta(days=1), Fraction(50)),
                   (start+timedelta(days=10), Fraction(10))]
        self.assertFalse(has_disputed_high_in_week(distant, distant[-1][0]))

    def test_abba_and_repeated_price_hopping(self):
        for price in [20,100,100,20,20,100,20]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertIsNone(row["seven_day_reference_price"])

    def test_suppression_expires_when_disputed_high_leaves_seven_days(self):
        values = [10,50,10] + [10] * 10
        for price in values:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertEqual(row["seven_day_discount_pct"], 0)
        # Without a high baseline inside the rolling window, comparison resumes.
        self.assertEqual(row["seven_day_reference_price"], 10)


if __name__ == "__main__":
    unittest.main()
