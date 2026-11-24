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
        apply_snapshot(self.db, [document(native(int(value * 100)))], batch)
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
        self.assertTrue(has_disputed_high_in_week(distant, distant[-1][0]))
        outside = [(start, Fraction(10)),
                   (start+timedelta(days=7), Fraction(50)),
                   (start+timedelta(days=15), Fraction(10))]
        self.assertFalse(has_disputed_high_in_week(outside, outside[-1][0]))

    def test_fourteen_day_window_and_weekly_carryover(self):
        start = datetime(2026, 11, 1, 6, 30, tzinfo=TW)
        def blocked(points, day):
            return has_disputed_high_in_week(
                [(start+timedelta(days=d), Fraction(p)) for d,p in points],
                start+timedelta(days=day))
        self.assertTrue(blocked([(0,89),(7,179),(14,89)],14))
        self.assertTrue(blocked([(0,89),(11,179),(14,89),(17,89)],17))
        self.assertFalse(blocked([(0,89),(11,179),(14,89),(19,89)],19))
        self.assertTrue(blocked([(0,459),(4,899),(7,449)],7))

    def test_near_aba_exact_percentage_boundaries(self):
        start = datetime(2026, 11, 1, 6, 30, tzinfo=TW)
        cases = [
            ((459, 899, Fraction(899, 2)), True),
            ((500, 900, 520), True),
            ((500, 250, 490), True),
            ((500, 900, 480), True),
            ((500, 900, 400), False),
            ((500, 510, 495), False),
            ((500, 450, 400), False),
            ((500, 630, 525), True),
            ((500, 629, 525), False),
            ((500, 900, Fraction(52501, 100)), False),
            ((500, 380, 475), True),
            ((500, 490, 480), False),
            ((10, 11, 10), True),
        ]
        for values, expected in cases:
            with self.subTest(values=values):
                points = [(start + timedelta(days=i), Fraction(v))
                          for i, v in enumerate(values)]
                self.assertEqual(has_disputed_high_in_week(points, points[-1][0]),
                                 expected)

    def test_near_aba_window_and_carryover(self):
        start = datetime(2026, 11, 1, 6, 30, tzinfo=TW)
        def blocked(points, day):
            return has_disputed_high_in_week(
                [(start + timedelta(days=d), Fraction(p)) for d, p in points],
                start + timedelta(days=day))
        self.assertTrue(blocked([(0,459),(7,899),(14,449.5)],14))
        self.assertFalse(blocked([(0,459),(7,899),(15,449.5)],15))
        self.assertTrue(blocked([(0,459),(11,899),(14,449.5)],17))
        self.assertFalse(blocked([(0,459),(11,899),(14,449.5)],19))
        self.assertTrue(blocked([(0,500),(11,250),(14,490)],17))

    def test_near_aba_suppresses_real_sqlite_projection_without_losing_history(self):
        for price in [459, 899, 899, Fraction(899, 2)]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertEqual(row["seven_day_discount_pct"], 0)
        self.assertIsNone(row["seven_day_reference_price"])
        historical = list(self.db.execute(
            "SELECT payable_total_cents FROM product_price_observations ORDER BY observed_at"))
        self.assertEqual([x[0] for x in historical], [45900,89900,89900,44950])

    def test_long_aba_blocks_published_weekly_discount(self):
        for price in [89] * 7 + [179] * 6 + [89]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertEqual(row["seven_day_discount_pct"], 0)
        self.assertIsNone(row["seven_day_reference_price"])

    def test_recent_aba_blocks_even_after_old_high_expires(self):
        for price in [10,50,10] + [10] * 8 + [40,10]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertIsNone(row["seven_day_reference_price"])

    def test_abba_and_repeated_price_hopping(self):
        for price in [20,100,100,20,20,100,20]:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertIsNone(row["seven_day_reference_price"])

    def test_suppression_expires_after_reversal_window_and_weekly_high(self):
        values = [10,50,10] + [10] * 14
        for price in values:
            row = self.add(price)
        self.assertEqual(row["seven_day_is_price_deal"], 0)
        self.assertEqual(row["seven_day_discount_pct"], 0)
        # Once both reversal and disputed-high windows have expired, resume.
        self.assertEqual(row["seven_day_reference_price"], 10)


    def test_streaming_publisher_handles_more_than_one_write_batch(self):
        """Exercise the real SQLite projection on thousands of distinct SKUs."""
        from verified_weekly import UPDATE_BATCH_SIZE
        count = UPDATE_BATCH_SIZE + 7
        conn = sqlite3.connect(":memory:")
        self.addCleanup(conn.close)
        conn.executescript("""
            CREATE TABLE stores (
                store_uuid TEXT PRIMARY KEY, status TEXT, is_open INTEGER,
                last_seen TEXT);
            CREATE TABLE products (
                store_uuid TEXT, product_uuid TEXT, last_seen TEXT,
                price_evidence_status TEXT, delivery_available INTEGER,
                is_open INTEGER, status TEXT,
                seven_day_reference_price REAL, seven_day_discount_pct REAL,
                seven_day_discount_amount REAL, seven_day_is_price_deal INTEGER,
                archive_reference_price REAL, archive_discount_pct REAL,
                archive_discount_amount REAL, archive_evidence_count INTEGER,
                archive_is_price_deal INTEGER,
                PRIMARY KEY(store_uuid,product_uuid));
            CREATE TABLE product_price_observations (
                batch_id TEXT, store_uuid TEXT, product_uuid TEXT,
                observed_at TEXT, payable_total_cents INTEGER,
                received_quantity INTEGER);
        """)
        previous, latest = "20261101063000", "20261102063000"
        older, newer = (datetime.strptime(b, "%Y%m%d%H%M%S")
            .replace(tzinfo=TW).isoformat() for b in (previous, latest))
        sid = "ffffffff-ffff-4fff-8fff-ffffffffffff"
        conn.execute("INSERT INTO stores VALUES(?,?,?,?)",
                     (sid, "active", 1, newer))
        conn.executemany(
            "INSERT INTO products(store_uuid,product_uuid,last_seen,"
            "price_evidence_status,delivery_available,is_open,status) "
            "VALUES(?,?,?,?,?,?,?)",
            ((sid, str(i), newer, "VERIFIED_MENU", 1, 1, "active")
             for i in range(count)))
        conn.executemany(
            "INSERT INTO product_price_observations VALUES(?,?,?,?,?,?)",
            ((batch, sid, str(i), at, price, 1)
             for i in range(count)
             for batch,at,price in
             ((previous,older,10000),(latest,newer,2000))))
        result = update_verified_weekly(conn, latest)
        self.assertEqual(result["qualified_comparisons"], count)
        self.assertEqual(result["oscillation_suppressed"], 0)
        self.assertEqual(result["deals"], count)
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) FROM products WHERE seven_day_discount_pct=80"
        ).fetchone()[0], count)

if __name__ == "__main__":
    unittest.main()
