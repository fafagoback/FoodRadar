"""Cross-path regressions for the seven-day maximum verified unit-price rule."""
import sqlite3
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from price_change import PriceChange, compare_effective_prices
from alert_engine import UberEatsAlertEngine
from export_static_snapshots import calculate_previous_discounts


class PriceComparisonContractTests(unittest.TestCase):
    def test_week_high_keeps_sustained_sale(self):
        from datetime import datetime, timedelta, timezone
        tz=timezone(timedelta(hours=8))
        now=datetime(2026,10,27,18,0,tzinfo=tz)
        samples=[
            ((now-timedelta(days=6)).isoformat(), 100),
            *[((now-timedelta(days=i)).isoformat(), 50) for i in range(1,6)]
        ]
        week=PriceChange.seven_day_high(samples,now.isoformat(),50)
        self.assertEqual(week['reference'],100)
        self.assertEqual(week['comparison']['discount_pct'],50)
        self.assertEqual(PriceChange.seven_day_high(samples[:1],now.isoformat(),50)['reference'],100)
        self.assertIsNone(PriceChange.seven_day_high(
            [((now-timedelta(days=8)).isoformat(),100)],now.isoformat(),50))
        self.assertIsNone(PriceChange.seven_day_high([(now.isoformat(),100)],now.isoformat(),50))


    def test_unit_rule_includes_all_drops_without_promo(self):
        self.assertIsNone(compare_effective_prices(None, 120))
        self.assertIsNone(compare_effective_prices(0, 120))
        self.assertEqual(compare_effective_prices(180, 120)["discount_pct"], 33.33)
        self.assertEqual(compare_effective_prices(120, 120)["direction"], "flat")
        self.assertEqual(compare_effective_prices(30, 50)["direction"], "up")
        self.assertEqual(compare_effective_prices(1000, 100)["discount_pct"], 90.0)
        self.assertEqual(compare_effective_prices(89 / 2, 55)["direction"], "up")
        self.assertEqual(PriceChange.persisted(PriceChange.compare(180, 120)), {
            "reference_price": 180.0,
            "price_direction": "down",
            "is_price_drop": 1,
            "price_change_amount": -60.0,
            "price_change_percent": -33.33,
            "discount_amount": 60.0,
            "discount_pct": 33.33,
            "is_price_deal": 1,
        })
        self.assertFalse(PriceChange.qualifies(PriceChange.compare(120,120)))
        self.assertTrue(PriceChange.qualifies(PriceChange.compare(180,120), 30))
        self.assertFalse(PriceChange.qualifies(PriceChange.compare(180,120), 40))
        self.assertEqual(PriceChange.persisted(PriceChange.compare(None,120))["price_direction"], "unknown")
        self.assertEqual(PriceChange.persisted(PriceChange.compare(None,120))["is_price_drop"], 0)
        self.assertEqual(PriceChange.persisted(PriceChange.compare(120,120))["is_price_drop"], 0)
        self.assertEqual(PriceChange.persisted(PriceChange.compare(180,120))["is_price_drop"], 1)

    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.db.row_factory = sqlite3.Row
        self.addCleanup(self.db.close)
        self.db.executescript("""
        CREATE TABLE crawl_batches(crawled_time TEXT);
        CREATE TABLE stores(
            store_id TEXT, crawled_time TEXT, order_action_url TEXT, store_url TEXT,
            rating_value REAL, review_count INTEGER, region TEXT, locality TEXT,
            street_address TEXT
        );
        CREATE TABLE products(
            product_id TEXT, store_id TEXT, crawled_time TEXT, store_name TEXT,
            product_name TEXT, category_name TEXT, description TEXT,
            price REAL, quantity INTEGER, promo_type TEXT, eff_price REAL,
            is_open INTEGER
        );
        """)
        self.db.execute("INSERT INTO stores VALUES('store1','20261031063000','','',4.5,10,'','','')")
        self.db.execute("INSERT INTO crawl_batches VALUES('20261031063000')")

    def insert(self, pid, time, price, valid=1, store="store1"):
        self.db.execute(
            "INSERT INTO products VALUES(?,?,?,'商店','商品','類別','',?,1,'無',?,?)",
            (pid, store, time, price, price, valid)
        )
        self.db.execute("INSERT INTO crawl_batches VALUES(?)", (time,))

    def test_alert_and_sqlite_export_apply_seven_day_cutoff_and_ignore_invalid_observations(self):
        # An older high outside seven days must NEVER be used as today's
        # reference. Only the verified 10/26 in-hours price can qualify.
        self.insert("old", "20261001063000", 400)
        self.insert("old", "20261026063000", 180)
        self.insert("old", "20261030063000", 999, valid=0)
        self.insert("old", "20261031063000", 120)
        # A no-promo 90-percent drop within seven days is valid.
        self.insert("deep", "20261025063000", 1000)
        self.insert("deep", "20261031063000", 100)
        # A long-ago high is excluded, even for a large drop.
        self.insert("expired", "20261001063000", 500)
        self.insert("expired", "20261031063000", 120)
        # Same price repeated is not a price drop.
        self.insert("steady", "20261027063000", 120)
        self.insert("steady", "20261031063000", 120)
        # Same product ID from another store must not be compared.
        self.insert("old", "20261026063000", 10, store="store2")
        self.insert("old", "20261031063000", 30, store="store2")
        self.db.commit()

        engine = UberEatsAlertEngine(db_path=":memory:")
        alerts = engine._detect_big_discounts(
            self.db, "20261031063000", "20261030063000", 0, 0
        )
        got = {r["product_id"]: r for r in alerts}
        self.assertEqual(set(got), {"old", "deep"})
        self.assertEqual((got["old"]["original_price"], got["old"]["current_price"]), (180, 120))
        self.assertEqual(got["deep"]["discount_pct"], 90.0)

        exported = calculate_previous_discounts(self.db, "20261031063000")
        snapshot = {r["product_id"]: r for r in exported}
        self.assertEqual(set(snapshot), {"old", "deep"})
        self.assertEqual(snapshot["old"]["discount_pct"], 33.3)
        self.assertEqual(snapshot["deep"]["discount_pct"], 90.0)

    def test_sustained_sale_is_still_discounted_when_last_observation_is_flat(self):
        for time, price in [("20261026063000", 180), ("20261030063000", 120),
                            ("20261031063000", 120)]:
            self.insert("old", time, price)
        self.db.commit()
        alerts = UberEatsAlertEngine(db_path=":memory:")._detect_big_discounts(
            self.db, "20261031063000", "20261030063000", 30, 0
        )
        self.assertEqual(len(alerts),1)
        self.assertEqual(alerts[0]["original_price"],180)
        self.assertEqual(alerts[0]["current_price"],120)
        results=calculate_previous_discounts(self.db, "20261031063000",30,0)
        self.assertEqual(len(results),1)
        self.assertEqual(results[0]["original_price"],180)
        self.assertEqual(results[0]["current_price"],120)


if __name__ == "__main__":
    unittest.main()
