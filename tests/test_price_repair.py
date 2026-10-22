"""Test that old derived prices can be repaired without fetching menus."""
import sqlite3
import json
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from repair_effective_prices import repair


class StoredEffectivePricesTests(unittest.TestCase):
    def test_repair_legacy_false_discount_after_bogo_expiry(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "serving.db"
            with sqlite3.connect(db) as conn:
                conn.executescript("""
                    CREATE TABLE products(
                      store_uuid TEXT, product_uuid TEXT, price REAL,
                      promo_type TEXT, quantity INTEGER, effective_price REAL,
                      reference_price REAL, discount_amount REAL,
                      discount_pct REAL, is_price_deal INTEGER);
                    CREATE TABLE events(
                      id INTEGER PRIMARY KEY, event_time TEXT,
                      store_uuid TEXT, product_uuid TEXT, event_type TEXT,
                      old_state TEXT, new_state TEXT);
                """)
                conn.execute("INSERT INTO products VALUES(?,?,?,?,?,?,?,?,?,?)",
                             ('s','p',55,'無',1,55,89,34,38.2,1))
                before={'price':89,'promo_type':'買1送1','quantity':2,
                        'price_valid':True,'is_open':1}
                after={'price':55,'promo_type':'無','quantity':1,
                       'price_valid':True,'is_open':1}
                conn.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                             (1,'2026-10-21T17:31:47+08:00','s','p','PRICE_CHANGED',
                              json.dumps(before),json.dumps(after)))
            self.assertEqual(repair(db),(1,0))
            with sqlite3.connect(db) as conn:
                result=conn.execute(
                    "SELECT reference_price,discount_amount,discount_pct,is_price_deal "
                    "FROM products").fetchone()
                self.assertEqual(result,(44.5,0,0,0))

    def test_repair_does_not_reuse_stale_price_change_after_flat_crawl(self):
        # Last change was 180 -> 120, but a subsequent verified crawl stayed 120.
        # Rebuilding derived fields must keep a flat 120 -> 120 comparison.
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "serving.db"
            with sqlite3.connect(db) as conn:
                conn.executescript("""
                    CREATE TABLE products(
                      store_uuid TEXT, product_uuid TEXT, price REAL,
                      promo_type TEXT, quantity INTEGER, effective_price REAL,
                      reference_price REAL, discount_amount REAL,
                      discount_pct REAL, is_price_deal INTEGER, last_seen TEXT);
                    CREATE TABLE events(
                      id INTEGER PRIMARY KEY, event_time TEXT,
                      store_uuid TEXT, product_uuid TEXT, event_type TEXT,
                      old_state TEXT, new_state TEXT);
                """)
                conn.execute("INSERT INTO products VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                             ('s','p',120,'無',1,120,120,0,0,0,'2026-10-22T17:31:47+08:00'))
                before={'price':180,'promo_type':'無','quantity':1,
                        'price_valid':True,'is_open':1}
                after={'price':120,'promo_type':'無','quantity':1,
                       'price_valid':True,'is_open':1}
                conn.execute("INSERT INTO events VALUES(?,?,?,?,?,?,?)",
                             (1,'2026-10-21T17:31:47+08:00','s','p','PRICE_CHANGED',
                              json.dumps(before),json.dumps(after)))
            self.assertEqual(repair(db),(1,0))
            with sqlite3.connect(db) as conn:
                actual=conn.execute(
                    "SELECT reference_price,discount_amount,discount_pct,is_price_deal "
                    "FROM products").fetchone()
                self.assertEqual(actual,(120,0,0,0))

    def test_repair_preserves_payable_price_and_multibuy(self):
        with tempfile.TemporaryDirectory() as folder:
            db = Path(folder) / "serving.db"
            conn = sqlite3.connect(db)
            conn.execute("CREATE TABLE products(store_uuid TEXT,product_uuid TEXT,price REAL,promo_type TEXT,quantity INTEGER,effective_price REAL)")
            conn.executemany("INSERT INTO products VALUES(?,?,?,?,?,?)", [
                ("s", "discount", 168.30, "66折特惠", 1, 100.98),
                ("s", "bogo", 200, "買1送1", 2, 100),
                ("s", "normal", 150, "無", 1, 150),
            ])
            conn.commit()
            conn.close()
            self.assertEqual(repair(db), (3, 1))
            conn = sqlite3.connect(db)
            rows = dict((r[0], r[1:]) for r in conn.execute("SELECT product_uuid,price,effective_price FROM products"))
            conn.close()
            self.assertEqual(rows["discount"], (168.30, 168.30))
            self.assertEqual(rows["bogo"], (200, 100))
            self.assertEqual(rows["normal"], (150, 150))
            self.assertEqual(repair(db), (3, 0))


if __name__ == "__main__":
    unittest.main()
