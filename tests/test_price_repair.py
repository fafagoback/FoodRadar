"""Test that old derived prices can be repaired without fetching menus."""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
from repair_effective_prices import repair


class StoredEffectivePricesTests(unittest.TestCase):
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
