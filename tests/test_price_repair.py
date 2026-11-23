"""Legacy repair must never reclassify old text as certified price evidence."""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
from repair_effective_prices import repair
from test_packed_updates import document, apply_snapshot


class StoredEffectivePricesTests(unittest.TestCase):
    def test_v1_unverified_legacy_db_refused(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/'serving.db'
            with sqlite3.connect(db) as conn:
                conn.execute('CREATE TABLE products(price REAL,promo_type TEXT,quantity INTEGER,effective_price REAL)')
                conn.execute("INSERT INTO products VALUES(200,'買A送B',2,100)")
            with self.assertRaisesRegex(ValueError,'V1 SQLite'):
                repair(db)

    def test_v2_repair_only_numeric_quotes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/'serving.db'
            with sqlite3.connect(db) as conn:
                conn.row_factory=sqlite3.Row
                sid='ffffffff-ffff-4fff-8fff-ffffffffffff'
                pid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
                before=document(sid,[pid])
                apply_snapshot(conn,[before],'20261021063000')
                after=document(sid,[pid])
                after['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price']=50
                apply_snapshot(conn,[after],'20261022063000')
            self.assertEqual(repair(db),(1,1))
            with sqlite3.connect(db) as conn:
                row=conn.execute('SELECT seven_day_discount_pct,seven_day_is_price_deal FROM products').fetchone()
                self.assertEqual(row,(50,1))


if __name__=='__main__':
    unittest.main()
