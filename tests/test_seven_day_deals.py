"""End-to-end 7-day maximum price baseline for a sustained sale."""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
sys.path.insert(0,str(ROOT/'tests'))

from test_packed_updates import apply_snapshot
from verified_weekly import update_verified_weekly
from test_price_observation import menu


class SevenDayDealTests(unittest.TestCase):
    def test_sustained_half_price_is_still_a_deal_days_later(self):
        with tempfile.TemporaryDirectory() as temp:
            db=Path(temp)/'serving.db'
            conn=sqlite3.connect(db)
            conn.row_factory=sqlite3.Row
            try:
                # Same SKU, same operating-hours menu, one higher regular
                # observation six days ago, then five days of unchanged sale.
                for batch,price in (
                    ('20261021091759',100),
                    ('20261022091759',50),
                    ('20261023091759',50),
                    ('20261024091759',50),
                    ('20261026091759',50),
                    ('20261027091759',50),
                ):
                    apply_snapshot(conn,[menu(batch,price)],batch)
                update_verified_weekly(conn, '20261027091759')
                row=conn.execute('SELECT * FROM products').fetchone()
                self.assertEqual(row['seven_day_reference_price'],100)
                self.assertEqual(row['seven_day_discount_pct'],50)
                self.assertEqual(row['seven_day_discount_amount'],50)
                self.assertEqual(row['seven_day_is_price_deal'],1)
                from audit_deal_pipeline import report
                audit=report(db)
                self.assertEqual(audit['certified_7day_30pct_deals'],1)
                self.assertEqual(audit['seven_day_discrepancies'],[])
                # This sale is old: the conventional last-crawl comparator
                # is flat, but the new week-high comparator stays 50% down.
                self.assertEqual(row['discount_pct'],0)
                self.assertEqual(row['is_price_drop'],0)
                apply_snapshot(conn,[menu('20261028092000',50)],'20261028092000')
                update_verified_weekly(conn, '20261028092000')
                row=conn.execute('SELECT * FROM products').fetchone()
                self.assertEqual(row['seven_day_discount_pct'],0)
                self.assertEqual(row['seven_day_is_price_deal'],0)
                audit=report(db)
                self.assertEqual(audit['comparisons'].get('raw_menu_discounts_30pct',0),0)
                # A deliberate corrupt publisher value must STOP deployment.
                conn.execute('UPDATE products SET seven_day_discount_pct=50,seven_day_is_price_deal=1')
                conn.commit()
                with self.assertRaisesRegex(ValueError,'seven-day'):
                    report(db)
            finally:
                conn.close()


if __name__=='__main__':
    unittest.main()
