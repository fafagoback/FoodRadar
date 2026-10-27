"""Recovery contract: archived menu quotes are not verified price events."""
import copy
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
sys.path.insert(0,str(ROOT/'tests'))
from serving_state import apply_snapshot
from repair_effective_prices import repair
from restore_hf_menu_quotes import apply_archival_quote_references, canonical_menu_quotes
from test_price_observation import menu, SID, PID

def old_quote(batch,price):
    doc=menu(batch,price)
    doc['menu_status']='present'
    doc['closedMessage']='Delivery unavailable'
    doc['isOpen']=True
    return doc

class ArchiveQuotePriceTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=sqlite3.Row
        self.addCleanup(self.conn.close)
        current=menu('20261027091759',50)
        apply_snapshot(self.conn,[current],'20261027091759')
        repair_db_needed=False
        # Full repair requires on-disk DB; verified last-seven price stays zero
        # with one current observation, so the separation can be tested as is.

    def row(self):
        return self.conn.execute('SELECT * FROM products WHERE product_uuid=?',(PID,)).fetchone()

    def test_repeated_quotes_create_separate_nonverified_suggestion(self):
        result=apply_archival_quote_references(self.conn,'20261027091759',[
            ('20261021091759',[old_quote('20261021091759',100)]),
            ('20261023091759',[old_quote('20261023091759',100)]),
        ])
        row=self.row()
        self.assertEqual(result['archive_quote_candidates'],1)
        self.assertEqual(row['archive_reference_price'],100)
        self.assertEqual(row['archive_discount_pct'],50)
        self.assertEqual(row['archive_evidence_count'],2)
        self.assertEqual(row['archive_is_price_deal'],1)
        self.assertEqual(row['seven_day_is_price_deal'],0)
        self.assertEqual(row['is_price_drop'],0)

    def test_single_anomalous_high_quote_is_not_enough(self):
        result=apply_archival_quote_references(self.conn,'20261027091759',[
            ('20261021091759',[old_quote('20261021091759',100)]),
            ('20261023091759',[old_quote('20261023091759',50)]),
        ])
        self.assertEqual(result['archive_quote_candidates'],0)
        self.assertIsNone(self.row()['archive_reference_price'])

    def test_closed_outside_hours_or_older_than_seven_days_rejected(self):
        offhour=old_quote('20261020010344',100)
        self.assertEqual(apply_archival_quote_references(self.conn,'20261027091759',[
            ('20261020010344',[offhour]),
            ('20261018091759',[old_quote('20261018091759',100)]),
        ])['archive_quote_candidates'],0)

    def test_duplicate_general_and_bogo_sections_do_not_invent_sale(self):
        one=old_quote('20261021091759',100)
        original=one['hasMenu']['hasMenuSection'][0]
        duplicate=copy.deepcopy(original)
        duplicate['name']='買1送1'
        one['hasMenu']['hasMenuSection']=[original,duplicate]
        self.assertEqual(canonical_menu_quotes(one,SID)[PID],50)
        two=copy.deepcopy(one)
        apply_archival_quote_references(self.conn,'20261027091759',[
            ('20261021091759',[one]),('20261023091759',[two])
        ])
        self.assertEqual(self.row()['archive_discount_pct'],0)

    def test_restore_resets_old_suggestions_after_expiry(self):
        self.test_repeated_quotes_create_separate_nonverified_suggestion()
        apply_archival_quote_references(self.conn,'20261027091759',[])
        self.assertEqual(self.row()['archive_is_price_deal'],0)

if __name__=='__main__':
    unittest.main()
