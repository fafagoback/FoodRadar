import json
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))

from price_observation import price_observation_valid
from serving_state import apply_snapshot
from test_packed_updates import document

SID='ffffffff-ffff-4fff-8fff-ffffffffffff'
PID='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'


def menu(batch,price):
    doc=document(SID,[PID])
    doc['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price']=price
    doc['openingHoursSpecification']=[{
        '@type':'OpeningHoursSpecification',
        'dayOfWeek':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday'],
        'opens':'09:00:00','closes':'18:30:00'}]
    doc['workingHoursTagline']='Opens 9:00 AM' if batch[8:12]<'0900' else 'Open until 6:30 PM'
    return doc


class PriceValidityTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(':memory:')
        self.conn.row_factory=sqlite3.Row
        self.addCleanup(self.conn.close)

    def test_real_archived_noodle_price_does_not_fake_a_discount(self):
        self.assertFalse(price_observation_valid(menu('20261014014059',300),'20261014014059'))
        self.assertTrue(price_observation_valid(menu('20261014091759',210),'20261014091759'))
        self.assertFalse(price_observation_valid(menu('20261020010344',300),'20261020010344'))
        self.assertTrue(price_observation_valid(menu('20261020090813',210),'20261020090813'))

        for batch,price in [('20261014014059',300),('20261014091759',210),
                            ('20261020010344',300),('20261020090813',210)]:
            apply_snapshot(self.conn,[menu(batch,price)],batch)
        row=self.conn.execute('SELECT * FROM products WHERE product_uuid=?',(PID,)).fetchone()
        self.assertEqual(row['price'],210)
        self.assertEqual(json.loads(row['recent_prices']),[210])
        self.assertEqual(row['discount_pct'],0)
        self.assertEqual(row['discount_amount'],0)
        self.assertEqual(row['is_price_deal'],0)
        events=self.conn.execute("SELECT event_type,new_state FROM events WHERE product_uuid=?",(PID,)).fetchall()
        types=[e['event_type'] for e in events]
        self.assertIn('PRICE_OBSERVED',types)
        self.assertNotIn('PRICE_CHANGED',types)
        observations=[json.loads(e['new_state']) for e in events if e['event_type'] in ('NEW','PRICE_OBSERVED','PRICE_CHANGED')]
        self.assertTrue(all(s['price_valid'] for s in observations if s['price']>0))
        self.assertEqual([s['price'] for s in observations if s['price']>0],[210])

    def test_baseline_valid_price_is_recorded_even_without_any_future_price_changes(self):
        apply_snapshot(self.conn,[menu('20261006091759',210)],'20261006091759')
        apply_snapshot(self.conn,[menu('20261014014059',300)],'20261014014059')
        apply_snapshot(self.conn,[menu('20261020090813',210)],'20261020090813')
        events=self.conn.execute("SELECT event_type,event_time,new_state FROM events WHERE event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED')").fetchall()
        self.assertEqual(len(events),1)
        self.assertEqual(events[0]['event_type'],'NEW')
        self.assertEqual(json.loads(events[0]['new_state'])['price'],210)
        self.assertTrue(json.loads(events[0]['new_state'])['price_valid'])

    def test_closed_after_known_price_preserves_previous_deal_state(self):
        for batch,price in [('20261014091759',300),('20261015091759',210),
                            ('20261016014059',300)]:
            apply_snapshot(self.conn,[menu(batch,price)],batch)
        row=self.conn.execute('SELECT * FROM products').fetchone()
        self.assertEqual((row['price'],row['reference_price'],row['discount_pct'],row['is_price_deal']),(210,300,30,1))
        rows=self.conn.execute("SELECT new_state FROM events WHERE event_type='PRICE_CHANGED'").fetchall()
        self.assertEqual(len(rows),1)
        self.assertEqual(json.loads(rows[0][0])['price_valid'],True)

    def test_per_store_captured_at_not_early_batch_start(self):
        doc=menu('20261014014059',300)
        doc['captured_at']='2026-10-14T09:15:00+08:00'
        doc['workingHoursTagline']='Open until 6:30 PM'
        self.assertTrue(price_observation_valid(doc,'20261014014059'))

    def test_overnight_and_no_opening_schedules(self):
        doc=menu('20261014014059',300)
        doc['openingHoursSpecification']=[{'dayOfWeek':['Tuesday'],
                                           'opens':'22:00:00','closes':'03:00:00'}]
        doc['workingHoursTagline']='Open until 3:00 AM'
        self.assertTrue(price_observation_valid(doc,'20261014014059')) # Wednesday 01:40
        self.assertFalse(price_observation_valid(doc,'20261015014059')) # Thursday 01:40
        doc.pop('openingHoursSpecification')
        doc['workingHoursTagline']='Opens 9:00 AM'
        self.assertFalse(price_observation_valid(doc,'20261014014059'))
        doc['workingHoursTagline']=''
        self.assertTrue(price_observation_valid(doc,'20261014014059')) # Legacy isOpen fallback


if __name__=='__main__':
    unittest.main()
