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
        self.assertIn('NEW',types)
        self.assertNotIn('PRICE_OBSERVED',types)
        self.assertNotIn('PRICE_CHANGED',types)
        observations=[json.loads(e['new_state']) for e in events if e['event_type'] in ('NEW','PRICE_OBSERVED','PRICE_CHANGED')]
        self.assertTrue(all(s['price_valid'] for s in observations if s['price']>0))
        self.assertEqual([s['price'] for s in observations if s['price']>0],[210])

    def test_closed_hour_raw_cannot_create_or_mutate_database_rows(self):
        result=apply_snapshot(self.conn,[menu('20261014014059',300)],'20261014014059')
        self.assertEqual(result['skipped_closed_stores'],1)
        for table in ('stores','products','events','product_search'):
            self.assertEqual(self.conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0],0,table)

        apply_snapshot(self.conn,[menu('20261014091759',210)],'20261014091759')
        store=dict(self.conn.execute('SELECT * FROM stores').fetchone())
        product=dict(self.conn.execute('SELECT * FROM products').fetchone())
        self.assertEqual(store['first_seen'],'2026-10-14T09:17:59+08:00')
        self.assertEqual(product['first_seen'],store['first_seen'])
        event_count=self.conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]

        fake=menu('20261015014059',999)
        fake['name']='非營業時間假店名'
        fake['hasMenu']['hasMenuSection'][0]['hasMenuItem'].append({
            'identifier':'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
            'name':'非營業時間假商品','offers':{'price':1}})
        apply_snapshot(self.conn,[fake],'20261015014059')
        self.assertEqual(dict(self.conn.execute('SELECT * FROM stores').fetchone()),store)
        self.assertEqual(dict(self.conn.execute('SELECT * FROM products').fetchone()),product)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM products').fetchone()[0],1)
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM events').fetchone()[0],event_count)

        apply_snapshot(self.conn,[menu('20261015091759',210)],'20261015091759')
        row=self.conn.execute('SELECT price,discount_pct,first_seen FROM products').fetchone()
        self.assertEqual((row['price'],row['discount_pct'],row['first_seen']),
                         (210,0,product['first_seen']))

    def test_conflicting_isopen_and_missing_hours_are_rejected(self):
        doc=menu('20261014014059',300)
        doc['workingHoursTagline']='Open until 6:30 PM'
        self.assertFalse(price_observation_valid(doc,'20261014014059'))
        doc.pop('openingHoursSpecification')
        doc['workingHoursTagline']=''
        self.assertFalse(price_observation_valid(doc,'20261014014059'))
        doc['closedMessage']='Temporarily closed'
        doc['workingHoursTagline']='Open until 10 PM'
        self.assertFalse(price_observation_valid(doc,'20261014014059'))

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

    def test_bogo_expiry_is_price_increase_not_false_38_percent_discount(self):
        original = menu('20261013091759', 89)
        item = original['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]
        item['promo_type'] = '買1送1'
        item['quantity'] = 2
        apply_snapshot(self.conn, [original], '20261013091759')

        expired = menu('20261021091759', 55)
        apply_snapshot(self.conn, [expired], '20261021091759')
        row = self.conn.execute('SELECT price,effective_price,reference_price,discount_amount,discount_pct,is_price_deal FROM products').fetchone()
        self.assertEqual((row['price'], row['effective_price'], row['reference_price']), (55, 55, 44.5))
        self.assertEqual((row['discount_amount'], row['discount_pct'], row['is_price_deal']), (0, 0, 0))
        change = self.conn.execute("SELECT old_state,new_state FROM events WHERE event_type='PRICE_CHANGED'").fetchone()
        self.assertIsNotNone(change)
        self.assertEqual(json.loads(change['old_state'])['effective_price'], 44.5)
        self.assertEqual(json.loads(change['new_state'])['effective_price'], 55)

    def test_promotion_only_change_is_a_real_unit_price_drop(self):
        apply_snapshot(self.conn, [menu('20261013091759', 89)], '20261013091759')
        promoted = menu('20261014091759', 89)
        item = promoted['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]
        item['promo_type'] = '買1送1'
        item['quantity'] = 2
        apply_snapshot(self.conn, [promoted], '20261014091759')
        row = self.conn.execute('SELECT price,effective_price,reference_price,discount_amount,discount_pct,is_price_deal FROM products').fetchone()
        self.assertEqual((row['price'], row['effective_price'], row['reference_price']), (89, 44.5, 89))
        self.assertEqual((row['discount_amount'], row['discount_pct'], row['is_price_deal']), (44.5, 50, 1))
        types = [r[0] for r in self.conn.execute("SELECT event_type FROM events WHERE event_type IN ('PRICE_CHANGED','PROMOTION_CHANGED')")]
        self.assertIn('PRICE_CHANGED', types)
        self.assertIn('PROMOTION_CHANGED', types)

    def test_direct_three_tenths_discount_uses_payable_price_not_original(self):
        # Uber menu offer price is the discounted payable amount (30),
        # while the original 100 is only promotional context.
        discounted = menu('20261013091759', 30)
        discounted['hasMenu']['hasMenuSection'][0]['name'] = '3折特惠'
        apply_snapshot(self.conn, [discounted], '20261013091759')
        self.assertEqual(self.conn.execute('SELECT promo_type FROM products').fetchone()[0], '3折特惠')

        regular = menu('20261014091759', 50)
        apply_snapshot(self.conn, [regular], '20261014091759')
        row = self.conn.execute(
            'SELECT price,effective_price,reference_price,discount_amount,discount_pct,is_price_deal FROM products'
        ).fetchone()
        self.assertEqual((row['price'], row['effective_price'], row['reference_price']), (50, 50, 30))
        self.assertEqual((row['discount_amount'], row['discount_pct'], row['is_price_deal']), (0, 0, 0))
        history = [json.loads(r['new_state']) for r in self.conn.execute(
            "SELECT new_state FROM events WHERE event_type IN ('NEW','PRICE_CHANGED') ORDER BY event_time"
        ).fetchall()]
        self.assertEqual([r['effective_price'] for r in history], [30, 50])

    def test_direct_discount_begins_without_double_application(self):
        regular = menu('20261013091759', 50)
        apply_snapshot(self.conn, [regular], '20261013091759')
        discounted = menu('20261014091759', 30)
        discounted['hasMenu']['hasMenuSection'][0]['name'] = '3折特惠'
        apply_snapshot(self.conn, [discounted], '20261014091759')
        row = self.conn.execute(
            'SELECT price,effective_price,reference_price,discount_amount,discount_pct,is_price_deal FROM products'
        ).fetchone()
        self.assertEqual((row['price'], row['effective_price'], row['reference_price']), (30, 30, 50))
        self.assertEqual((row['discount_amount'], row['discount_pct'], row['is_price_deal']), (20, 40, 1))

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
        self.assertFalse(price_observation_valid(doc,'20261014014059')) # Unknown: raw only


if __name__=='__main__':
    unittest.main()
