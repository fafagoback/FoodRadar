import json
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))

from price_observation import price_observation_valid, menu_catalog_valid
from test_packed_updates import apply_snapshot
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
        self.assertEqual([s['price'] for s in observations if s['price']>0],[210,210])

    def test_closed_hours_keep_raw_quotes_without_creating_verified_price_events(self):
        from repair_effective_prices import repair
        first=menu('20261014014059',300)
        result=apply_snapshot(self.conn,[first],'20261014014059')
        self.assertEqual(result['stores'],1)
        row=self.conn.execute('SELECT price,menu_quote_price,delivery_available FROM products').fetchone()
        self.assertEqual(tuple(row),(0,300,0))
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM product_search').fetchone()[0],1)

        live=menu('20261014091759',210)
        apply_snapshot(self.conn,[live],'20261014091759')
        row=self.conn.execute('SELECT price,menu_quote_price,delivery_available FROM products').fetchone()
        self.assertEqual(tuple(row),(210,210,1))
        # The prior 300 quote is a comparable historical menu price even
        # though it was not a validated payable / delivery price.
        last=self.conn.execute('SELECT menu_quote_history FROM products').fetchone()[0]
        self.assertEqual([point[1] for point in json.loads(last)],[210])  # 01:40 closed 300 excluded

        late=menu('20261015014059',999)
        apply_snapshot(self.conn,[late],'20261015014059')
        row=self.conn.execute('SELECT price,menu_quote_price,delivery_available FROM products').fetchone()
        self.assertEqual(tuple(row),(210,999,0))
        self.assertEqual([point[1] for point in json.loads(self.conn.execute(
            'SELECT menu_quote_history FROM products').fetchone()[0])],[210])
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='PRICE_CHANGED'").fetchone()[0],0)

    def test_yuchi_shilin_overnight_bogo_is_not_new_discount(self):
        from price_observation import menu_price_observation_valid
        from repair_effective_prices import repair
        # Same product UUID, 55 NTD list price in regular category every time.
        # BOGO category exists only in OPEN-hours snapshots.
        def yuchi(batch, bogo, is_open, opens):
            d=menu(batch,55)
            d['isOpen']=True  # Uber's flag was stale even at 08:42.
            d['closedMessage']='Delivery unavailable'
            d['workingHoursTagline']='Open until 1:00 AM' if is_open else 'Opens 9:30 AM'
            d['openingHoursSpecification']=[{
                'dayOfWeek':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'],
                'opens':'09:30','closes':'01:00'}]
            d['hasMenu']['hasMenuSection'][0]['name']='台灣50年經典'
            if bogo:
                d['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['test_native_bogo']=True
                sec={'name':'Buy 1, get 1 free','hasMenuItem':[
                    dict(d['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0])]}
                d['hasMenu']['hasMenuSection'].insert(0,sec)
            self.assertEqual(menu_price_observation_valid(d,batch),is_open)
            return d
        for batch, promo, open_hour in [
            ('20261021084227',False,False),
            ('20261022233044',True,True),
            ('20261025082758',False,False),
            ('20261025223028',True,True),
            ('20261027235841',True,True)]:
            apply_snapshot(self.conn,[yuchi(batch,promo,open_hour,'09:30')],batch)
        p=self.conn.execute("SELECT menu_quote_price,menu_quote_effective_price,menu_quote_history FROM products").fetchone()
        self.assertEqual((p[0],p[1]),(55,27.5))
        self.assertEqual([point[1] for point in json.loads(p[2])],[27.5,27.5,27.5])
        self.assertEqual(
            [(point[2], point[3], point[4]) for point in json.loads(p[2])],
            [(55, '買1送1', 2)] * 3,
        )

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
        # Archived getStoreV1 snapshots showed isOpen=True even when the
        # restaurant could not deliver. These must NEVER create price events.
        doc['closedMessage']='Delivery unavailable'
        self.assertFalse(price_observation_valid(doc,'20261014091759'))

    def test_delivery_unavailable_keeps_menu_visible_without_verified_discount(self):
        doc = menu('20261023091759', 120)
        doc['closedMessage'] = 'Delivery unavailable'
        doc['menu_status'] = 'present'
        self.assertFalse(price_observation_valid(doc, '20261023091759'))
        self.assertTrue(menu_catalog_valid(doc, '20261023091759'))
        counts = apply_snapshot(self.conn, [doc], '20261023091759', baseline=False)
        self.assertEqual((counts['stores'], counts['products']), (1, 1))
        row = self.conn.execute('SELECT price,menu_quote_price,menu_quote_effective_price,delivery_available FROM products').fetchone()
        self.assertEqual(tuple(row), (0, 120, 120, 0))
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM product_search').fetchone()[0], 1)
        event = self.conn.execute("SELECT new_state FROM events WHERE event_type='NEW'").fetchone()
        self.assertFalse(json.loads(event[0])['price_valid'])

        available = menu('20261024091759', 90)
        apply_snapshot(self.conn, [available], '20261024091759')
        row = self.conn.execute('SELECT price,menu_quote_price,delivery_available,discount_pct FROM products').fetchone()
        self.assertEqual(tuple(row), (90, 90, 1, 0))
        self.assertEqual(self.conn.execute("SELECT COUNT(*) FROM events WHERE event_type='PRICE_CHANGED'").fetchone()[0], 0)

    def test_delivery_banner_does_not_override_real_closure(self):
        doc = menu('20261023014059', 500)
        doc['closedMessage'] = 'Delivery unavailable'
        self.assertTrue(menu_catalog_valid(doc, '20261023014059'))
        result = apply_snapshot(self.conn, [doc], '20261023014059')
        self.assertEqual(result['stores'], 1)
        self.assertEqual(self.conn.execute('SELECT delivery_available FROM products').fetchone()[0],0)

    def test_baseline_valid_price_is_recorded_even_without_any_future_price_changes(self):
        apply_snapshot(self.conn,[menu('20261006091759',210)],'20261006091759')
        apply_snapshot(self.conn,[menu('20261014014059',300)],'20261014014059')
        apply_snapshot(self.conn,[menu('20261020090813',210)],'20261020090813')
        events=self.conn.execute("SELECT event_type,event_time,new_state FROM events WHERE event_type IN ('NEW','PRICE_OBSERVED','PRICE_CHANGED')").fetchall()
        self.assertEqual(len(events),2)
        self.assertEqual([event['event_type'] for event in events], ['NEW', 'PRICE_OBSERVED'])
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
        item['test_native_bogo'] = True
        item['test_native_bogo'] = True
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
        discounted['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['test_native_direct_old_price'] = 100
        apply_snapshot(self.conn, [discounted], '20261013091759')
        self.assertEqual(self.conn.execute('SELECT promo_type FROM products').fetchone()[0], '直接折價')

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
        discounted['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['test_native_direct_old_price'] = 50
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

    def test_menu_catalog_does_not_mean_currently_open(self):
        """A closed-hour menu is searchable but cannot be called open or comparable."""
        closed = menu('20261023014059', 55)
        self.assertTrue(menu_catalog_valid(closed, '20261023014059'))
        apply_snapshot(self.conn, [closed], '20261023014059')
        store = self.conn.execute('SELECT status,is_open FROM stores').fetchone()
        product = self.conn.execute('SELECT status,is_open,menu_quote_price,menu_quote_history FROM products').fetchone()
        self.assertEqual(tuple(store), ('active', 0))
        self.assertEqual(tuple(product)[:3], ('active', 0, 55))
        self.assertEqual(json.loads(product['menu_quote_history']), [])
        self.assertEqual(self.conn.execute('SELECT COUNT(*) FROM product_search').fetchone()[0], 1)

    def test_same_sku_promo_section_uses_its_own_raw_price(self):
        """Different original prices across category duplicates must not mix."""
        import copy
        doc = menu('20261026091759', 100)
        plain = doc['hasMenu']['hasMenuSection'][0]
        plain['name'] = '一般飲料'
        promo = copy.deepcopy(plain)
        promo['name'] = '買1送1'
        promo['hasMenuItem'][0]['offers']['price'] = 80
        promo['hasMenuItem'][0]['test_native_bogo'] = True
        doc['hasMenu']['hasMenuSection'] = [plain, promo]
        apply_snapshot(self.conn, [doc], '20261026091759')
        result = self.conn.execute('SELECT price,promo_type,quantity,effective_price,menu_quote_price FROM products').fetchone()
        self.assertEqual(tuple(result), (100,'未驗證',1,100,100))
        doc['hasMenu']['hasMenuSection'] = [promo, plain]
        apply_snapshot(self.conn, [doc], '20261027091759')
        result = self.conn.execute('SELECT price,promo_type,quantity,effective_price FROM products').fetchone()
        self.assertEqual(result['promo_type'], '未驗證')
        self.assertEqual(self.conn.execute(
            'SELECT COUNT(*) FROM product_price_observations').fetchone()[0], 0)

    def test_same_sku_in_normal_and_bogo_sections_is_order_independent(self):
        """A category carousel/order swap must never invent a 50% price jump."""
        import copy
        first = menu('20261026091759',100)
        regular = first['hasMenu']['hasMenuSection'][0]
        regular['name'] = '所有商品'
        regular['hasMenuItem'][0]['test_native_bogo'] = True
        featured = copy.deepcopy(regular)
        featured['name'] = '買1送1'
        first['hasMenu']['hasMenuSection'] = [regular, featured]
        apply_snapshot(self.conn,[first],'20261026091759')
        row = self.conn.execute(
            'SELECT price,promo_type,quantity,effective_price FROM products').fetchone()
        self.assertEqual(tuple(row),(100,'買1送1',2,50))

        swapped = menu('20261027091759',100)
        swapped['hasMenu']['hasMenuSection'] = [copy.deepcopy(featured),copy.deepcopy(regular)]
        apply_snapshot(self.conn,[swapped],'20261027091759')
        row = self.conn.execute(
            'SELECT price,promo_type,quantity,effective_price,discount_pct,is_price_drop FROM products').fetchone()
        self.assertEqual(tuple(row),(100,'買1送1',2,50,0,0))
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) FROM events WHERE event_type='PRICE_CHANGED'").fetchone()[0],0)

        # A genuine disappearance of the promotional category remains a rise.
        ended = menu('20261028091759',100)
        ended['hasMenu']['hasMenuSection'][0]['name'] = '所有商品'
        apply_snapshot(self.conn,[ended],'20261028091759')
        row = self.conn.execute(
            'SELECT effective_price,reference_price,price_direction FROM products').fetchone()
        self.assertEqual(tuple(row),(100,50,'up'))


if __name__=='__main__':
    unittest.main()
