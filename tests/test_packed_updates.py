import contextlib
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
from serving_state import apply_snapshot as apply_real_snapshot
from verified_prices import quote_from_native_item
from verified_weekly import update_verified_weekly
from build_web_db import build_web_db
from verify_packed_local import verify
from publish_turso_delta import plan_table


def document(sid,pids):
    return {'@type':'Restaurant','@id':f'https://www.ubereats.com/tw/store/test/{sid}',
        'store_uuid':sid,'name':'測試餐廳','isOpen':True,
        'openingHoursSpecification':[{'dayOfWeek':['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'],'opens':'00:00','closes':'00:00'}],
        'address':{'streetAddress':'台北市測試路1號'},
        'hasMenu':{'hasMenuSection':[{'name':'飯','hasMenuItem':[
            {'identifier':pid,'name':'雞肉飯','offers':{'price':100}} for pid in pids]}]}}

def apply_snapshot(conn, docs, batch, *args, **kwargs):
    """Test-only: turn explicit synthetic inputs into actual native V2 fields.

    Bare category/marketing text must not be converted into promotions.
    """
    from decimal import Decimal
    for doc in docs:
        for section in (doc.get("hasMenu") or {}).get("hasMenuSection") or []:
            for item in section.get("hasMenuItem") or []:
                raw_price = (item.get("offers") or {}).get("price")
                try:
                    cents = int(Decimal(str(raw_price)) * 100)
                except (ValueError, TypeError, ArithmeticError):
                    cents = 0
                raw = {
                    "uuid": item.get("identifier"), "title": item.get("name"),
                    "price": cents,
                    "catalogItemAnalyticsData": {"promoType": None},
                }
                if item.get("test_native_bogo"):
                    raw["catalogItemAnalyticsData"]["promoType"] = "BOGO"
                    raw["itemPromotion"] = {
                        "type": "buyXGetYItemPromotion",
                        "buyXGetYItemPromotion": {
                            "buyQuantity": item.get("test_buy_quantity", 1),
                            "getQuantity": item.get("test_free_quantity", 1),
                        },
                    }
                if item.get("test_native_direct_old_price") is not None:
                    old = item["test_native_direct_old_price"]
                    raw["catalogItemAnalyticsData"]["promoType"] = "DISCOUNTED_ITEM"
                    def price_label(value, struck=False):
                        return {"type": "text", "text": {
                            "text": {"text": f"NT${value}"},
                            "predefinedDecorations": ["STRIKE_THROUGH"] if struck else None}}
                    raw["itemThumbnailElements"] = [{"payload": {"labelPayload": {
                        "label": {"richTextElements": [price_label(raw_price), price_label(old, True)]}}}}]
                if item.get("test_native_boga"):
                    raw["catalogItemAnalyticsData"]["promoType"] = "BOGA"
                    raw["itemPromotion"] = {
                        "type": "buyXGetYItemPromotion",
                        "buyXGetYItemPromotion": {"buyQuantity": 1, "getQuantity": 0},
                    }
                item["verified_price"] = quote_from_native_item(raw)
    result = apply_real_snapshot(conn, docs, batch, *args, **kwargs)
    update_verified_weekly(conn, batch)
    return result


class PackedUpdateTests(unittest.TestCase):
    def test_same_day_recollection_is_baseline_not_new_store_or_product(self):
        import msgpack
        import zstandard
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), contextlib.ExitStack() as stack:
            root=Path(tmp)
            conn=sqlite3.connect(root/'state.db')
            conn.row_factory=sqlite3.Row
            stack.callback(conn.close)
            sid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            pid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            nid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            apply_snapshot(conn,[document(sid,[pid])],'20261006091947')
            apply_snapshot(conn,[document(sid,[pid,nid]),document(nid,[pid])],'20261006094852')
            build_web_db(root/'state.db',root/'a.db')
            with contextlib.closing(sqlite3.connect(root/'a.db')) as packed:
                refs=[]
                for payload, in packed.execute('SELECT payload FROM search_buckets'):
                    refs.extend(msgpack.unpackb(zstandard.ZstdDecompressor().decompress(payload),raw=False).get('f:new',[]))
                self.assertEqual(refs,[])
                self.assertEqual(packed.execute("SELECT COUNT(*) FROM store_directory WHERE substr(first_seen,1,10) > '2026-10-06'").fetchone()[0],0)

    def test_original_menu_text_supports_promotion_queries(self):
        with contextlib.closing(sqlite3.connect(':memory:')) as conn:
            conn.row_factory=sqlite3.Row
            doc=document('ffffffff-ffff-4fff-8fff-ffffffffffff',['ffffffff-ffff-4fff-8fff-ffffffffffff'])
            doc['hasMenu']['hasMenuSection'][0]['name']='買1送1'
            apply_snapshot(conn,[doc],'20261006063000')
            product=conn.execute('SELECT promo_type,quantity,effective_price FROM products').fetchone()
            # Menu section text alone is not a numeric BOGO proof.
            self.assertEqual(tuple(product),('無',1,100))

    def test_daily_observation_does_not_rewrite_current_bundles_or_indexes(self):
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), contextlib.ExitStack() as stack:
            root = Path(tmp)
            conn = sqlite3.connect(root/'state.db')
            conn.row_factory = sqlite3.Row
            stack.callback(conn.close)
            sid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            pid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            docs=[document(sid,[pid])]
            apply_snapshot(conn,docs,'20261006063000')
            build_web_db(root/'state.db',root/'a.db')
            apply_snapshot(conn,docs,'20261007063000')
            build_web_db(root/'state.db',root/'b.db')
            # The product remains searchable when its price is unchanged, but
            # the current snapshot time advances; old date-stamping tricks
            # must never erase that verified observation.
            with contextlib.closing(sqlite3.connect(root/'a.db')) as a, contextlib.closing(sqlite3.connect(root/'b.db')) as b:
                self.assertEqual(a.execute('SELECT * FROM search_buckets').fetchall(),
                                 b.execute('SELECT * FROM search_buckets').fetchall())
                self.assertEqual(a.execute('SELECT last_seen FROM store_directory').fetchone()[0],
                                 '2026-10-06T06:30:00+08:00')
                self.assertEqual(b.execute('SELECT last_seen FROM store_directory').fetchone()[0],
                                 '2026-10-07T06:30:00+08:00')
            self.assertEqual(conn.execute('SELECT reference_price,discount_pct FROM products').fetchone()[:], (100,0))
            # New identities sort before the old UUID but must append to IDs,
            # and a new product must leave the existing product ref unchanged.
            new_sid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            new_pid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            apply_snapshot(conn,[document(sid,[pid,new_pid]),document(new_sid,[new_pid])],'20261008063000')
            build_web_db(root/'state.db',root/'c.db')
            report = verify(root/'c.db')
            self.assertEqual(report['products'],3)
            with contextlib.closing(sqlite3.connect(root/'c.db')) as c:
                self.assertEqual(c.execute('SELECT store_id FROM store_directory WHERE store_uuid=?',(sid,)).fetchone()[0],0)

    def test_raw_delivery_unavailable_prices_drive_formal_seven_day_discount(self):
        import json
        import contextlib
        with tempfile.TemporaryDirectory() as tmp:
            file=Path(tmp)/'serving.db'
            con=sqlite3.connect(file)
            con.row_factory=sqlite3.Row
            sid='ffffffff-ffff-4fff-8fff-ffffffffffff'
            pid='aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            old=document(sid,[pid])
            old['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price']=100
            old['closedMessage']='Delivery unavailable'
            old['isOpen']=True  # open-hours menu, delivery is still unavailable
            apply_snapshot(con,[old],'20261022090000')
            current=document(sid,[pid])
            current['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price']=50
            current['closedMessage']='Delivery unavailable'
            current['isOpen']=True
            apply_snapshot(con,[current],'20261023090000')
            con.close()
            with contextlib.closing(sqlite3.connect(file)) as verify:
                p=verify.execute("SELECT menu_quote_price,delivery_available,seven_day_reference_price,seven_day_discount_pct,seven_day_is_price_deal,menu_quote_history FROM products").fetchone()
                self.assertEqual(tuple(p[:5]),(50,0,None,0,0))
                self.assertEqual(len(json.loads(p[5])),0)  # Delivery unavailable: no payable-price history.
            build_web_db(file, Path(tmp)/'packed.db')
            with contextlib.closing(sqlite3.connect(Path(tmp)/'packed.db')) as packed:
                meta={k:json.loads(v) for k,v in packed.execute('SELECT key,value FROM metadata')}
                self.assertEqual(meta['active_30pct_drops'],0)
                self.assertEqual(meta['seven_day_reference_products'],0)

    def test_stale_discount_vanishes_from_next_batch_even_if_row_is_active(self):
        import json
        import msgpack
        import zstandard
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), contextlib.ExitStack() as stack:
            root = Path(tmp)
            conn = sqlite3.connect(root/'state.db')
            conn.row_factory = sqlite3.Row
            stack.callback(conn.close)
            sid = 'ffffffff-ffff-4fff-8fff-ffffffffffff'
            old_pid = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            live_pid = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
            original = document(sid, [old_pid, live_pid])
            original['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price'] = 180
            apply_snapshot(conn, [original], '20261025063000')
            dropped = document(sid, [old_pid, live_pid])
            dropped['hasMenu']['hasMenuSection'][0]['hasMenuItem'][0]['offers']['price'] = 90
            apply_snapshot(conn, [dropped], '20261026063000')
            self.assertEqual(conn.execute('SELECT is_price_drop FROM products WHERE product_uuid=?',
                                          (old_pid,)).fetchone()[0], 1)
            # The store is valid today, but the discounted item is missing
            # from the currently observed menu. Its old record stays active
            # for the normal multi-crawl missing/reappearance policy.
            apply_snapshot(conn, [document(sid, [live_pid])], '20261027063000')
            stale = conn.execute('SELECT status,is_price_drop,last_seen FROM products WHERE product_uuid=?',
                                 (old_pid,)).fetchone()
            self.assertEqual((stale['status'], stale['is_price_drop']), ('active', 1))
            self.assertEqual(stale['last_seen'], '2026-10-26T06:30:00+08:00')
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM product_search').fetchone()[0], 2)
            build_web_db(root/'state.db', root/'packed.db')
            with contextlib.closing(sqlite3.connect(root/'packed.db')) as packed:
                metadata = dict((k, json.loads(v)) for k,v in packed.execute('SELECT key,value FROM metadata'))
                self.assertEqual(metadata['active_products'], 2)
                self.assertEqual(metadata['latest_processed_at'], '2026-10-27T06:30:00+08:00')
                tokens = {}
                for payload, in packed.execute('SELECT payload FROM search_buckets'):
                    for term, refs in msgpack.unpackb(zstandard.ZstdDecompressor().decompress(payload), raw=False).items():
                        tokens.setdefault(term, []).extend(refs)
                self.assertEqual(tokens.get('f:catalog'), [0,1])  # Last-known menu remains searchable.
                self.assertFalse(any(key.startswith('f:discount:') for key in tokens))
                self.assertIsNotNone(conn.execute('SELECT 1 FROM products WHERE product_uuid=?',(old_pid,)).fetchone())

    def test_closed_store_keeps_last_known_catalog_but_not_current_deals(self):
        import json
        with tempfile.TemporaryDirectory() as tmp, contextlib.redirect_stdout(io.StringIO()), contextlib.ExitStack() as stack:
            root = Path(tmp)
            conn = sqlite3.connect(root/'state.db')
            conn.row_factory = sqlite3.Row
            stack.callback(conn.close)
            old_sid = 'ffffffff-ffff-4fff-8fff-ffffffffffff'
            fresh_sid = 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa'
            pid = 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb'
            apply_snapshot(conn, [document(old_sid,[pid])], '20261026063000')
            closed = document(old_sid,[pid])
            closed['isOpen'] = False
            apply_snapshot(conn, [closed, document(fresh_sid,[pid])], '20261027063000')
            build_web_db(root/'state.db',root/'packed.db')
            with contextlib.closing(sqlite3.connect(root/'packed.db')) as packed:
                meta = {k:json.loads(v) for k,v in packed.execute('SELECT key,value FROM metadata')}
                self.assertEqual(meta['active_stores'],2)
                self.assertEqual(meta['active_products'],2)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM product_search').fetchone()[0],2)


if __name__ == '__main__':
    unittest.main()
