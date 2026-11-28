"""Long-run storage must remove only unreferenced, expired identities."""
import sqlite3,sys,unittest
from pathlib import Path
from unittest.mock import Mock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from storage_lifecycle import _expired,prune_expired_entities
from compact_checkpoint import binary_uuid

OLD="2026-08-01T12:00:00+08:00"
NEW="2026-11-28T12:00:00+08:00"
CUTOFF="2026-09-29T12:00:00+08:00"
GONE="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SHOP="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
POLD="cccccccc-cccc-4ccc-8ccc-cccccccccccc"
PKEEP="dddddddd-dddd-4ddd-8ddd-dddddddddddd"

class Part:
    def __init__(self):
        self.conn=sqlite3.connect(":memory:")
        self.conn.execute("CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB)")
        self.conn.execute("CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB)")
        self.ids={GONE,SHOP}
        self.gone=({"store_uuid":GONE,"status":"inactive","last_seen":OLD},
                   [{"product_uuid":POLD,"status":"inactive","last_seen":OLD}],[])
        self.shop=({"store_uuid":SHOP,"status":"active","last_seen":NEW},
                   [{"product_uuid":POLD,"status":"inactive","last_seen":OLD},
                    {"product_uuid":PKEEP,"status":"inactive","last_seen":OLD}],
                   [("20261128",SHOP,PKEEP,NEW,5000,"NONE",1,0,1,5000,5000,"")])
        self.remaining=None
        for sid in self.ids:
            for table in ("shop_state","price_history"):
                self.conn.execute("INSERT INTO "+table+" VALUES(?,?)",(binary_uuid(sid),b"example"))
    def read_shop(self,sid):
        return self.gone if sid==GONE else self.shop
    def put_shop(self,store,products,quotes):
        self.remaining=(store,products,quotes)
    def flush(self):self.conn.commit()

class Collection:
    def __init__(self):
        self.part=Part()
        self.parts=[self.part]
        self.lookup={sid:self.part for sid in self.part.ids}
        self.meta={"price_history_cutoff":CUTOFF}
    def ids(self):return sorted(self.lookup)
    def flush(self):self.part.flush()

class ExpiryTest(unittest.TestCase):
    def test_inactive_stores_and_unreferenced_skus_expire(self):
        state=Collection()
        result=prune_expired_entities(state)
        self.assertEqual(result["removed_inactive_stores"],1)
        self.assertEqual(result["removed_inactive_products"],2)
        self.assertEqual(result["preserved_observations"],1)
        self.assertNotIn(GONE,state.lookup)
        self.assertEqual([p["product_uuid"] for p in state.part.remaining[1]],[PKEEP])
        self.assertEqual(len(state.part.remaining[2]),1)
        self.assertEqual(state.part.conn.execute(
            "SELECT count(*) FROM shop_state WHERE store=?",(binary_uuid(GONE),)).fetchone()[0],0)
        self.assertEqual(state.part.conn.execute(
            "SELECT count(*) FROM price_history WHERE store=?",(binary_uuid(GONE),)).fetchone()[0],0)
    def test_stale_evidence_does_not_expire_a_product_with_recent_price(self):
        state=Collection()
        state.part.shop[2].append(("20261128",SHOP,POLD,NEW,4000,"NONE",1,0,1,4000,4000,""))
        result=prune_expired_entities(state)
        self.assertEqual(result["removed_inactive_products"],1)
        # The active store needs NO rewrite: both its SKUs are protected
        # by current 60-day native price evidence.
        self.assertIsNone(state.part.remaining)
        self.assertIn(SHOP,state.lookup)
    def test_unknown_or_naive_timestamp_is_preserved(self):
        from datetime import datetime
        cutoff=datetime.fromisoformat(CUTOFF)
        self.assertFalse(_expired("",cutoff))
        self.assertFalse(_expired("2020-01-01T10:00:00",cutoff))
        self.assertFalse(_expired("garbage",cutoff))
        self.assertTrue(_expired(OLD,cutoff))

if __name__=="__main__":unittest.main()
