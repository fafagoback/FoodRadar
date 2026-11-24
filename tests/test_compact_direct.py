"""Exercise direct compact mutation, including missing/excluded stores."""
import copy,contextlib,io,json,sqlite3,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/"src"),str(ROOT/"scripts")]
from compact_direct import CompactStore,CompactCollection
from compact_checkpoint import create
from serving_state import apply_snapshot
from verified_weekly import update_verified_weekly
from verified_prices import quote_from_native_item
from test_packed_updates import document
from prototype_pack_db import build as pack
from build_static_catalog import build as publish
from audit_deal_pipeline import report

SIDS=["ffffffff-ffff-4fff-8fff-ffffffffffff","bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
      "cccccccc-cccc-4ccc-8ccc-cccccccccccc"]
PID="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
def doc(sid,price,*,bogo=False):
    value=document(sid,[PID])
    item=value["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]
    item["offers"]["price"]=price
    raw={"uuid":PID,"title":item["name"],"price":price*100,"catalogItemAnalyticsData":{"promoType":None}}
    if bogo:
        raw["catalogItemAnalyticsData"]["promoType"]="BOGO"
        raw["itemPromotion"]={"type":"buyXGetYItemPromotion","buyXGetYItemPromotion":{"buyQuantity":1,"getQuantity":1}}
    item["verified_price"]=quote_from_native_item(raw)
    return value

class DirectCompactTest(unittest.TestCase):
    def compare(self,collection,legacy,root):
        counts=collection.stats()
        with sqlite3.connect(legacy) as db:
            db.row_factory=sqlite3.Row
            self.assertEqual(counts["stores"],db.execute("SELECT COUNT(*) FROM stores").fetchone()[0])
            self.assertEqual(counts["products"],db.execute("SELECT COUNT(*) FROM products").fetchone()[0])
            self.assertEqual(counts["observations"],db.execute("SELECT COUNT(*) FROM product_price_observations").fetchone()[0])
            self.assertEqual(collection.batch_rows(),[tuple(r) for r in db.execute("SELECT * FROM crawl_batches ORDER BY batch_id")])
            for s in collection.stores():
                original=dict(db.execute("SELECT * FROM stores WHERE store_uuid=?",(s["store_uuid"],)).fetchone())
                self.assertEqual(s,original)
                _,products,quotes=collection.shop(s["store_uuid"],derive_history=True)
                expected=[dict(r) for r in db.execute("SELECT * FROM products WHERE store_uuid=? ORDER BY product_uuid",(s["store_uuid"],))]
                for p in expected:p["price_evidence_source_hash"]=None
                for p in products+expected:
                    p["catalog_web"]=json.loads(p["catalog_web"]);p["menu_quote_history"]=json.loads(p["menu_quote_history"])
                self.assertEqual(products,expected)
                old=[tuple(r)[:-1] for r in db.execute("SELECT * FROM product_price_observations WHERE store_uuid=? ORDER BY product_uuid,observed_at,batch_id",(s["store_uuid"],))]
                self.assertEqual([q[:-1] for q in quotes],old)
        self.assertEqual(report(legacy),report(direct=collection))
        pack(legacy,root/"packed.db",static_only=True)
        orig=publish(root/"packed.db",legacy,root/"old")
        projection=pack(None,None,static_only=True,direct=collection)
        new=publish(None,None,root/"new",direct=collection,projection=projection)
        self.assertEqual(orig["files"],new["files"])
        self.assertEqual(orig["summary"],new["summary"])
        self.assertEqual(orig["counts"],new["counts"])
    def test_incremental_and_missing_excluded_new_store(self):
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            root=Path(tmp);legacy=root/"legacy.db"
            collection=CompactCollection([CompactStore(root/"direct.db",empty=True)])
            try:
                for day,docs,excluded in [
                    (20,[doc(SIDS[0],100),doc(SIDS[1],100)],[]),
                    (21,[doc(SIDS[0],150),doc(SIDS[1],100,bogo=True)],[]),
                    (22,[doc(SIDS[0],100)], [SIDS[1]]),
                    (23,[doc(SIDS[0],100),doc(SIDS[2],80)], []),
                    (24,[doc(SIDS[0],75)], []),
                ]:
                    batch=f"202611{day}133219"
                    with sqlite3.connect(legacy) as db:
                        db.row_factory=sqlite3.Row
                        expected=apply_snapshot(db,copy.deepcopy(docs),batch,excluded_store_uuids=excluded,refresh_search=False)
                        week=update_verified_weekly(db,batch)
                    actual=collection.apply_batch(copy.deepcopy(docs),batch,excluded)
                    self.assertTrue(all(actual.get(k,0)==v for k,v in expected.items()),(actual,expected))
                    self.assertEqual(collection.refresh_weekly(batch),week)
                self.compare(collection,legacy,root)
                # Prove absence of the retired database, rather than a hard-coded zero.
                self.assertFalse((root/"serving.db").exists())
                self.assertFalse((root/"raw-only-reconstructed.db").exists())
                self.assertFalse((root/"packed-serving.db").exists())
            finally:collection.close()

if __name__=="__main__":unittest.main()
