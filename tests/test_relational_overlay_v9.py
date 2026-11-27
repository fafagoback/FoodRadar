"""Independent tests for SQL overlay referential and byte-preserving behavior."""
import json
import sqlite3
import msgpack
import zstandard as zstd
import sys
import tempfile
import types
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from relational_overlay_v9 import build

BATCH="20261127143027"
def shop(sid,names,prices):
    store={"store_uuid":sid,"name":sid,"city":"台北市","locality":"信義區",
       "latitude":25.03,"longitude":121.56,"rating":4.2,"review_count":100,"status":"active"}
    ps=[{"store_uuid":sid,"product_uuid":sid+"-"+str(i),
         "product_name":n,"category":"主餐","description":"同規格",
         "price":price,"effective_price":price,"quantity":1,
         "status":"active","seven_day_is_price_deal":0,
         "seven_day_discount_pct":0,"promo_type":"NONE"}
        for i,(n,price) in enumerate(zip(names,prices),1)]
    qs=[(BATCH,sid,p["product_uuid"],"2026-11-27T14:30:27+08:00",
         int(p["price"]*100),"NONE",1,0,1,int(p["price"]*100),
         int(p["price"]*100),"") for p in ps]
    return store,ps,qs

class TestOverlay(unittest.TestCase):
    def setUp(self):
        temp=tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        root=Path(temp.name)
        self.original=root/"original.db"
        self.dest=root/"overlay.db"
        with sqlite3.connect(self.original) as db:
            db.executescript("""CREATE TABLE dictionary(key TEXT PRIMARY KEY,payload BLOB);
            CREATE TABLE shop_state(store BLOB PRIMARY KEY,payload BLOB);
            CREATE TABLE price_history(store BLOB PRIMARY KEY,payload BLOB);""")
            db.execute("INSERT INTO shop_state VALUES(?,?)",(b"A",b"original-unchanged"))
            db.execute("INSERT INTO price_history VALUES(?,?)",(b"A",b"history-unchanged"))
        a=shop("A",["商品1","商品2","商品3"],[100,200,300])
        b=shop("B",["商品2","商品3"],[200,300])
        self.items=[a,b]
        names=["商品1","商品2","商品3"]
        dictionaries={"product_name":names,"category":["主餐"],"description":["同規格"]}
        part=types.SimpleNamespace(path=self.original,d=dictionaries)
        self.state=types.SimpleNamespace(parts=[part],
            meta={"latest_batch":BATCH},
            shops=lambda:iter(self.items),
            stats=lambda:{"stores":2,"products":5,"observations":5})
    def test_shared_products_and_prices_and_exact_source(self):
        result=build(self.state,self.dest)
        self.assertEqual(result["definitions"],3)
        self.assertGreater(result["frontend_index_bytes"],0)
        self.assertEqual(result["links"],5)
        with sqlite3.connect(self.dest) as db:
            self.assertEqual(db.execute("SELECT count(*) FROM relational_prices").fetchone()[0],3)
            blob=db.execute("SELECT payload FROM relational_frontend_index WHERE key='shops'").fetchone()[0]
            rows=msgpack.unpackb(zstd.ZstdDecompressor().decompress(blob),raw=False)
            self.assertEqual(sum(len(s[11]) for s in rows),5)
            self.assertEqual(len(rows),2)
            self.assertEqual([x[1] for x in rows],["A","B"])
            self.assertEqual(db.execute("SELECT count(*) FROM relational_store_products").fetchone()[0],5)
            self.assertEqual(db.execute("SELECT payload FROM shop_state").fetchone()[0],b"original-unchanged")
            self.assertEqual(db.execute("SELECT payload FROM price_history").fetchone()[0],b"history-unchanged")
            self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0],"ok")
    def test_orphan_or_missing_tokens_fails_closed(self):
        self.items[1][1][0]["description"]="不同規格"
        with self.assertRaisesRegex(ValueError,"Missing checkpoint product token"):
            build(self.state,self.dest)
        self.assertFalse(self.dest.exists())
    def test_no_overwrite(self):
        self.dest.write_bytes(b"original")
        with self.assertRaises(FileExistsError):
            build(self.state,self.dest)
if __name__=="__main__":unittest.main()
