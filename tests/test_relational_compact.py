"""Test shared SQL products/prices and round-trip compressed branch history."""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from relational_compact import build,decompress

BATCH="20261127143027"
TIME="2026-11-27T14:30:27+08:00"
def sample(store,names,prices,bonus=0):
    s={"store_uuid":store,"name":store,"city":"台北市","locality":"大安區",
       "latitude":25.0,"longitude":121.5,"status":"active","is_open":1}
    products=[{"store_uuid":store,"product_uuid":store+"-"+str(i),
       "product_name":name,"category":"主餐","description":"共同規格",
       "price":price,"effective_price":price,"quantity":1,
       "status":"active","delivery_available":1,
       "discount_pct":bonus,"seven_day_discount_pct":bonus,
       "seven_day_is_price_deal":1 if bonus else 0,"foo":"額外資訊"}
       for i,(name,price) in enumerate(zip(names,prices))]
    quotes=[(BATCH,store,p["product_uuid"],TIME,int(p["price"]*100),
        "NONE",1,0,1,int(p["price"]*100),int(p["price"]*100),"hash")
        for p in products]
    return s,products,quotes

class TestCompactRelational(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.path=Path(tmp.name)/"test.db"
    def run_build(self,rows):
        result=build(rows,self.path,BATCH)
        db=sqlite3.connect(self.path)
        self.addCleanup(db.close)
        return result,db
    def test_five_branches_three_unique_products(self):
        result,db=self.run_build([
            sample("A",["商品1","商品2","商品3"],[100,200,300]),
            sample("B",["商品2","商品3"],[200,300])])
        self.assertEqual(result["stores"],2)
        self.assertEqual(result["products"],3)
        self.assertEqual(result["store_products"],5)
        self.assertEqual(result["observations"],5)
        self.assertEqual(db.execute("SELECT count(*) FROM store_aux").fetchone()[0],2)
        self.assertEqual(db.execute("SELECT count(*) FROM price_terms").fetchone()[0],6)
        self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0],"ok")
        s=db.execute("SELECT product_state,history FROM store_aux ORDER BY store_id").fetchall()
        self.assertEqual(len(decompress(s[0][0])[1]),3)
        self.assertEqual(len(decompress(s[1][0])[1]),2)
        self.assertEqual(sum(len(e) for _,e in decompress(s[0][1])),3)
        self.assertEqual(sum(len(e) for _,e in decompress(s[1][1])),2)
    def test_price_diff_does_not_merge(self):
        _,db=self.run_build([sample("A",["商品2"],[200]),
                              sample("B",["商品2"],[225])])
        self.assertEqual(db.execute("SELECT count(*) FROM products").fetchone()[0],1)
        self.assertEqual(len(db.execute("""SELECT t.menu FROM store_products x
                JOIN price_terms t ON x.price_id=t.id
                ORDER BY x.store_id""").fetchall()),2)
    def test_observation_orphans_abort(self):
        store,items,quotes=sample("A",["商品"],[100])
        quotes.append((BATCH,"A","not-found",TIME,10000,"NONE",1,0,1,10000,10000,""))
        with self.assertRaisesRegex(ValueError,"Orphan"):
            build([(store,items,quotes)],self.path,BATCH)
        self.assertFalse(self.path.exists())
    def test_no_existing_file_overwrite(self):
        self.run_build([sample("A",["商品"],[100])])
        with self.assertRaises(FileExistsError):
            build([sample("A",["商品"],[100])],self.path,BATCH)
    def test_60_day_expiry(self):
        store,items,quotes=sample("A",["商品"],[100])
        old=list(quotes[0])
        old[0]="20250101000000"
        old[3]="2025-01-01T00:00:00+08:00"
        result,db=self.run_build([(store,items,[quotes[0],tuple(old)])])
        self.assertEqual(result["expired_observations_ignored"],1)
        self.assertEqual(result["observations"],1)
        self.assertEqual(sum(len(e) for _,e in decompress(
            db.execute("SELECT history FROM store_aux").fetchone()[0])),1)
if __name__=="__main__":unittest.main()
