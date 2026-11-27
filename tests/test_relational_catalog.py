"""Shared catalog, branch-specific price, SQL and history regression tests.

All tests run offline; no UE or Hugging Face access is required.
"""
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from relational_catalog import build

BATCH="20261127071423"
AT="2026-11-27T07:14:23+08:00"

def shop(code,names,prices):
    store={"store_uuid":code,"name":code,"city":"台北市","locality":"信義區",
           "latitude":25.0,"longitude":121.5,"status":"active","is_open":1}
    products=[{
        "store_uuid":code,"product_uuid":f"{code}-{i}",
        "product_name":name,"category":"主餐","description":"同款同規格",
        "price":price,"effective_price":price,"quantity":1,
        "is_open":1,"status":"active","delivery_available":1,
        "discount_pct":0,"is_price_deal":0,"is_price_drop":0,
        "seven_day_is_price_deal":0
    } for i,(name,price) in enumerate(zip(names,prices),1)]
    quotes=[(BATCH,code,p["product_uuid"],AT,
             round(p["price"]*100),"NONE",1,0,1,
             round(p["price"]*100),round(p["price"]*100),"") for p in products]
    return store,products,quotes

class RelationalCatalogTest(unittest.TestCase):
    def setUp(self):
        directory=tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.path=Path(directory.name)/"catalog.db"

    def create(self,records):
        result=build(records,self.path,BATCH)
        db=sqlite3.connect(self.path)
        self.addCleanup(db.close)
        return result,db

    def test_shared_three_products_five_links(self):
        result,db=self.create([
            shop("A",["商品1","商品2","商品3"],[100,200,300]),
            shop("B",["商品2","商品3"],[200,300])])
        self.assertEqual(result["products"],3)
        self.assertEqual(result["store_products"],5)
        self.assertEqual(result["observations"],5)
        self.assertEqual(db.execute(
            'SELECT COUNT(*) FROM price_terms WHERE kind="CURRENT"').fetchone()[0],3)
        rows=db.execute("""SELECT s.uuid,p.product_name
            FROM store_products x JOIN stores s ON x.store_id=s.id
            JOIN products p ON p.id=x.product_id
            ORDER BY s.uuid,p.product_name""").fetchall()
        self.assertEqual(rows,[("A","商品1"),("A","商品2"),("A","商品3"),
                               ("B","商品2"),("B","商品3")])
        self.assertEqual(db.execute(
            'SELECT COUNT(*) FROM product_fts WHERE product_fts MATCH "商品2"'
        ).fetchone()[0],1)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM store_geo WHERE min_lat BETWEEN 24 AND 26"
        ).fetchone()[0],2)
        self.assertEqual(db.execute("PRAGMA integrity_check").fetchone()[0],"ok")

    def test_different_branch_price_does_not_merge(self):
        result,db=self.create([
            shop("A",["商品2"],[200]),shop("B",["商品2"],[225])])
        rows=db.execute("""SELECT s.uuid,t.price_cents
            FROM store_products x JOIN stores s ON x.store_id=s.id
            JOIN price_terms t ON t.id=x.price_term_id ORDER BY s.uuid""").fetchall()
        self.assertEqual(rows,[("A",20000),("B",22500)])
        self.assertEqual(result["products"],1)
        self.assertEqual(result["observations"],2)

    def test_same_name_different_spec_is_distinct(self):
        a=shop("A",["飲料"],[40])
        b=shop("B",["飲料"],[40])
        b[1][0]["description"]="大杯"
        result,_=self.create([a,b])
        self.assertEqual(result["products"],2)

    def test_orphan_price_aborts_without_published_database(self):
        item=shop("A",["商品1"],[100])
        item[2].append((BATCH,"A","missing",AT,10000,"NONE",1,0,1,10000,10000,""))
        with self.assertRaisesRegex(ValueError,"Orphan"):
            build([item],self.path,BATCH)
        self.assertFalse(self.path.exists())

    def test_existing_candidate_is_not_overwritten(self):
        self.create([shop("A",["商品1"],[100])])
        with self.assertRaises(FileExistsError):
            build([shop("A",["商品1"],[100])],self.path,BATCH)

    def test_sixty_day_retention(self):
        store,products,quotes=shop("A",["商品1"],[100])
        old=list(quotes[0])
        old[0]="20260101000000"
        old[3]="2026-01-01T00:00:00+08:00"
        result,db=self.create([(store,products,[quotes[0],tuple(old)])])
        self.assertEqual(result["expired_observations_ignored"],1)
        self.assertEqual(result["observations"],1)
        self.assertEqual(db.execute(
            "SELECT COUNT(*) FROM price_observations").fetchone()[0],1)

if __name__=="__main__":unittest.main()
