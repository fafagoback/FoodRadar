"""Parity of normalized working SQL, including subsequent crawl updates."""
import contextlib
import io
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from working_quotes import initialize
from test_packed_updates import apply_snapshot,document
from compact_checkpoint import create,expand
from verified_weekly import update_verified_weekly

class WorkingQuotesTest(unittest.TestCase):
    def test_same_values_and_next_crawl(self):
        with tempfile.TemporaryDirectory() as d,contextlib.redirect_stdout(io.StringIO()):
            p=Path(d); original=p/"old.db"
            sid="ffffffff-ffff-4fff-8fff-ffffffffffff"
            pid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
            for i,price in enumerate((100,150,100)):
                with sqlite3.connect(original) as db:
                    db.row_factory=sqlite3.Row
                    doc=document(sid,[pid])
                    doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["offers"]["price"]=price
                    apply_snapshot(db,[doc],f"202611{20+i}133219")
            create(original,p/"compact.db")
            for lean,name in ((False,"flat.db"),(True,"normalized.db")):
                expand(p/"compact.db",p/name,lean=lean)
                with sqlite3.connect(p/name) as db:
                    db.row_factory=sqlite3.Row
                    doc=document(sid,[pid])
                    doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["offers"]["price"]=75
                    apply_snapshot(db,[doc],"20261123133219")
            with sqlite3.connect(p/"flat.db") as a,sqlite3.connect(p/"normalized.db") as b:
                for t,order in (("products","store_uuid,product_uuid"),("stores","store_uuid"),
                                ("product_price_observations","batch_id,store_uuid,product_uuid")):
                    query=f"SELECT * FROM {t} ORDER BY {order}"
                    self.assertEqual(a.execute(query).fetchall(),b.execute(query).fetchall(),t)
                self.assertEqual(b.execute("SELECT COUNT(*) FROM events").fetchone()[0],0)
                self.assertEqual(b.execute("PRAGMA foreign_key_check").fetchall(),[])
    def test_upsert_does_not_replace_dictionary_identity(self):
        with sqlite3.connect(":memory:") as db:
            initialize(db)
            sql="INSERT OR REPLACE INTO product_price_observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?)"
            row=("20261123133219","s","p","2026-11-23T13:32:19+08:00",10000,"NONE",1,0,1,10000,10000,"original")
            db.execute(sql,row)
            db.execute(sql,("20261124133219",row[1],row[2],"2026-11-24T13:32:19+08:00",*row[4:]))
            db.execute(sql,(*row[:-1],"updated"))
            self.assertEqual(db.execute("SELECT source_hash FROM product_price_observations ORDER BY batch_id").fetchall(),[("updated",),("original",)])
            self.assertEqual(db.execute("SELECT COUNT(*) FROM work_quote_values").fetchone()[0],1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM work_quote_skus").fetchone()[0],1)
            self.assertEqual(db.execute("PRAGMA foreign_key_check").fetchall(),[])

if __name__=="__main__":unittest.main()
