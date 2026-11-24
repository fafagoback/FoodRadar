"""No Turso bundles in a production Pages build; preserve every published card."""
import contextlib
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"scripts"))
sys.path.insert(0,str(ROOT/"tests"))
from build_web_db import build_web_db
from test_packed_updates import apply_snapshot,document
from verify_packed_local import verify as verify_projection

class StaticProjectionParity(unittest.TestCase):
    def test_all_cards_identical_without_turso_bundles(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            origin=root/"origin.db"
            with sqlite3.connect(origin) as db:
                db.row_factory=sqlite3.Row
                sid="ffffffff-ffff-4fff-8fff-ffffffffffff"
                pid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
                second="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
                for batch,price in [("20261120133219",100),("20261121133219",140),("20261122133219",85)]:
                    doc=document(sid,[pid,second])
                    doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["offers"]["price"]=price
                    apply_snapshot(db,[doc],batch)
            with contextlib.redirect_stdout(__import__("io").StringIO()):
                build_web_db(origin,root/"full.db")
                build_web_db(origin,root/"static.db",static_only=True)
            with sqlite3.connect(root/"full.db") as a,sqlite3.connect(root/"static.db") as b:
                for table in ("web_products","store_directory"):
                    cols=[r[1] for r in a.execute(f"PRAGMA table_info({table})")]
                    if table=="store_directory":
                        cols.remove("chunk_count")
                    sql="SELECT "+",".join(cols)+f" FROM {table} ORDER BY 1"
                    self.assertEqual(a.execute(sql).fetchall(),b.execute(sql).fetchall())
                self.assertGreater(a.execute("SELECT COUNT(*) FROM store_bundles").fetchone()[0],0)
                self.assertEqual(b.execute("SELECT COUNT(*) FROM store_bundles").fetchone()[0],0)
                m1={k:json.loads(v) for k,v in a.execute("SELECT key,value FROM metadata")}
                m2={k:json.loads(v) for k,v in b.execute("SELECT key,value FROM metadata")}
                for key in ("active_products","active_platform_promotions","active_30pct_drops",
                            "current_verified_products","seven_day_reference_products"):
                    self.assertEqual(m1[key],m2[key],key)
            result=verify_projection(root/"static.db")
            self.assertEqual(result["projection"],"static-only")
            self.assertEqual(result["web_products"],2)

if __name__=="__main__":
    unittest.main()
