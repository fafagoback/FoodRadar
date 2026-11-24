import gzip
import hashlib
import io
import sqlite3
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
from raw_capture import open_seekable_raw_archive
from canonical_history import quote_points

class RawReplayTests(unittest.TestCase):
    def test_backward_member_reads_identical_and_original_unchanged(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/"archive.tar.gz"
            entries={"z/body.json":b'{"price":100}',"a/body.json":b'{"price":80}'}
            with tarfile.open(p,"w:gz") as tf:
                for name,body in entries.items():
                    info=tarfile.TarInfo(name)
                    info.size=len(body)
                    tf.addfile(info,io.BytesIO(body))
            original=hashlib.sha256(p.read_bytes()).hexdigest()
            with open_seekable_raw_archive(p) as tf:
                for name in ["a/body.json","z/body.json","a/body.json"]:
                    self.assertEqual(tf.extractfile(name).read(),entries[name])
            self.assertEqual(hashlib.sha256(p.read_bytes()).hexdigest(),original)

    def test_quote_history_keeps_exact_promotion_and_times(self):
        with sqlite3.connect(":memory:") as db:
            db.execute("""CREATE TABLE product_price_observations(
                store_uuid TEXT,product_uuid TEXT,observed_at TEXT,batch_id TEXT,
                menu_price_cents INTEGER,promotion_type TEXT,buy_quantity INTEGER,
                free_quantity INTEGER,received_quantity INTEGER,payable_total_cents INTEGER)""")
            db.executemany("INSERT INTO product_price_observations VALUES(?,?,?,?,?,?,?,?,?,?)",[
                ("s","p","2026-11-23T18:30:00+08:00","a",10000,"SAME_SKU_BXGY",1,1,2,10000),
                ("s","p","2026-11-24T06:30:00+08:00","b",10000,"NONE",1,0,1,10000)])
            self.assertEqual([r[2] for r in quote_points(db)],[
                ["2026-11-23T18:30:00+08:00",50.0,100.0,"買1送1",2],
                ["2026-11-24T06:30:00+08:00",100.0,100.0,"無",1]])

if __name__=="__main__":
    unittest.main()
