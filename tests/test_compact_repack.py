import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"src"))
from compact_repack import SCHEMA, digest, select, build, benchmark
from compact_checkpoint import _pack, _unpack
from compact_direct import _same_wire
import zstandard as zstd


class RepackTests(unittest.TestCase):
    def setUp(self):
        t=tempfile.TemporaryDirectory()
        self.addCleanup(t.cleanup)
        self.root=Path(t.name)
        self.source=self.root/"source.db"
        with sqlite3.connect(self.source) as db:
            db.execute("PRAGMA page_size=1024")
            db.executescript(SCHEMA)
            db.execute("INSERT INTO dictionary VALUES(?,?)",("test",_pack(["description"]*200)))
            for i in range(30):
                key=i.to_bytes(16,"big")
                db.execute("INSERT INTO shop_state VALUES(?,?)",(key,_pack([[i],[[i,j,True] for j in range(100)]])))
                db.execute("INSERT INTO price_history VALUES(?,?)",(key,_pack([[j,[[[10,0,0,0,1,10,10],b'\x01']]] for j in range(100)])))

    def test_physical_codec_round_trip(self):
        candidate=self.root/"direct.db"
        build(self.source,candidate,512)
        self.assertEqual(digest(candidate),digest(self.source))
        self.assertIsNotNone(benchmark(candidate,1))
        with sqlite3.connect(candidate) as db:
            self.assertEqual(db.execute("PRAGMA page_size").fetchone()[0],512)

    def test_recurring_cross_shop_normalization_is_lossless(self):
        # Three shops reference the same 100 goods and independent prices.
        # A fresh checkpoint can encode shared content in a single template
        # pool; a later run can reuse the same pool without recrawling.
        from compact_templates import Templates
        from compact_direct import CompactStore
        ids=[bytes([i])*16 for i in (1,2,3)]
        names=[f"共同商品{i}" for i in range(100)]
        with sqlite3.connect(self.source) as db:
            db.execute("DELETE FROM shop_state")
            db.execute("DELETE FROM price_history")
            dicts={"__store_columns":["name"],
                   "__product_columns":["product_name","category","description","product_uuid"],
                   "product_name":names,"category":["日用品"],
                   "description":["共同規格"]}
            for k,v in dicts.items():
                db.execute("INSERT INTO dictionary VALUES(?,?)",(k,_pack(v)))
            for j,key in enumerate(ids):
                rows=[[i,0,0,bytes([j+1,i+1])] for i in range(100)]
                db.execute("INSERT INTO shop_state VALUES(?,?)",
                           (key,_pack([[j],rows])))
                db.execute("INSERT INTO price_history VALUES(?,?)",
                           (key,_pack([[i,[[[10000+j*100,0,1,0,1,10000+j*100,10000+j*100],
                                             b'\\x03']]] for i in range(100)])))
        source_sha=digest(self.source)
        candidate=self.root/"normalized.db"
        info=build(self.source,candidate,1024,levels=(12,),share=True)
        self.assertGreater(info["shared_groups"],0)
        self.assertEqual(digest(candidate),source_sha)
        with sqlite3.connect(candidate) as db:
            data={k:_unpack(v) for k,v in db.execute("SELECT key,payload FROM dictionary")}
            codec=Templates(data)
            self.assertEqual(len(codec.pool),100)
            raw=zstd.ZstdDecompressor()
            for key in ids:
                payload=db.execute("SELECT payload FROM shop_state WHERE store=?",(key,)).fetchone()[0]
                header,rows=__import__("msgpack").unpackb(raw.decompress(payload),raw=False)
                self.assertEqual(len(rows),100)
                self.assertEqual(len(rows[0]),2) # shared-content reference + local uuid
                self.assertEqual(codec.rows(rows)[0][:3],[0,0,0])
        # Physical sizes can vary on tiny fixtures; only canonical identities
        # and exact price evidence are required for this proof.

    def test_selection_does_not_touch_source_and_rejects_slower_candidate(self):
        original=self.source.read_bytes()
        def measure(path):
            return {"scan_ms":1 if Path(path)==self.source else 2}
        chosen,report=select(self.source,self.root,measure=measure)
        self.assertIsNone(chosen)
        self.assertTrue(all(not x["read_gate_pass"] for x in report["candidates"]))
        self.assertEqual(self.source.read_bytes(),original)

    def test_production_stage_17_never_bypasses_persistent_normalization(self):
        stage=(Path(__file__).resolve().parents[1]/"scripts"/"db_stage_merge.py").read_text()
        self.assertIn("save_direct(state,path,optimize=True)",stage)
        self.assertNotIn("save_direct(state,path,optimize=False)",stage)

    def test_recompressed_payload_not_needlessly_rewritten(self):
        a=_pack([True,1,"abc"]*100)
        b=zstd.ZstdCompressor(level=16).compress(zstd.ZstdDecompressor().decompress(a))
        self.assertTrue(_same_wire(a,b))
        self.assertFalse(_same_wire(a,_pack([1,True,"abc"]*100)))


if __name__=="__main__":unittest.main()
