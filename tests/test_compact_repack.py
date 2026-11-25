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

    def test_selection_does_not_touch_source_and_rejects_slower_candidate(self):
        original=self.source.read_bytes()
        def measure(path):
            return {"scan_ms":1 if Path(path)==self.source else 2}
        chosen,report=select(self.source,self.root,measure=measure)
        self.assertIsNone(chosen)
        self.assertTrue(all(not x["read_gate_pass"] for x in report["candidates"]))
        self.assertEqual(self.source.read_bytes(),original)

    def test_recompressed_payload_not_needlessly_rewritten(self):
        a=_pack([True,1,"abc"]*100)
        b=zstd.ZstdCompressor(level=16).compress(zstd.ZstdDecompressor().decompress(a))
        self.assertTrue(_same_wire(a,b))
        self.assertFalse(_same_wire(a,_pack([1,True,"abc"]*100)))


if __name__=="__main__":unittest.main()
