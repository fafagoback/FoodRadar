import sqlite3,sys,tempfile,unittest
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/"src"))
from compact_payload import PayloadCodec,KEY,MAGIC
from compact_repack import build,digest,SCHEMA,benchmark
from compact_checkpoint import _pack
import zstandard as zstd
class TrainedPayloadTests(unittest.TestCase):
    def test_exact_legacy_and_trained_frames(self):
        samples=[f"same-product-description-category-{i}-branch-{i%10}".encode()*10 for i in range(250)]
        trained=zstd.train_dictionary(2048,samples).as_bytes()
        codec=PayloadCodec(trained);plain=PayloadCodec()
        value=["same-product-description-category-42-branch-2"]*20
        self.assertEqual(codec.unpack(codec.pack(value)),value)
        self.assertEqual(codec.unpack(plain.pack(value)),value)
        self.assertEqual(codec.unpack(MAGIC+codec.encoder.compress(codec.raw(plain.pack(value)))),value)
    def test_database_roundtrip_with_trained_dictionary(self):
        with tempfile.TemporaryDirectory() as t:
            root=Path(t);src=root/"src.db";dst=root/"dst.db"
            with sqlite3.connect(src) as db:
                db.executescript(SCHEMA)
                db.execute("INSERT INTO dictionary VALUES(?,?)",("test",_pack(["test"])))
                for i in range(150):
                    sid=i.to_bytes(16,"big")
                    db.execute("INSERT INTO shop_state VALUES(?,?)",(sid,_pack([[i],[[j,i,"same-category-description-"+str(j)] for j in range(150)]])))
                    db.execute("INSERT INTO price_history VALUES(?,?)",(sid,_pack([[j,[[[100,0,0,0,1,100,100],b'\x01']]] for j in range(100)])))
            info=build(src,dst,1024,train=2048)
            self.assertGreater(info["trained_dictionary_bytes"],0)
            self.assertEqual(digest(src),digest(dst))
            self.assertIsNotNone(benchmark(dst,1))
