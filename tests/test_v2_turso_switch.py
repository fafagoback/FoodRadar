"""V1 -> V2 Turso reset must require verified full-raw replay proof."""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'scripts'))
from test_turso_delta import SQLiteRemote
from prototype_pack_db import SCHEMA
from publish_turso_delta import publish

class VersionSwitchTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.remote=SQLiteRemote()
        self.addCleanup(self.remote.conn.close)
        self.old=Path(self.tmp.name)/"old.db"
        self.new=Path(self.tmp.name)/"new.db"
        with sqlite3.connect(self.old) as con:
            con.executescript(SCHEMA)
            con.executemany("INSERT INTO metadata VALUES(?,?)",[
                ("application",'"FoodRadar"'),
                ("schema_version","1"),
                ("latest_batch",'"20261029063000"'),
                ("source_counts",'{"stores":1,"products":1,"crawl_batches":1}')
            ])
        with sqlite3.connect(self.new) as con:
            con.executescript(SCHEMA)
            con.executemany("INSERT INTO metadata VALUES(?,?)",[
                ("application",'"FoodRadar"'),
                ("schema_version","2"),
                ("latest_batch",'"20261030063000"'),
                ("source_counts",'{"stores":1,"products":1,"crawl_batches":2}')
            ])
        publish(self.old,self.remote,True)
        self.proof=Path(self.tmp.name)/"verified-rebuild.json"
        self.proof.write_text(json.dumps({
            "format":"foodradar-full-raw-replay-v2",
            "batch_id":"20261030063000",
            "verified_manifest_count":2,
            "manifest_digest":"f"*64,
            "source_counts":{"stores":1,"products":1,"crawl_batches":2}
        }))

    def test_v1_to_v2_without_attestation_fails_without_writes(self):
        with self.assertRaises(ValueError):
            publish(self.new,self.remote,True,reset_all=True)
        self.assertEqual(
            self.remote.query("SELECT value FROM metadata WHERE key='schema_version'")[0]["value"],"1")

    def test_v1_to_v2_with_full_attestation_is_transactional(self):
        result=publish(self.new,self.remote,True,
                       verified_rebuild_manifest=self.proof,reset_all=True)
        self.assertTrue(result["applied"])
        self.assertTrue(result["verified_full_raw_rebuild"])
        self.assertEqual(
            self.remote.query("SELECT value FROM metadata WHERE key='schema_version'")[0]["value"],"2")
        self.assertEqual(
            self.remote.query("SELECT value FROM metadata WHERE key='latest_batch'")[0]["value"],'"20261030063000"')


if __name__=="__main__":
    unittest.main()
