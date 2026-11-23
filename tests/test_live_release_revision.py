"""Same latest batch does not prove the deleted HF Raw history reached Turso."""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from audit_live_turso import verify_release_identity

class VerifiedLiveRevision(unittest.TestCase):
    def test_replay_revision_must_match_not_just_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/"packed.db"
            with sqlite3.connect(path) as db:
                db.execute("CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT)")
                db.executemany("INSERT INTO metadata VALUES(?,?)",[
                    ("latest_batch",json.dumps("20261118033731")),
                    ("data_revision",json.dumps("sha-new")),
                ])
            self.assertEqual(
                verify_release_identity(
                    {"latest_batch":"20261118033731","data_revision":"sha-new"},
                    path,"20261118033731"),
                "sha-new")
            with self.assertRaisesRegex(ValueError,"release mismatch"):
                verify_release_identity(
                    {"latest_batch":"20261118033731","data_revision":"sha-old"},
                    path,"20261118033731")
            with self.assertRaisesRegex(ValueError,"release mismatch"):
                verify_release_identity(
                    {"latest_batch":"20261117033731","data_revision":"sha-new"},
                    path,"20261118033731")
