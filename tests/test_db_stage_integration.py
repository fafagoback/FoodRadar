"""End-to-end offline checks for store result integrity and failed-wave takeover."""
from __future__ import annotations

import hashlib
import json
import sys
import tempfile
import unittest
from pathlib import Path

import zstandard as zstd

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"scripts"))
from db_stage_merge import read_verified_documents


def write_fixture(base,artifact,rows):
    folder=base/artifact
    folder.mkdir(parents=True,exist_ok=True)
    payloads={}
    with (folder/"documents.jsonl.zst").open("wb") as output:
        with zstd.ZstdCompressor(level=3).stream_writer(output,closefd=False) as writer:
            for uuid in rows:
                raw=(json.dumps({"store_uuid":uuid,
                                 "document":{"store_uuid":uuid,"batch_id":"20261126072649"}},
                                ensure_ascii=False,separators=(",",":"))+"\n").encode("utf-8")
                writer.write(raw)
                payloads[uuid]=hashlib.sha256(raw).hexdigest()
    return payloads


class StageMergeTests(unittest.TestCase):
    def test_verify_exact_per_store_sha(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            hashes=write_fixture(path,"db-worker-123-01",["one","two"])
            results=[{"store_uuid":sid,"status":"completed",
                      "result":{"artifact":"db-worker-123-01","sha256":sha}}
                     for sid,sha in hashes.items()]
            spool,failures=read_verified_documents(path,results)
            try:
                self.assertEqual(len(spool),2)
                self.assertEqual({doc["store_uuid"] for doc in spool},{"one","two"})
                self.assertEqual(failures,[])
            finally:
                spool.close()

    def test_ignores_uncommitted_retry_artifact(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            write_fixture(path,"db-worker-123-01",["one"])
            hashes=write_fixture(path,"db-worker-123-01-w01",["one"])
            result=[{"store_uuid":"one","status":"completed",
                     "result":{"artifact":"db-worker-123-01-w01",
                               "sha256":hashes["one"]}}]
            spool,_=read_verified_documents(path,result)
            try:self.assertEqual(len(spool),1)
            finally:spool.close()

    def test_rejects_tampered_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            write_fixture(path,"db-worker-123-01",["one"])
            rows=[{"store_uuid":"one","status":"completed",
                   "result":{"artifact":"db-worker-123-01",
                             "sha256":"0"*64}}]
            with self.assertRaises(ValueError):
                read_verified_documents(path,rows)

    def test_rejects_missing_record(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)
            write_fixture(path,"db-worker-123-01",["one"])
            rows=[{"store_uuid":"two","status":"completed",
                   "result":{"artifact":"db-worker-123-01",
                             "sha256":"0"*64}}]
            with self.assertRaises(RuntimeError):
                read_verified_documents(path,rows)


if __name__=="__main__":
    unittest.main()
