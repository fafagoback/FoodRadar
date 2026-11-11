"""Fail-safe partial capture: known bad catalogs never erase good state."""
import hashlib
import io
import json
import sqlite3
import sys
import tarfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"src"))
sys.path.insert(0,str(ROOT/"tests"))

from pipeline import raw_manifest_should_replay
from raw_capture import documents_from_hf_raw
from test_packed_updates import apply_snapshot, document

SID_OK="11111111-1111-4111-8111-111111111111"
SID_BAD="22222222-2222-4222-8222-222222222222"
PID="33333333-3333-4333-8333-333333333333"
BATCH="20261111024503"
URL="https://www.ubereats.com/tw/store/test/"


def partial_manifest():
    good={"store_uuid":SID_OK,"store_url":URL+SID_OK,"success":True,
          "response_path":"stores/verified.body",
          "captured_at":"2026-11-11T02:45:03+08:00"}
    bad={"store_uuid":SID_BAD,"store_url":URL+SID_BAD,"success":False,
         "error":"Catalog native code 500 at offset 1020"}
    return {"format":"foodradar-immutable-http-body-v1",
            "batch_id":BATCH,"complete":False,"partial_replayable":True,
            "archive_path":f"Raw/{BATCH}/capture.tar.gz",
            "feed_pages":6,
            "stores":[good,bad],
            "successful_store_count":1,"failed_store_count":1,
            "failed_store_uuids":[SID_BAD],
            "responses":{}}


class PartialRawAndStateTests(unittest.TestCase):
    def test_partial_requires_explicit_validated_store_metadata(self):
        meta=partial_manifest()
        self.assertTrue(raw_manifest_should_replay(
            meta,f"Raw/{BATCH}/manifest.json","Raw"))
        with self.assertRaisesRegex(ValueError,"store count mismatch"):
            raw_manifest_should_replay(
                {**meta,"failed_store_count":2},
                f"Raw/{BATCH}/manifest.json","Raw")
        with self.assertRaisesRegex(ValueError,"Unclassified"):
            changed=partial_manifest()
            changed["stores"][1]["error"]="HTTP 429"
            raw_manifest_should_replay(
                changed,f"Raw/{BATCH}/manifest.json","Raw")

    def test_partial_raw_decodes_only_verified_store_and_keeps_bytes(self):
        meta=partial_manifest()
        original=b'{"status":"success","data":{"title":"Verified","catalogSectionsMap":{}}}'
        filename="stores/verified.body"
        meta["responses"]={filename:hashlib.sha256(original).hexdigest()}
        with tempfile.TemporaryDirectory() as tmp:
            tar=Path(tmp)/"native.tar.gz"
            with tarfile.open(tar,"w:gz") as out:
                info=tarfile.TarInfo(filename)
                info.size=len(original)
                out.addfile(info,io.BytesIO(original))
            with tarfile.open(tar,"r:gz") as archive:
                with patch("menu_core.convert_api_data_to_schema",
                           return_value={"name":"verified"}),\
                     patch("snapshot_validation.validate_document"):
                    docs=documents_from_hf_raw(archive,meta)
                self.assertEqual(archive.extractfile(filename).read(),original)
            self.assertEqual(len(docs),1)
            self.assertEqual(docs[0]["store_uuid"],SID_OK)

    def test_failed_store_previous_prices_and_presence_not_aged(self):
        with sqlite3.connect(":memory:") as db:
            db.row_factory=sqlite3.Row
            first=document(SID_BAD,[PID])
            apply_snapshot(db,[first],"20261110045438")
            store_before=dict(db.execute(
                "SELECT * FROM stores WHERE store_uuid=?",(SID_BAD,)).fetchone())
            product_before=dict(db.execute(
                "SELECT * FROM products WHERE store_uuid=? AND product_uuid=?",
                (SID_BAD,PID)).fetchone())
            apply_snapshot(db,[document(SID_OK,["new-sku"])],
                           "20261111024503",
                           excluded_store_uuids=[SID_BAD])
            store_after=dict(db.execute(
                "SELECT * FROM stores WHERE store_uuid=?",(SID_BAD,)).fetchone())
            product_after=dict(db.execute(
                "SELECT * FROM products WHERE store_uuid=? AND product_uuid=?",
                (SID_BAD,PID)).fetchone())
            self.assertEqual(store_before,store_after)
            self.assertEqual(product_before,product_after)
            self.assertEqual(db.execute(
                "SELECT value FROM metadata WHERE key='latest_batch'").fetchone()[0],
                "20261111024503")

    def test_excluded_store_must_not_also_have_verified_document(self):
        with sqlite3.connect(":memory:") as db:
            db.row_factory=sqlite3.Row
            with self.assertRaisesRegex(ValueError,"verified store"):
                apply_snapshot(db,[document(SID_BAD,[PID])],BATCH,
                               excluded_store_uuids=[SID_BAD])


if __name__=="__main__":
    unittest.main()
