"""Actual blob decoding and excluded-batch store history acceptance."""
import base64
import hashlib
import io
import json
import sys
import unittest
from pathlib import Path

import msgpack
import zstandard

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"scripts"))
from publish_turso_delta import decode_turso_cell
from audit_removed_batch import verify_target_bundles

class RemovedBatchAudit(unittest.TestCase):
    def make_bundle(self, time):
        pcols=["product_name","menu_quote_history"]
        ecols=["event_time","event_type"]
        data={"product_columns":pcols,"event_columns":ecols,
              "products":[[0,["ARIEL抗菌洗衣精800g瓶裝-高效除蟎",
                               json.dumps([[time,89,89,"直接折價",1]])]]],
              "events":[[time,"PRICE_CHANGED"]]}
        body=msgpack.packb(data,use_bin_type=True)
        return {"payload":zstandard.ZstdCompressor().compress(body),
                "checksum":hashlib.sha256(body).hexdigest()}

    def test_decode_binary_turso_cell(self):
        value=b"\x00\xfefoodradar"
        self.assertEqual(decode_turso_cell({"type":"blob","base64":base64.b64encode(value).decode()}),value)
        self.assertEqual(decode_turso_cell({"type":"integer","value":"23"}),23)
        self.assertIsNone(decode_turso_cell({"type":"null"}))

    def test_excluded_observations_fail(self):
        excluded="2026-11-13T11:06:57+08:00"
        with self.assertRaisesRegex(ValueError,"still appears"):
            verify_target_bundles([self.make_bundle(excluded)], excluded)
        valid=verify_target_bundles(
            [self.make_bundle("2026-11-14T10:56:31+08:00")],excluded)
        self.assertEqual(valid["matching_products"],1)
        self.assertEqual(valid["excluded_quote_points"],0)
