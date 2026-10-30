"""Static exported deals must have same native proof as Turso."""
import json
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
from test_packed_updates import apply_snapshot,document
from export_verified_public import export_verified_public

SID="ffffffff-ffff-4fff-8fff-ffffffffffff"
A="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
B="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


class V2PublicExport(unittest.TestCase):
    def test_export_publishes_only_native_same_sku_bogo(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/"serving.db"
            with sqlite3.connect(db) as con:
                con.row_factory=sqlite3.Row
                first=document(SID,[A,B])
                apply_snapshot(con,[first],"20261029063000")
                current=document(SID,[A,B])
                items=current['hasMenu']['hasMenuSection'][0]['hasMenuItem']
                items[0]["test_native_bogo"]=True
                items[1]["test_native_boga"]=True
                apply_snapshot(con,[current],"20261030063000")
            site=Path(tmp)/"site"
            result=export_verified_public(db,site,"20261030063000")
            self.assertEqual(result["promotions"],1)
            self.assertEqual(result["discounts"],1)
            promos=json.loads((site/"promotions.json").read_text())["items"]
            deals=json.loads((site/"discounts.json").read_text())["items"]
            self.assertEqual(promos[0]["product_id"],A)
            self.assertEqual(deals[0]["current_price"],50)
            self.assertEqual(deals[0]["discount_pct"],50)
            self.assertFalse(any(d["product_id"]==B for d in deals+promos))
            self.assertEqual(json.loads((site/"history.json").read_text())["history"],{})

    def test_export_rejects_unmatched_batch(self):
        with tempfile.TemporaryDirectory() as tmp:
            db=Path(tmp)/"serving.db"
            with sqlite3.connect(db) as con:
                con.row_factory=sqlite3.Row
                apply_snapshot(con,[document(SID,[A])],"20261029063000")
            with self.assertRaises(ValueError):
                export_verified_public(db,Path(tmp)/"site","20261030063000")


if __name__=="__main__":
    unittest.main()
