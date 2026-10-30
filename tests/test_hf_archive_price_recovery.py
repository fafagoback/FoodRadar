"""Legacy Json-only archives may not reconstruct unverified discounts."""
import sqlite3
import sys
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'src'))
sys.path.insert(0,str(ROOT/'scripts'))
from test_packed_updates import document,apply_snapshot
from restore_hf_menu_quotes import (
    canonical_menu_quotes,apply_archival_quote_references,
    restore_archive_price_references)
from raw_price_enrichment import enrich_converted_with_native

SID="ffffffff-ffff-4fff-8fff-ffffffffffff"
PID="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


class ArchiveQuotePriceTests(unittest.TestCase):
    def setUp(self):
        self.conn=sqlite3.connect(":memory:")
        self.conn.row_factory=sqlite3.Row
        self.addCleanup(self.conn.close)
        self.doc=document(SID,[PID])
        apply_snapshot(self.conn,[self.doc],"20261027063000")

    def test_legacy_json_never_produces_native_discount(self):
        old=document(SID,[PID])
        old["hasMenu"]["hasMenuSection"][0]["name"]="買1送1"
        self.assertEqual(canonical_menu_quotes(old,SID),{})
        result=apply_archival_quote_references(self.conn,"20261027063000",[
            ("20261021063000",[old]),("20261022063000",[old])])
        self.assertEqual(result["archive_quote_candidates"],0)
        p=self.conn.execute("SELECT archive_reference_price,archive_discount_pct,archive_is_price_deal FROM products").fetchone()
        self.assertEqual(tuple(p),(None,0,0))

    def test_legacy_repair_entrypoint_fails_closed(self):
        with self.assertRaisesRegex(ValueError,"uncertified"):
            restore_archive_price_references("not-a-real-db",{}, "20261027063000")

    def test_api_raw_enrichment_adds_numeric_native_proof(self):
        doc=document(SID,[PID])
        native={"data":{"catalogSectionsMap":{"main":[{
            "payload":{"standardItemsPayload":{"catalogItems":[{
                "uuid": PID, "title":"雞肉飯", "price":10000,
                "catalogItemAnalyticsData":{"promoType":"BOGO"},
                "itemPromotion":{"type":"buyXGetYItemPromotion",
                    "buyXGetYItemPromotion":{"buyQuantity":1,"getQuantity":1}}
            }]}}}]}}}
        enriched=enrich_converted_with_native(doc,native)
        quote=enriched["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]["verified_price"]
        self.assertEqual((quote["kind"],quote["original_total_cents"],quote["payable_total_cents"]),
            ("SAME_SKU_BXGY",20000,10000))

    def test_json_without_raw_has_no_native_proof(self):
        item=self.doc["hasMenu"]["hasMenuSection"][0]["hasMenuItem"][0]
        self.assertNotIn("verified_price",item)


if __name__=="__main__":
    unittest.main()
