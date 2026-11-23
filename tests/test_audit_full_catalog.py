"""Unit tests for all-store native Raw->ETL product coverage audit."""
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/"scripts"))
from audit_full_catalog import audit_merged_native, sku_identifier


def native(*items):
    return {"catalogSectionsMap":{"main":[{
        "payload":{"standardItemsPayload":{"catalogItems":list(items)}}}]}}


def doc(*items):
    return {"hasMenu":{"hasMenuSection":[{"hasMenuItem":list(items)}]}}


class WholeCatalogCoverageTests(unittest.TestCase):
    def test_all_native_ids_survive_in_derived_menu(self):
        source=native(
            {"uuid":"sku1","title":"A","price":25900},
            {"uuid":"sku2","title":"B","price":11900},
        )
        derived=doc(
            {"identifier":"sku1","name":"A","offers":{"price":"259.00"}},
            {"identifier":"sku2","name":"B","offers":{"price":"119.00"}},
        )
        report=audit_merged_native(source,derived)
        self.assertEqual(report["raw_unique_skus"],2)
        self.assertEqual(report["raw_missing_in_etl"],0)
        self.assertEqual(report["price_mismatches"],0)

    def test_missing_sku_is_detected_without_haagen_keyword(self):
        source=native(
            {"uuid":"sku1","title":"Some product","price":12900},
            {"uuid":"sku2","title":"另一個產品","price":9900}
        )
        derived=doc({"identifier":"sku1","name":"Some product","offers":{"price":"129.00"}})
        report=audit_merged_native(source,derived)
        self.assertEqual(report["raw_missing_in_etl"],1)
        self.assertEqual(report["raw_missing_examples"],["id:sku2"])

    def test_wrong_price_detected(self):
        source=native({"uuid":"sku1","title":"Coffee","price":15900})
        derived=doc({"identifier":"sku1","name":"Coffee","offers":{"price":"1590.00"}})
        self.assertEqual(audit_merged_native(source,derived)["price_mismatches"],1)

    def test_duplicate_categories_do_not_double_count(self):
        item={"uuid":"sku1","title":"Same item","price":10900}
        source={"catalogSectionsMap":{"a":[{"payload":{"standardItemsPayload":{"catalogItems":[item]}}}],
                                     "b":[{"payload":{"standardItemsPayload":{"catalogItems":[item]}}}]}}
        derived=doc({"identifier":"sku1","name":"Same item","offers":{"price":"109.00"}})
        self.assertEqual(audit_merged_native(source,derived)["raw_unique_skus"],1)

    def test_no_id_name_fallback(self):
        self.assertEqual(sku_identifier({"title":" 試算 商品 "}),"name:試算 商品")


if __name__=="__main__":
    unittest.main()
