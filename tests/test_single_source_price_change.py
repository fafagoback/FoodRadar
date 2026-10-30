"""Architecture contract: only PriceChange may decide whether prices went down."""
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class SingleSourcePriceChangeTests(unittest.TestCase):
    def test_all_python_entrypoints_call_shared_price_change(self):
        for path in (
            "src/serving_state.py",
            "src/alert_engine.py",
            "src/export_static_snapshots.py",
         ):
            with self.subTest(path=path):
                source = (ROOT / path).read_text(encoding="utf-8")
                self.assertIn("PriceChange.", source)
        v2=(ROOT / "src/verified_weekly.py").read_text(encoding="utf-8")
        self.assertIn("Fraction", v2)
        self.assertIn("product_price_observations", v2)
        repair=(ROOT / "scripts/repair_effective_prices.py").read_text(encoding="utf-8")
        self.assertIn("update_verified_weekly", repair)
        self.assertNotIn("calculate_effective_price", repair)

    def test_sql_and_browser_do_not_recalculate_price_changes(self):
        exporter = (ROOT / "src/export_static_snapshots.py").read_text(encoding="utf-8")
        self.assertNotIn("ROUND((h.prev_eff_price - curr.eff_price)", exporter)
        self.assertNotIn("AND curr.eff_price < h.prev_eff_price", exporter)
        for path in ("web/app.js", "web/packed-turso.js", "web/data-contract.js"):
            with self.subTest(path=path):
                source = (ROOT / path).read_text(encoding="utf-8")
                self.assertNotIn("compareUnitPrices", source)
                self.assertNotIn("refPrice > effective", source)
        reader = (ROOT / "web/packed-turso.js").read_text(encoding="utf-8")
        self.assertIn("product.price_direction", reader)
        self.assertIn("Number(product.is_price_drop || 0)", reader)
        self.assertNotIn("is_price_drop: direction === 'down'", reader)
        self.assertIn("product.price_change_percent", reader)
        page = (ROOT / "web/app.js").read_text(encoding="utf-8")
        self.assertIn("compared.price_change_amount", page)

    def test_normalized_table_preserves_authoritative_fields(self):
        normalized = (ROOT / "src/serving_state.py").read_text(encoding="utf-8")
        for field in ("price_direction", "is_price_drop", "price_change_amount", "price_change_percent"):
            self.assertIn(field, normalized)
        projected = (ROOT / "scripts/build_web_db.py").read_text(encoding="utf-8")
        self.assertNotIn("SET reference_price=NULL", projected)


if __name__ == "__main__":
    unittest.main()
