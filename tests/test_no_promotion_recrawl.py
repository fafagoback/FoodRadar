"""Production capture paths must never re-crawl for promotion changes."""
import ast
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

class NoPromotionRecrawl(unittest.TestCase):
    def test_production_paths_do_not_import_or_call_recheck(self):
        for filename in ("distributed_crawl.py", "dynamic_crawl.py", "batch_crawl.py"):
            tree = ast.parse((ROOT / "src" / filename).read_text())
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    self.assertNotIn(node.module, ("promo_api_recheck", "promo_target_recheck"), filename)
                if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    self.assertNotIn(node.func.id, ("verify_suspicious_store", "targeted_recheck", "latest_raw_promotion_baseline"), filename)
    def test_merger_keeps_catalog_and_raw_checks(self):
        source = (ROOT / "src" / "distributed_crawl.py").read_text()
        self.assertIn("Unassembled deferred catalog", source)
        self.assertIn("original HTTP body checksum mismatch", source)
        self.assertIn("Lost HTTP response body during shard merge", source)
