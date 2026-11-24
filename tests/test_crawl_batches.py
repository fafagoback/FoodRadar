import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from crawl_batches import plan_batches, MAX_BATCHES, MAX_PARALLEL

class NativeBatchTests(unittest.TestCase):
    def test_current_scope_has_dynamic_queue_and_exactly_once(self):
        stores = [str(i) for i in range(343)]
        owners, count, weights = plan_batches(stores, {})
        self.assertGreater(count, MAX_PARALLEL)
        self.assertEqual(set(owners), set(stores))
        self.assertEqual(set(owners.values()), set(range(count)))
        self.assertEqual(plan_batches(reversed(stores), {})[0], owners)

    def test_very_large_scope_uses_native_matrix_limit(self):
        owners, count, _ = plan_batches((str(i) for i in range(20000)), {})
        self.assertEqual(count, MAX_BATCHES)
        self.assertEqual(len(owners), 20000)

    def test_heavy_stores_first_and_balanced(self):
        ids = [str(i) for i in range(100)]
        owners, count, weights = plan_batches(ids, {"0": 100000, "1": 90000})
        self.assertEqual(owners["0"], 0)
        self.assertNotEqual(owners["0"], owners["1"])

    def test_small_roster_does_not_make_empty_jobs(self):
        owners, count, _ = plan_batches(["x", "y", "x"], {})
        self.assertEqual(count, 2)
        self.assertEqual(set(owners), {"x", "y"})
