import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from crawl_batches import plan_batches, MAX_BATCHES, MAX_PARALLEL


class FixedWorkerTests(unittest.TestCase):
    def test_fixed_fifteen_workers_and_exactly_once(self):
        for size in (343, 400, 524, 20000):
            with self.subTest(size=size):
                stores = [str(i) for i in range(size)]
                owners, count, weights = plan_batches(stores, {})
                self.assertEqual(count, 15)
                self.assertEqual(MAX_BATCHES, MAX_PARALLEL)
                self.assertEqual(set(owners), set(stores))
                self.assertEqual(set(owners.values()), set(range(15)))
                self.assertEqual(plan_batches(reversed(stores), {})[0], owners)
                loads = [sum(weights[s] for s in stores if owners[s] == i)
                         for i in range(count)]
                self.assertLessEqual(max(loads) - min(loads), 400)

    def test_heavy_stores_are_separated(self):
        ids = [str(i) for i in range(100)]
        owners, count, weights = plan_batches(ids, {"0": 100000, "1": 90000})
        self.assertEqual(count, 15)
        self.assertEqual(owners["0"], 0)
        self.assertNotEqual(owners["0"], owners["1"])
        loads = [sum(weights[s] for s in ids if owners[s] == i)
                 for i in range(count)]
        self.assertEqual(max(loads), 100000)

    def test_small_roster_keeps_fixed_worker_ids_without_duplicate_stores(self):
        owners, count, _ = plan_batches(["x", "y", "x"], {})
        self.assertEqual(count, 15)
        self.assertEqual(set(owners), {"x", "y"})
        self.assertEqual(owners, {"x": 0, "y": 1})
