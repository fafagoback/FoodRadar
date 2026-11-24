import sys
import unittest
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from adaptive_threads import AdaptiveThreads, average_best, run_adaptive

def snap(successes=0, throttles=0, cooldown=0, interval=.18):
    return dict(successes=successes, rate_limit_events=throttles,
                cooldown_remaining_seconds=cooldown, interval_seconds=interval)

class AdaptiveThreadTests(unittest.TestCase):
    def test_growth_revert_and_backoff_within_run(self):
        c = AdaptiveThreads(6)
        c.reset(0, snap())
        c.observe(30, snap(120), True)
        self.assertEqual((c.best, c.target), (6, 7))
        c.observe(60, snap(240), True)
        self.assertEqual((c.best, c.target), (6, 6))
        c.observe(61, snap(240, 1, 30, .65), True)
        self.assertEqual(c.target, 3)
        self.assertIsNone(c.best)
        c.observe(90, snap(240, 1, 1, .65), True)
        self.assertEqual(c.target, 3)

    def test_tail_does_not_learn(self):
        c = AdaptiveThreads(6)
        c.reset(0, snap())
        c.observe(30, snap(40), False)
        self.assertIsNone(c.best)

    def test_average_half_up_and_ignore_insufficient_samples(self):
        self.assertEqual(average_best([{"optimal_threads": 5},
                                      {"optimal_threads": 6},
                                      {"optimal_threads": None}]), 6)
        self.assertIsNone(average_best([{}]))

    def test_every_item_once(self):
        class Limiter:
            def snapshot(self): return snap()
        got = []
        run_adaptive(list(range(20)), lambda x: x, Limiter(), 4, got.append)
        self.assertEqual(sorted(got), list(range(20)))

if __name__ == "__main__":
    unittest.main()
