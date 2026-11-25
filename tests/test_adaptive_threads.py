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

    def test_reference_updates_when_service_slows_later_in_same_run(self):
        c = AdaptiveThreads(6)
        c.reset(0, snap())
        c.observe(30, snap(120), True)
        c.observe(60, snap(240), True)
        self.assertEqual(c.best, 6)
        self.assertEqual(c.direction, -1)
        # Throughput dropped long after the first measurement.
        c.observe(90, snap(300), True)
        self.assertEqual(c.best_rate, 2)
        c.observe(180, snap(480), True)
        self.assertEqual(c.target, 5)
        c.observe(210, snap(552), True)
        self.assertEqual(c.best, 5)

    def test_adapts_above_two_when_measured_higher_at_nondefault_pace(self):
        c = AdaptiveThreads(2, window_seconds=10, minimum_successes=4)
        c.reset(0, snap(interval=.22))
        c.observe(10, snap(40, interval=.22), True)
        self.assertEqual((c.best, c.target), (2, 3))
        c.observe(20, snap(87, interval=.22), True)
        self.assertEqual(c.best, 3)

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
