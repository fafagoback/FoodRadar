"""A fast healthy crawl must not be permanently slowed by a past 429."""
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from menu_core import AdaptiveRateLimiter


class AdaptivePacingTests(unittest.TestCase):
    def test_baseline_matches_fast_preincident_pace(self):
        limiter = AdaptiveRateLimiter(min_interval=0.18, max_interval=0.30)
        metrics = limiter.snapshot()
        self.assertEqual(metrics["interval_seconds"], 0.18)
        self.assertEqual(metrics["rate_limit_events"], 0)
        self.assertEqual(metrics["requests"], 0)

    def test_verified_throttle_slows_all_workers_without_discarding_data(self):
        limiter = AdaptiveRateLimiter(min_interval=0.18, max_interval=0.30)
        limiter.trigger_backoff(15, "Catalog application 429")
        metrics = limiter.snapshot()
        self.assertEqual(metrics["rate_limit_events"], 1)
        self.assertGreaterEqual(metrics["interval_seconds"], 0.65)
        self.assertGreater(metrics["cooldown_remaining_seconds"], 0)
        self.assertEqual(metrics["successes"], 0)

    def test_sustained_success_recovers_speed_not_instantly(self):
        limiter = AdaptiveRateLimiter(min_interval=0.18, max_interval=0.30)
        limiter.trigger_backoff(0, "Catalog application 429")
        throttled = limiter.current_interval
        for _ in range(40):
            limiter.record_success()
        self.assertEqual(limiter.current_interval, throttled)
        with patch("menu_core.time.monotonic", return_value=limiter.last_throttle_at + 61):
            for _ in range(40):
                limiter.record_success()
        self.assertLess(limiter.current_interval, throttled)
        self.assertGreaterEqual(limiter.current_interval, limiter.min_interval)
        self.assertEqual(limiter.snapshot()["successes"], 80)

    def test_cooldown_bounded_and_repeated_throttle_escalates(self):
        limiter = AdaptiveRateLimiter(min_interval=0.18, max_interval=0.30)
        limiter.trigger_backoff(100000, "Catalog HTTP 429")
        first = limiter.current_interval
        self.assertLessEqual(limiter.snapshot()["cooldown_remaining_seconds"], 120.0)
        limiter.trigger_backoff(15, "Catalog application 429")
        self.assertGreater(limiter.current_interval, first)
        self.assertEqual(limiter.snapshot()["rate_limit_events"], 2)
        self.assertLessEqual(limiter.current_interval, 2.0)


if __name__ == "__main__":
    unittest.main()
