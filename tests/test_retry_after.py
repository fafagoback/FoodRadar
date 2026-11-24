import sys
from pathlib import Path
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from menu_core import AdaptiveRateLimiter, response_retry_after

class RetryAfterTests(unittest.TestCase):
    def test_explicit_long_server_cooldown_is_not_capped(self):
        class Response:
            headers = {"Retry-After": "600"}
        delay = response_retry_after(Response())
        self.assertEqual(delay, 600)
        lim = AdaptiveRateLimiter()
        lim.trigger_backoff(120, retry_after=delay)
        self.assertGreater(lim.snapshot()["cooldown_remaining_seconds"], 599)

    def test_invalid_header(self):
        class Response:
            headers = {"Retry-After": "invalid"}
        self.assertIsNone(response_retry_after(Response()))
