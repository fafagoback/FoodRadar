import sys
import time
import unittest
from pathlib import Path
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from sb_task_pool import SupabaseQueue


class ReusablePoolTransportTests(unittest.TestCase):
    def test_requests_share_one_session_and_measure_actual_http_time(self):
        q = SupabaseQueue(None, "123")
        q.token = "test-token"
        q.token_until = time.monotonic() + 1000
        ok = Mock(status_code=200)
        ok.json.return_value = {"ok": True}
        q.http.post = Mock(return_value=ok)
        self.assertEqual(q._rpc("read", after=0), {"ok": True})
        self.assertEqual(q._rpc("read", after=100), {"ok": True})
        self.assertEqual(q.http.post.call_count, 2)
        self.assertEqual(q.metrics()["rpc_calls"], 2)
        self.assertIn("http_seconds", q.metrics())

    def test_429_retries_do_not_change_request_identity(self):
        q = SupabaseQueue(None, "123")
        q.token = "test-token"
        q.token_until = time.monotonic() + 1000
        throttle = Mock(status_code=429, headers={"Retry-After": "0"})
        success = Mock(status_code=200)
        success.json.return_value = {"finished": 0, "claimed": [], "drained": False}
        q.http.post = Mock(side_effect=[throttle, success])
        with patch("sb_task_pool.time.sleep"), patch("sb_task_pool.random.random",return_value=0):
            result = q._rpc("exchange", worker=1, owner="x", slots=4,
                            finished=[], request_id="fixed")
        self.assertEqual(result["claimed"], [])
        self.assertEqual(q.metrics()["http_429"], 1)
        calls = q.http.post.call_args_list
        self.assertEqual(calls[0].kwargs["data"], calls[1].kwargs["data"])
        self.assertEqual(q.metrics()["rpc_calls"], 2)


if __name__ == "__main__":
    unittest.main()
