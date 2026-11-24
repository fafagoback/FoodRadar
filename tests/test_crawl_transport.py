import sys
import unittest
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from crawl_transport import TransportPool

class TransportTests(unittest.TestCase):
    def test_sessions_isolate_cookies_but_reuse_thread_adapter(self):
        pool = TransportPool()
        try:
            first = pool.session()
            adapter = first.adapters["https://"]
            first.cookies.set("test", "secret")
            with patch.object(adapter, "close", wraps=adapter.close) as close:
                first.close()
                close.assert_not_called()
                second = pool.session()
                self.assertIs(second.adapters["https://"], adapter)
                self.assertIsNone(second.cookies.get("test"))
                second.close()
                close.assert_not_called()
                pool.close()
                close.assert_called_once()
        finally:
            pool.close()
