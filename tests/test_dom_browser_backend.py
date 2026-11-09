"""Tests for FoodRadar's genuinely independent store webpage reader."""
import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0,str(Path(__file__).resolve().parents[1] / "scripts"))
from dom_browser_backend import open_dom_browser, classify_storefront, diagnostic


class ChromiumStub:
    def __init__(self):
        self.connected = []
        self.launched = []
    def connect_over_cdp(self, url, timeout):
        self.connected.append((url, timeout))
        return object()
    def launch(self, **kwargs):
        self.launched.append(kwargs)
        return object()


class PlaywrightStub:
    def __init__(self):
        self.chromium = ChromiumStub()


class DomSourceTests(unittest.TestCase):
    def test_hosted_browser_fails_closed_on_cloudflare(self):
        page = "www.ubereats.com\n正在執行安全驗證\nPerformance and Security by Cloudflare"
        self.assertEqual(classify_storefront(403,page),"BLOCKED_BY_PLATFORM_SECURITY")
        output=diagnostic(403,page,"github_hosted")
        self.assertIsNone(output["count"])
        self.assertFalse(output["complete"])
        self.assertTrue(output["requires_external_browser"])

    def test_remote_browser_does_not_masquerade_as_a_hundred_percent_scan(self):
        output=diagnostic(403,"正在執行安全驗證","remote_cdp")
        self.assertFalse(output["complete"])
        self.assertFalse(output["requires_external_browser"])

    def test_live_page_may_proceed_to_dom_scanner(self):
        self.assertIsNone(diagnostic(200,"草莓冰淇淋 259 元","remote_cdp"))

    def test_browser_error_not_reported_as_no_products(self):
        output=diagnostic(503,"service temporarily unavailable","github_hosted")
        self.assertIsNone(output["count"])
        self.assertEqual(output["status"],"STORE_FRONTEND_HTTP_ERROR")

    def test_remote_cdp_uses_configured_secret_endpoint(self):
        p=PlaywrightStub()
        with patch.dict(os.environ,{"FOODRADAR_DOM_CDP_URL":"wss://browser.example.test/cdp?token=SECRET"}):
            _,source=open_dom_browser(p)
        self.assertEqual(source,"remote_cdp")
        self.assertEqual(len(p.chromium.connected),1)
        self.assertEqual(len(p.chromium.launched),0)

    def test_no_remote_secret_uses_locally_launched_chromium(self):
        p=PlaywrightStub()
        with patch.dict(os.environ,{"FOODRADAR_DOM_CDP_URL":""}):
            _,source=open_dom_browser(p)
        self.assertEqual(source,"github_hosted")
        self.assertEqual(len(p.chromium.launched),1)
        self.assertEqual(len(p.chromium.connected),0)

    def test_invalid_remote_endpoint_must_fail_not_fallback(self):
        p=PlaywrightStub()
        with patch.dict(os.environ,{"FOODRADAR_DOM_CDP_URL":"not-a-browser-url"}):
            with self.assertRaises(ValueError):
                open_dom_browser(p)
        self.assertFalse(p.chromium.launched)


if __name__ == "__main__":
    unittest.main()
