"""Independent DOM browser backend for FoodRadar's *second* audit method.

The browser reads the actual store webpage. It deliberately does NOT fetch
FoodRadar HF data or any Uber Eats JSON API, since that would fabricate an
independent comparison source.

To run through an authorized remote browser instead of a GitHub datacenter IP:
  FOODRADAR_DOM_CDP_URL=wss://<your-browser-provider>/...?token=<secret>
Store the entire URL as a GitHub Actions secret. Never log it or include it
in a report. A residential/self-hosted browser still needs lawful access and
may be challenged by the storefront; no provider guarantees 100% access.
"""
from __future__ import annotations

import os
from urllib.parse import urlsplit


def open_dom_browser(playwright):
    """Return (Playwright Chromium browser, source label).

    Remote CDP executes the webpage in the connected browser's own network.
    An ordinary GitHub-hosted runner uses a datacenter IP, which Uber Eats
    Cloudflare currently challenges in real-world tests.
    """
    remote = os.environ.get("FOODRADAR_DOM_CDP_URL", "").strip()
    if remote:
        parsed = urlsplit(remote)
        if parsed.scheme not in {"ws", "wss", "http", "https"} or not parsed.netloc:
            raise ValueError("FOODRADAR_DOM_CDP_URL must be an absolute CDP URL")
        # Never print the raw URL: it normally includes a provider API token.
        browser = playwright.chromium.connect_over_cdp(remote, timeout=60000)
        return browser, "remote_cdp"
    browser = playwright.chromium.launch(headless=True, args=["--no-sandbox"])
    return browser, "github_hosted"


def classify_storefront(status, visible_text):
    """The blocked page is a failed observation, NOT zero products."""
    text = str(visible_text or "").casefold()
    if status in (401, 403, 429) or (
            "cloudflare" in text and ("驗證" in text or "checking your browser" in text)
    ) or ("正在執行安全驗證" in text):
        return "BLOCKED_BY_PLATFORM_SECURITY"
    if isinstance(status, int) and status >= 400:
        return "STORE_FRONTEND_HTTP_ERROR"
    return "PAGE_LOADED"


def diagnostic(status, visible_text, source):
    state = classify_storefront(status, visible_text)
    if state == "PAGE_LOADED":
        return None
    return {
        "count": None,
        "items": [],
        "complete": False,
        "status": state,
        "http_status": status,
        "browser_source": source,
        "requires_external_browser": source == "github_hosted",
        "error": (
            "Storefront returned a security challenge; no product DOM was available."
            if state == "BLOCKED_BY_PLATFORM_SECURITY"
            else "Storefront page could not be loaded."
        ),
    }
