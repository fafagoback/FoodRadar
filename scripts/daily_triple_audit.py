"""Independent two-method observation audit. Read-only; never publishes menu data."""
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import requests
from playwright.sync_api import sync_playwright
from dom_browser_backend import open_dom_browser, acquire_dom_context, diagnostic
from live_webkit_dom import scan_live_webpage

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from menu_core import attach_delivery_location, normalize_store_uuid
from catalog_complete import capture_catalog, merge_catalog_pages

TW = timezone(timedelta(hours=8))
OUT = ROOT / "audit-results"
OUT.mkdir(parents=True, exist_ok=True)
STORES = [
    ("costco_neihu", "Costco 好市多內湖店",
     "https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%85%A7%E6%B9%96%E5%BA%97/rHKi5OrjWY2DLNdY8WriVA", 25.063, 121.577),
    ("pxmart_beitou", "全聯福利中心北投明德",
     "https://www.ubereats.com/tw/store/%E5%85%A8%E8%81%AF%E7%A6%8F%E5%88%A9%E4%B8%AD%E5%BF%83-%E5%8C%97%E6%8A%95%E6%98%8E%E5%BE%B7/2w4z4S62WsaVTf3t2o_ntA", 25.115, 121.520),
    ("uber_grocery_beitou", "Uber Eats 優市北投店",
     "https://www.ubereats.com/tw/store/uber-eats-%E5%84%AA%E5%B8%82-%E5%8C%97%E6%8A%95%E5%BA%97/0uUO4WnaXv-V8OsNuuueQg", 25.123246847935267, 121.52951826298867),
]
HEADERS = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/126.0 Safari/537.36",
           "Accept-Language": "zh-TW,zh;q=0.9", "Content-Type": "application/json", "x-csrf-token": "x"}

def norm(s):
    return re.sub(r"\s+", "", str(s or "").strip().lower())

def loc_cookie(lat, lon):
    from urllib.parse import quote
    raw = {"address":{"address1":"台北市","city":"台北市","country":"TW","postalCode":"100"},
           "latitude":lat,"longitude":lon,"reference":"audit-tw","referenceType":"google_places","type":"google_places"}
    return quote(json.dumps(raw, ensure_ascii=False), safe="")

def collect_api(label, url, lat, lon, store_dir):
    uuid = normalize_store_uuid("", url)
    if not re.fullmatch(r"[0-9a-f-]{36}", uuid): raise RuntimeError("invalid store UUID")
    session = requests.Session()
    store = {"store_uuid":uuid,"store_url":url,"discovered_points":[{"lat":lat,"lon":lon,"county":"台北市"}]}
    if not attach_delivery_location(session, store): raise RuntimeError("delivery-location unavailable")
    response = session.post("https://www.ubereats.com/api/getStoreV1",
             headers=HEADERS, json={"storeUuid":uuid,"sfNuggetCount":24}, timeout=30)
    (store_dir / "api-base.body").write_bytes(response.content)
    response.raise_for_status()
    data = response.json().get("data")
    if not isinstance(data, dict) or not isinstance(data.get("catalogSectionsMap"), dict):
        raise RuntimeError("base menu missing")
    def capture(root, filename, res):
        path = root / filename
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(res.content)
        return filename
    pages = capture_catalog(session, uuid, data, store_dir, HEADERS, capture)
    bodies = [json.loads((store_dir / x["path"]).read_bytes()) for x in pages["catalog_responses"]]
    native = merge_catalog_pages(data, bodies)
    rows = {}
    for sections in (native.get("catalogSectionsMap") or {}).values():
        if not isinstance(sections, list): continue
        for section in sections:
            payload = section.get("payload") or {}
            for item in (payload.get("standardItemsPayload") or {}).get("catalogItems") or []:
                if not isinstance(item, dict): continue
                name = str(item.get("title") or "").strip()
                sku = str(item.get("uuid") or item.get("itemUuid") or item.get("id") or "")
                if name:
                    rows[sku or "name:"+norm(name)] = {"name":name,"price_cents":item.get("price"),"sku":sku}
    if not rows: raise RuntimeError("no API items; cannot validate")
    return {"count":len(rows),"items":list(rows.values()),"pages":len(bodies),
            "categories":pages.get("catalog_section_count",0),"complete":bool(pages.get("catalog_complete"))}

JS_DOM = r"""() => {
  // This scanner reads rendered HTML only. No Uber/FoodRadar API requests.
  // Prefer product-specific DOM containers; fall back to semantic cards.
  const selectors = [
    '[data-testid*="catalog-item"]', '[data-testid*="store-item"]',
    '[data-testid*="menu-item"]', '[data-testid*="item-card"]',
    '[data-testid*="itemCard"]', '[data-testid*="product"]',
    '[role="listitem"]', 'article', 'li'
  ];
  const priceRegex = /(?:NT\s*[$＄]|\$|＄)\s*\d[\d,]*(?:\.\d{1,2})?/;
  const result = [];
  const already = new Set();
  for (const selector of selectors) {
    for (const node of document.querySelectorAll(selector)) {
      if (!node.getClientRects().length) continue;
      const text = (node.innerText || '').trim();
      if (!text || text.length > 1100 || !priceRegex.test(text)) continue;
      const lines = text.split(/\n/).map(s => s.trim()).filter(Boolean);
      if (lines.length > 18) continue; // likely a whole category, not one SKU
      const header = node.querySelector('h2,h3,h4,h5,[role="heading"]');
      const image = node.querySelector('img[alt]');
      const heading = (header?.innerText || '').trim();
      const imageName = (image?.getAttribute('alt') || '').trim();
      const firstLine = lines.find(x => !priceRegex.test(x) && x.length > 2) || '';
      const name = heading || (imageName.length > 4 ? imageName : firstLine);
      if (!name || name.length < 3 || name.length > 220) continue;
      const price = (text.match(priceRegex) || [])[0] || '';
      const key = name.toLowerCase().replace(/\s+/g,'') + '|' + price;
      if (already.has(key)) continue;
      already.add(key);
      const itemId = node.getAttribute('data-item-id')
        || node.getAttribute('data-uuid')
        || node.getAttribute('data-product-id')
        || '';
      result.push({name, price_text:price, item_id:itemId,
                   text:text.slice(0,500), selector});
    }
  }
  return result;
}"""

def collect_browser(browser, label, url, lat, lon, store_dir, browser_source="github_hosted"):
    context, owned = acquire_dom_context(
        browser, browser_source, locale="zh-TW",
        viewport={"width":1440,"height":980}, device_scale_factor=1)
    page = None
    try:
        context.add_cookies([{"name":"uev2.loc","value":loc_cookie(lat,lon),
                             "domain":".ubereats.com","path":"/"}])
        page = context.new_page()
        response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
        page.wait_for_timeout(3500)
        http_status = response.status if response else None
        page_title = page.title()
        visible_text = page.evaluate("document.body?.innerText?.slice(0,1000) || ''")
        blocked = diagnostic(http_status, visible_text, browser_source)
        if blocked:
            blocked["title"] = page_title
            return blocked
        cards = {}
        stale = 0
        for step in range(int(os.environ.get("AUDIT_SCROLL_LIMIT","180"))):
            for entry in page.evaluate(JS_DOM):
                name = str(entry.get("name") or "").strip()
                if len(name) > 2:
                    cards[norm(name)] = entry
            before = page.evaluate("document.scrollingElement.scrollTop")
            page.mouse.wheel(0, 900)
            page.wait_for_timeout(400)
            after = page.evaluate("document.scrollingElement.scrollTop")
            stale = stale + 1 if before == after else 0
            if stale >= 5:
                break
        dom = {"count":len(cards),"items":list(cards.values()),
               "complete":False,"status":"OBSERVED_ONLY_CATEGORY_COMPLETENESS_UNPROVEN",
               "browser_source":browser_source}
        if not cards:
            dom["status"] = "NO_PRODUCT_CARD_SELECTOR_MATCH"
        return dom
    finally:
        if owned:
            context.close()
        elif page is not None:
            # Preserve user's authorized Chrome profile for next storefront.
            page.close()

def compare(a,b):
    # Both paths yield native SKU UUIDs. Compare those (not name heuristics).
    # HTML only exposes initial preview, so API-only SKUs are NOT missing
    # from Method A; absence from HTML does not indicate any crawler defect.
    aa={x["sku"]:x for x in a.get("items",[])
        if isinstance(x,dict) and x.get("sku")}
    bb={x["sku"]:x for x in b.get("items",[])
        if isinstance(x,dict) and x.get("sku")}
    overlap=set(aa)&set(bb)
    mismatched=sorted(
        sku for sku in overlap
        if type(aa[sku].get("price_cents")) in (int,float)
        and type(bb[sku].get("price_cents")) in (int,float)
        and aa[sku]["price_cents"]!=bb[sku]["price_cents"])
    return {
        "api_skus":len(aa),"html_preview_skus":len(bb),
        "matched_sku_count":len(overlap),
        "html_preview_missing_from_api":len(set(bb)-set(aa)),
        "html_preview_missing_samples":sorted(set(bb)-set(aa))[:25],
        "price_mismatch_count":len(mismatched),
        "price_mismatch_samples":mismatched[:25],
        "api_only_skus_not_exposed_in_html_preview":len(set(aa)-set(bb)),
        "complete_inventory_comparison":False,
        "warning":"HTML is the first-page hydrated getStoreV1 preview, not a complete independent inventory."
    }

def safe_run(func):
    try: return func(), None
    except Exception as exc: return None, type(exc).__name__+": "+str(exc)[:400]

def main():
    report = {"date":datetime.now(TW).strftime("%Y-%m-%d"),"generated_at":datetime.now(TW).isoformat(),
              "method_a":"existing getStoreV1 + getCatalogPresentationV2",
              "method_b":"independent WebKit browser actual UI cards, category clicks and scrolling","stores":[]}
    # Method B uses the ACTUAL visible UI rendered by WebKit. We deliberately
    # never inspect React state, intercepted API responses or method A output
    # while collecting visible product cards.
    with sync_playwright() as pw:
        browser=pw.webkit.launch(headless=True)
        try:
            for slug,label,url,lat,lon in STORES:
                folder=OUT / report["date"] / slug
                folder.mkdir(parents=True,exist_ok=True)
                a,ae=safe_run(lambda:collect_api(label,url,lat,lon,folder))
                def visible_page():
                    ctx=browser.new_context(locale="zh-TW",
                                            viewport={"width":1440,"height":980})
                    try:
                        page=ctx.new_page()
                        response=page.goto(url,wait_until="domcontentloaded",timeout=45000)
                        page.wait_for_timeout(1600)
                        status=response.status if response else None
                        text=page.evaluate("() => (document.body?.innerText||'').slice(0,700)")
                        if status in (401,403,429) or "正在執行安全驗證" in text:
                            return {"count":None,"items":[],"complete":False,
                                    "status":"WEBPAGE_SECURITY_CHALLENGE","http_status":status}
                        observed=scan_live_webpage(
                            page,
                            limit_categories=int(os.environ.get("DOM_CATEGORY_LIMIT","0")),
                            scroll_steps=int(os.environ.get("DOM_SCROLL_STEPS","8")))
                        observed["http_status"]=status
                        return observed
                    finally:
                        ctx.close()
                b,be=safe_run(visible_page)
                record={"store":label,"url":url,
                        "api":a or {"count":None,"error":ae},
                        "browser":b or {"count":None,"complete":False,
                                        "status":"WEBPAGE_READ_FAILED","error":be}}
                if a and b and b.get("items"):
                    record["api_vs_dom"]=compare(a,b)
                if b and b.get("status")=="REAL_RENDERED_UI_OBSERVED_NOT_PROVEN_COMPLETE":
                    record["status"]="RENDERED_UI_OBSERVED_COVERAGE_NOT_PROVEN"
                elif b and b.get("status")=="WEBPAGE_SECURITY_CHALLENGE":
                    record["status"]="WEBPAGE_SECURITY_CHALLENGE"
                else:
                    record["status"]="UNVERIFIED"
                report["stores"].append(record)
                (folder/"result.json").write_text(
                    json.dumps(record,ensure_ascii=False,indent=2),encoding="utf-8")
                print("DAILY_REAL_RENDERED_UI="+json.dumps({
                    "store":label,"api":record["api"].get("count"),
                    "ui":record["browser"].get("count"),
                    "status":record["browser"].get("status"),
                    "visited":record["browser"].get("categories_visited"),
                    "total_categories":record["browser"].get("categories_listed"),
                    "compared":record.get("api_vs_dom")
                },ensure_ascii=False),flush=True)
        finally:
            browser.close()
    report["all_sample_stores_rendered"]=all(
        s.get("browser",{}).get("status")=="REAL_RENDERED_UI_OBSERVED_NOT_PROVEN_COMPLETE"
        and s.get("browser",{}).get("count",0)>0 for s in report["stores"])
    full_path=OUT/"full-catalog-latest.json"
    if full_path.exists():
        report["full_catalog_reconciliation"]=json.loads(full_path.read_text(encoding="utf-8"))
    else:
        report["full_catalog_reconciliation"]={"status":"NOT_RUN"}
    report["both_independent_methods_verified"]=all(
        s.get("api",{}).get("complete") is True
        and s.get("browser",{}).get("complete") is True
        for s in report["stores"])
    (OUT/"latest.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    (OUT/report["date"]/"report.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print("AUDIT_REPORT_COMPLETE="+json.dumps({
        "date":report["date"],
        "both_independent_methods_verified":report["both_independent_methods_verified"],
        "stores":[{"name":s["store"],"status":s["status"],
                   "api_count":s["api"].get("count"),
                   "dom_count":s["browser"].get("count")} for s in report["stores"]]
    },ensure_ascii=False),flush=True)
    if not report["both_independent_methods_verified"]:
        # A report is not a pass. GitHub Actions still uploads it and alerts.
        raise SystemExit("Independent storefront coverage NOT proven; inspect audit-results/latest.json")
if __name__=="__main__":main()
