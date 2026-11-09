"""Independent three-method observation audit. Read-only; never publishes menu data."""
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
from PIL import Image
import pytesseract
from playwright.sync_api import sync_playwright

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

JS_DOM = """() => {
  let selectors = [
    '[data-testid*="catalog-item"]','[data-testid*="store-item"]',
    '[data-testid*="menu-item"]','[data-testid*="item-card"]',
    '[data-testid*="itemCard"]'
  ];
  let out = [];
  for (const sel of selectors) {
    for (const node of document.querySelectorAll(sel)) {
      const txt = (node.innerText || '').trim();
      const img = node.querySelector('img[alt]');
      const name = img?.getAttribute('alt') || txt.split('\\n')[0] || '';
      if (name && name.length > 2) out.push({name, text:txt.slice(0,500)});
    }
  }
  return out;
}"""

def collect_browser_and_ocr(browser, label, url, lat, lon, store_dir):
    context = browser.new_context(locale="zh-TW", viewport={"width":1440,"height":980},
                                  device_scale_factor=1)
    context.add_cookies([{"name":"uev2.loc","value":loc_cookie(lat,lon),
                         "domain":".ubereats.com","path":"/"}])
    page = context.new_page()
    response = page.goto(url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(3500)
    http_status = response.status if response else None
    page_title = page.title()
    visible_text = page.evaluate("document.body?.innerText?.slice(0,1000) || ''")
    security_challenge = (http_status in (401, 403, 429)
        or "正在執行安全驗證" in visible_text
        or "Cloudflare" in visible_text and "驗證" in visible_text)
    if security_challenge:
        proof = store_dir / "screenshot-000.png"
        page.screenshot(path=str(proof))
        blocked = {"count":None,"items":[],"complete":False,
                   "status":"BLOCKED_BY_PLATFORM_SECURITY","http_status":http_status,
                   "title":page_title, "evidence":str(proof.relative_to(ROOT)),
                   "error":f"Uber storefront HTTP {http_status}: Cloudflare challenge, no products rendered"}
        blocked_ocr = {"count":None,"items":[],"complete":False,
                       "status":"SCREENSHOT_IS_SECURITY_CHALLENGE",
                       "http_status":http_status, "error":blocked["error"],
                       "screenshots":1}
        context.close()
        return blocked, blocked_ocr, [str(proof.relative_to(ROOT))]
    cards, ocr_titles, image_paths = {}, set(), []
    stale = 0
    try:
        for step in range(int(os.environ.get("AUDIT_SCROLL_LIMIT","180"))):
            for entry in page.evaluate(JS_DOM):
                name = str(entry.get("name") or "").strip()
                if len(name) > 2: cards[norm(name)] = entry
            # Screenshot is an independent pixel observation, not DOM extraction.
            shot = store_dir / ("screenshot-%03d.png" % step)
            page.screenshot(path=str(shot))
            image_paths.append(str(shot.relative_to(ROOT)))
            # Estimate visible product cards independently from screenshot pixels.
            # Count only OCR text with a nearby visible price, not arbitrary UI labels.
            o = pytesseract.image_to_data(Image.open(shot), lang="chi_tra+eng",
                                          output_type=pytesseract.Output.DICT)
            grouped = {}
            for i, word in enumerate(o["text"]):
                word = str(word or "").strip()
                if not word or float(o["conf"][i]) < 25: continue
                key = (o["block_num"][i], o["par_num"][i], o["line_num"][i])
                rec = grouped.setdefault(key, {"words":[], "top":o["top"][i],
                                                "left":o["left"][i]})
                rec["words"].append(word)
            visible_lines = [{"text":" ".join(g["words"]),"top":g["top"],
                              "left":g["left"]} for g in grouped.values()]
            for line in visible_lines:
                if not re.search(r"(?:NT\\$|\\$|＄)\\s*[0-9][0-9,.]*|[0-9][0-9,.]*\\s*元",line["text"]):
                    continue
                adjacent = [z for z in visible_lines if z is not line
                            and abs(z["top"]-line["top"]) < 115
                            and abs(z["left"]-line["left"]) < 340
                            and len(z["text"]) >= 4
                            and not re.search(r"(?:NT\\$|\\$|＄)\\s*[0-9]",z["text"])]
                if adjacent:
                    best = min(adjacent,key=lambda z: abs(z["top"]-line["top"]))
                    name = norm(best["text"])
                    if len(name) >= 4: ocr_titles.add(name)
            before = page.evaluate("document.scrollingElement.scrollTop")
            page.mouse.wheel(0, 900)
            page.wait_for_timeout(400)
            after = page.evaluate("document.scrollingElement.scrollTop")
            stale = stale + 1 if before == after else 0
            if stale >= 5: break
        # Reaching page bottom does not prove all category tabs were opened.
        dom = {"count":len(cards),"items":list(cards.values()),
               "complete":False,"status":"OBSERVED_ONLY_CATEGORY_COMPLETENESS_UNPROVEN",
               "screenshots":len(image_paths)}
        # OCR lines are not equivalent to SKUs; report their count explicitly as candidates.
        ocr = {"count":len(ocr_titles),"items":sorted(ocr_titles),
               "complete":False,"status":"OCR_TEXT_CANDIDATES_NOT_VERIFIED_SKUS",
               "screenshots":len(image_paths)}
        if not cards: dom["status"] = "NO_PRODUCT_CARD_SELECTOR_MATCH"
        return dom, ocr, image_paths
    finally:
        context.close()

def compare(a,b):
    aa = {norm(x["name"]) for x in a.get("items",[]) if isinstance(x,dict) and x.get("name")}
    bb = {norm(x["name"]) for x in b.get("items",[]) if isinstance(x,dict) and x.get("name")}
    return {"a_only_count":len(aa-bb),"b_only_count":len(bb-aa),
            "a_only_examples":sorted(aa-bb)[:25],"b_only_examples":sorted(bb-aa)[:25],
            "warning":"Names may collide; SKU-level equality requires browser item identity."}

def safe_run(func):
    try: return func(), None
    except Exception as exc: return None, type(exc).__name__+": "+str(exc)[:400]

def main():
    report = {"date":datetime.now(TW).strftime("%Y-%m-%d"),"generated_at":datetime.now(TW).isoformat(),
              "method_a":"existing getStoreV1 + getCatalogPresentationV2",
              "method_b":"independent Chromium page DOM observation",
              "method_c":"independent screenshot OCR observation","stores":[]}
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True, args=["--no-sandbox"])
        for slug,label,url,lat,lon in STORES:
            folder=OUT / report["date"] / slug
            folder.mkdir(parents=True,exist_ok=True)
            a,ae=safe_run(lambda:collect_api(label,url,lat,lon,folder))
            bc,be=safe_run(lambda:collect_browser_and_ocr(browser,label,url,lat,lon,folder))
            b,c,shots=(bc or (None,None,[]))
            record={"store":label,"url":url,"api":a or {"count":None,"error":ae},
                    "browser":b or {"count":None,"error":be},
                    "screenshot_ocr":c or {"count":None,"error":be},
                    "screen_paths":shots}
            if a and b: record["api_vs_dom"]=compare(a,b)
            record["status"]=("BROWSER_BLOCKED_BY_PLATFORM_SECURITY"
                if b and b.get("status")=="BLOCKED_BY_PLATFORM_SECURITY"
                else "UNVERIFIED" if ae or be or not b or not b.get("count")
                else "OBSERVED_NOT_PROVEN_COMPLETE")
            report["stores"].append(record)
            (folder/"result.json").write_text(json.dumps(record,ensure_ascii=False,indent=2))
            print(label,record["api"]["count"],record["browser"]["count"],record["screenshot_ocr"]["count"],flush=True)
        browser.close()
    full_path=OUT/"full-catalog-latest.json"
    if full_path.exists():
        report["full_catalog_reconciliation"]=json.loads(full_path.read_text(encoding="utf-8"))
    else:
        report["full_catalog_reconciliation"]={"status":"NOT_RUN"}
    report["all_three_independent_methods_verified"]=all(
        s.get("api",{}).get("complete") is True
        and s.get("browser",{}).get("complete") is True
        and s.get("screenshot_ocr",{}).get("complete") is True
        for s in report["stores"])
    (OUT/"latest.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    (OUT/report["date"]/"report.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print("AUDIT_REPORT_COMPLETE="+json.dumps({
        "date":report["date"],
        "all_three_independent_methods_verified":report["all_three_independent_methods_verified"],
        "stores":[{"name":s["store"],"status":s["status"],
                   "api_count":s["api"].get("count"),
                   "dom_count":s["browser"].get("count"),
                   "ocr_count":s["screenshot_ocr"].get("count")} for s in report["stores"]]
    },ensure_ascii=False),flush=True)
    if not report["all_three_independent_methods_verified"]:
        # A report is not a pass. GitHub Actions still uploads it and alerts.
        raise SystemExit("Independent storefront coverage NOT proven; inspect audit-results/latest.json")
if __name__=="__main__":main()
