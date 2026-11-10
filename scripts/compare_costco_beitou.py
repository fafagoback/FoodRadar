"""One-off live same-store comparison: Costco Beitou, matched delivery coordinates.

Method A: Uber native store/category APIs.
Method B: WebKit rendering: visible item cards, tabs, and scrolling only.
Never declare whole-store completeness without independent UI coverage proof.
"""
from __future__ import annotations
import json
import math
import tempfile
from pathlib import Path
from playwright.sync_api import sync_playwright
from daily_triple_audit import collect_api, loc_cookie
from live_webkit_dom import scan_live_webpage

NAME = "Costco 好市多 北投店"
URL = "https://www.ubereats.com/tw/store/costco-%E5%A5%BD%E5%B8%82%E5%A4%9A-%E5%8C%97%E6%8A%95%E5%BA%97/pOIgHV_HWhCkGt_liXWnpw"
# Taipei Metro Beitou station (R22), SAME delivery coordinate for both.
LAT, LON = 25.13184, 121.498632

with tempfile.TemporaryDirectory(prefix="costco-beitou-audit-") as td:
    result = {"store": NAME, "store_url": URL, "delivery_lat": LAT, "delivery_lon": LON}
    with sync_playwright() as pw:
        browser = pw.webkit.launch(headless=True)
        try:
            context = browser.new_context(locale="zh-TW", viewport={"width": 1440, "height": 980})
            context.add_cookies([{
                "name": "uev2.loc", "value": loc_cookie(LAT, LON),
                "domain": ".ubereats.com", "path": "/",
            }])
            page = context.new_page()
            try:
                response = page.goto(URL, wait_until="domcontentloaded", timeout=60000)
                page.wait_for_timeout(1800)
                result["ui_http_status"] = response.status if response else None
                result["ui_url"] = page.url
                result["ui_title"] = page.title()
                body = page.evaluate("() => (document.body?.innerText || '').slice(0,900)")
                result["ui_body_intro"] = body[:250]
                result["store_identified_in_ui"] = "北投" in (result["ui_title"] + body)
                if result["ui_http_status"] in (401, 403, 429) or "正在執行安全驗證" in body:
                    raise RuntimeError("WebKit encountered security challenge; cannot compare products")
                if not result["store_identified_in_ui"]:
                    raise RuntimeError("WebKit did not verify Costco Beitou store identity")
                print("COSTCO_BEITOU_UI_START="+json.dumps({
                    "http": result["ui_http_status"], "title": result["ui_title"],
                    "address": [LAT, LON]}, ensure_ascii=False), flush=True)
                # Scan all categories, including out-of-viewport cards loaded by scroll.
                ui = scan_live_webpage(page, limit_categories=0, scroll_steps=50, stop_after_stale=6)
                result["ui_skus"] = ui["count"]
                result["categories_listed"] = ui["categories_listed"]
                result["categories_visited"] = ui["categories_visited"]
                result["category_stats"] = ui["category_stats"]
                result["ui_errors"] = ui["errors"]
                result["ui_complete"] = ui["complete"]
                result["category_visit_coverage"] = (ui["categories_listed"]>0 and
                        ui["categories_visited"] == ui["categories_listed"] and not ui["errors"])
                result["ui_sample"] = ui["items"][:6]
                bb={x["sku"]:x for x in ui["items"] if x.get("sku")}
            finally:
                context.close()
        finally:
            browser.close()
    print("COSTCO_BEITOU_UI_CAPTURED="+json.dumps({
        "count":result.get("ui_skus"),"categories":result.get("categories_listed"),
        "visited":result.get("categories_visited"),"errors":result.get("ui_errors"),
        "stats":result.get("category_stats"),
    },ensure_ascii=False),flush=True)
    # API collected at the same store URL / delivery point in the same CI run.
    api = collect_api(NAME,URL,LAT,LON,Path(td))
    aa = {x["sku"]:x for x in api["items"] if x.get("sku")}
    same=set(aa)&set(bb)
    only_ui=set(bb)-set(aa)
    only_api=set(aa)-set(bb)
    differences=[]
    for sku in sorted(same):
        x=aa[sku].get("price_cents")
        y=bb[sku].get("price_cents")
        if isinstance(x,(float,int)) and math.isfinite(x) and isinstance(y,int):
            if round(x) != y:
                differences.append({"sku":sku,"name":bb[sku]["name"],
                  "api_cents":x,"ui_cents":y})
    result.update({
        "api_skus":api["count"], "api_pages":api["pages"],
        "api_categories":api["categories"],"api_pagination_complete":api["complete"],
        "common_skus":len(same),
        "only_ui_skus":len(only_ui),
        "only_api_skus":len(only_api),
        "ui_only_examples":[{"sku":sku,**bb[sku]} for sku in sorted(only_ui)[:15]],
        "api_only_examples":[{"sku":sku,**aa[sku]} for sku in sorted(only_api)[:15]],
        "price_difference_count":len(differences),
        "price_difference_examples":differences[:15],
        "not_proven_complete":True,
    })
    # A durable record for CI artifacts and exact reproducible SKU comparisons.
    output=Path("audit-results")/"costco-beitou-two-method-latest.json"
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,ensure_ascii=False,indent=2),encoding="utf-8")
    print("COSTCO_BEITOU_FINAL="+json.dumps(result, ensure_ascii=False),flush=True)
    if result.get("categories_visited") != result.get("categories_listed"):
        raise SystemExit("UI category visitation incomplete; cannot certify entire storefront")
    if not result.get("ui_skus") or not result.get("api_skus"):
        raise RuntimeError("At least one data path returned zero items; comparison not verified")
